package handler

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/worldline-go/wkafka"
)

//go:embed _ui/dist/*
var uiFS embed.FS

//go:embed files/*
var fileFS embed.FS

// @Title wkafka API
// @BasePath /wkafka/
// @description Kafka library
//
//go:generate swag init -pd -d ./ -g handler.go --ot json -o ./files
type Handler struct {
	id      string
	client  *wkafka.Client
	mux     *http.ServeMux
	pattern string
	addr    string

	channels map[uint64]chan MessageChannel
	key      uint64
	mutex    sync.RWMutex

	pubsub pubsub
}

var PluginName = "handler"

type Config struct {
	Enabled    bool          `cfg:"enabled"     json:"enabled"`
	Addr       string        `cfg:"addr"        json:"addr"`
	PathPrefix string        `cfg:"path_prefix" json:"path_prefix"`
	PubSub     *PubSubConfig `cfg:"pubsub"      json:"pubsub"`
}

func (c *Config) ToOption() Option {
	return func(o *option) {
		o.Addr = c.Addr
		o.PathPrefix = c.PathPrefix
		o.PubSub = c.PubSub
	}
}

func PluginWithName() (string, wkafka.PluginFunc[Config]) {
	return PluginName, Plugin
}

func Plugin(ctx context.Context, client *wkafka.Client, cfg Config) error {
	if !cfg.Enabled {
		return nil
	}

	h, err := New(ctx, client, cfg.ToOption())
	if err != nil {
		return err
	}

	if h.pubsub != nil {
		go func() {
			if err := h.StartPubSub(ctx); err != nil && !errors.Is(err, context.Canceled) {
				client.Logger().Error("failed to start pubsub", "error", err)
				client.Close()
			}

			h.Close()
		}()
	}

	go func() {
		if err := h.Serve(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			client.Logger().Error("failed to serve handler", "error", err)
			client.Close()
		}
	}()

	return nil
}

// NewHandler returns a http.Handler implementation.
func New(ctx context.Context, client *wkafka.Client, opts ...Option) (*Handler, error) {
	o := option{}
	o.apply(opts...)

	if o.Addr == "" {
		o.Addr = ":17070"
	}

	h := &Handler{
		id:       ulid.Make().String(),
		client:   client,
		channels: make(map[uint64]chan MessageChannel),
		addr:     o.Addr,
	}

	if o.PubSub != nil {
		pubsub, err := o.PubSub.New(client.AppName()+"-"+client.GroupID(), client.Logger())
		if err != nil {
			return nil, err
		}

		h.pubsub = pubsub

		h.TriggerInfo(ctx)
	}

	// add trigger function to detect changes
	client.AddTrigger(h.TriggerInfo)

	prefix := "/" + strings.Trim(o.PathPrefix, "/")
	if prefix != "/" {
		prefix += "/"
	}

	h.pattern = prefix + "wkafka/"

	ui, err := h.UI()
	if err != nil {
		return nil, err
	}

	fileShare, err := h.File()
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc(http.MethodPut+" "+h.pattern+"v1/skip", h.SkipSet)
	mux.HandleFunc(http.MethodPatch+" "+h.pattern+"v1/skip", h.SkipUpdate)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"v1/info", h.Info)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"v1/event", h.Event)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"ui/", ui.ServeHTTP)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"files/", fileShare.ServeHTTP)

	h.mux = mux

	return h, nil
}

func (h *Handler) Close() error {
	if h.pubsub != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		h.PublishDelete(ctx)
		return h.pubsub.Close()
	}

	return nil
}

func (h *Handler) UI() (http.Handler, error) {
	f, err := fs.Sub(uiFS, "_ui/dist")
	if err != nil {
		return nil, err
	}

	folderM := folder{
		Index:          true,
		StripIndexName: true,
		SPA:            true,
		PrefixPath:     h.pattern + "ui/",
		CacheRegex: []*RegexCacheStore{
			{
				Regex:        `index\.html$`,
				CacheControl: "no-store",
			},
		},
	}

	folderM.SetFs(http.FS(f))

	return folderM.Handler()
}

func (h *Handler) File() (http.Handler, error) {
	f, err := fs.Sub(fileFS, "files")
	if err != nil {
		return nil, err
	}

	folderM := folder{
		PrefixPath: h.pattern + "files/",
		CacheRegex: []*RegexCacheStore{
			{
				Regex:        `.*`,
				CacheControl: "no-store",
			},
		},
	}

	folderM.SetFs(http.FS(f))

	return folderM.Handler()
}

func (h *Handler) Handler() (string, http.Handler) {
	return h.pattern, h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle(h.Handler())

	s := http.Server{
		Addr:    h.addr,
		Handler: mux,
	}

	if h.client != nil {
		if l := h.client.Logger(); l != nil {
			l.Info("starting listening wkafka handler", "addr", s.Addr)
		}
	}

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	return s.ListenAndServe()
}

// @Summary Patch the skip.
// @Tags wkafka
// @Accept json
// @Produce json
// @Param skip body SkipRequest true "skip"
// @Success 200 {object} Response
// @Router /v1/skip [PATCH]
func (h *Handler) SkipUpdate(w http.ResponseWriter, r *http.Request) {
	var skipRequest SkipRequest

	if err := json.NewDecoder(r.Body).Decode(&skipRequest); err != nil {
		httpResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	if h.pubsub != nil {
		if err := h.pubsub.Publish(r.Context(), PubSubModelPublish{
			Type:  "skip-append",
			Value: skipRequest,
		}); err != nil {
			httpResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		h.client.Skip(r.Context(), wkafka.SkipAppend(skipRequest))
	}

	httpResponse(w, "skip appended", http.StatusOK)
}

// @Summary Set the skip.
// @Tags wkafka
// @Accept json
// @Produce json
// @Param skip body SkipRequest true "skip"
// @Success 200 {object} Response
// @Router /v1/skip [PUT]
func (h *Handler) SkipSet(w http.ResponseWriter, r *http.Request) {
	var skipRequest SkipRequest

	if err := json.NewDecoder(r.Body).Decode(&skipRequest); err != nil {
		httpResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	if h.pubsub != nil {
		if err := h.pubsub.Publish(r.Context(), PubSubModelPublish{
			Type:  "skip-replace",
			Value: skipRequest,
		}); err != nil {
			httpResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		h.client.Skip(r.Context(), wkafka.SkipReplace(skipRequest))
	}

	httpResponse(w, "skip replaced", http.StatusOK)
}

func (h *Handler) getInfo() *InfoResponse {
	return &InfoResponse{
		ID:        h.id,
		DLQTopics: h.client.DLQTopics(),
		Topics:    h.client.Topics(),
		Skip:      h.client.SkipCheck(),
		DLQRecord: dlqRecord(h.client.DLQRecord()),
		RetryAt:   h.client.DLQRetryAt().Format(time.RFC3339),
	}
}

// Info returns the current information of the client.
// @Summary Get the current information of the client.
// @Tags wkafka
// @Success 200 {object} InfoResponse
// @Router /v1/info [GET]
func (h *Handler) Info(w http.ResponseWriter, r *http.Request) {
	if h.pubsub == nil {
		httpResponseJSON(w, h.getInfo(), http.StatusOK)
		return
	}

	// send info request to pubsub and get the response
	h.PublishInfo(r.Context())
}

func (h *Handler) Event(w http.ResponseWriter, r *http.Request) {
	clientKey, messageChan := h.addClient()

	// prepare the header
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// prepare the flusher
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpResponse(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// first value
	h.TriggerInfo(r.Context())

	vByte, _ := json.Marshal(h.getInfo())

	fmt.Fprintf(w, "event: info\ndata: %s\n\n", string(vByte))
	flusher.Flush()

	for {
		select {
		case message := <-messageChan:
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", message.Type, message.Value)
			flusher.Flush()
		case <-r.Context().Done():
			h.deleteClient(clientKey)
			return
		}
	}
}
