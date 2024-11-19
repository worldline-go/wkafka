package handler

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"sync"

	"github.com/worldline-go/wkafka"
)

//go:embed _ui/dist/*
var uiFS embed.FS

type Handler struct {
	client  *wkafka.Client
	mux     *http.ServeMux
	pattern string

	channels        map[uint64]chan string
	lastMessageInfo string
	key             uint64
	mutex           sync.RWMutex
}

// NewHandler returns a http.Handler implementation.
func New(client *wkafka.Client, opts ...Option) (*Handler, error) {
	o := option{}
	o.apply(opts...)

	h := &Handler{
		client:   client,
		channels: make(map[uint64]chan string),
	}

	// add trigger function to detect changes
	client.Trigger = append(client.Trigger, h.TriggerInfo)

	prefix := "/" + strings.Trim(o.PathPrefix, "/")
	if prefix != "/" {
		prefix += "/"
	}

	h.pattern = prefix + "wkafka/"

	ui, err := h.UI()
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc(http.MethodPut+" "+h.pattern+"v1/skip", h.SkipSet)
	mux.HandleFunc(http.MethodPatch+" "+h.pattern+"v1/skip", h.SkipUpdate)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"v1/info", h.Info)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"v1/event", h.Event)
	mux.HandleFunc(http.MethodGet+" "+h.pattern+"ui/", ui.ServeHTTP)

	h.mux = mux

	h.TriggerInfo()

	return h, nil
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

func (h *Handler) SkipUpdate(w http.ResponseWriter, r *http.Request) {
	skipRequest := &SkipRequest{}

	if err := json.NewDecoder(r.Body).Decode(skipRequest); err != nil {
		httpResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.client.Skip(wkafka.SkipAppend(skipRequest.Skip))

	httpResponse(w, "skip appended", http.StatusOK)
}

func (h *Handler) SkipSet(w http.ResponseWriter, r *http.Request) {
	skipRequest := &SkipRequest{}

	if err := json.NewDecoder(r.Body).Decode(skipRequest); err != nil {
		httpResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.client.Skip(wkafka.SkipReplace(skipRequest.Skip))

	httpResponse(w, "skip replaced", http.StatusOK)
}

func (h *Handler) getInfo() *InfoResponse {
	return &InfoResponse{
		DLQTopics: h.client.DLQTopics,
		Topics:    h.client.Topics,
		Skip:      h.client.SkipCheck(),
		DLQRecord: dlqRecord(h.client.DLQRecord()),
	}
}

func (h *Handler) Info(w http.ResponseWriter, _ *http.Request) {
	httpResponseJSON(w, h.getInfo(), http.StatusOK)
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
	fmt.Fprintf(w, "event: info\ndata: %s\n\n", h.lastMessageInfo)
	flusher.Flush()

	for {
		select {
		case message := <-messageChan:
			fmt.Fprintf(w, "event: info\ndata: %s\n\n", message)
			flusher.Flush()
		case <-r.Context().Done():
			h.deleteClient(clientKey)
			return
		}
	}
}
