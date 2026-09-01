package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type responseWriter struct {
	header http.Header
	status int
}

func (w *responseWriter) Header() http.Header            { return w.header }
func (w *responseWriter) Write(body []byte) (int, error) { return len(body), nil }
func (w *responseWriter) WriteHeader(status int)         { w.status = status }

func TestEventRemovesClientWhenSSENotSupported(t *testing.T) {
	h := &Handler{channels: make(map[uint64]chan MessageChannel)}
	recorder := &responseWriter{header: make(http.Header)}
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "/event", nil)
	require.NoError(t, err)

	h.Event(recorder, request)

	require.Equal(t, http.StatusInternalServerError, recorder.status)
	require.Empty(t, h.channels)
}
