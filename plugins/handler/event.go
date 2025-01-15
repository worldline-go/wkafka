package handler

import (
	"context"
	"encoding/json"
)

type MessageChannel struct {
	Type  string
	Value string
}

func (h *Handler) addClient() (uint64, <-chan MessageChannel) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	messageChan := make(chan MessageChannel, 64)
	h.key++

	h.channels[h.key] = messageChan

	return h.key, messageChan
}

func (h *Handler) deleteClient(keys ...uint64) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for _, k := range keys {
		delete(h.channels, k)
	}
}

func (h *Handler) broadcastMessage(message MessageChannel) {
	deleteClients := make([]uint64, 0, 10)
	defer func() {
		h.deleteClient(deleteClients...)
	}()

	h.mutex.RLock()
	defer h.mutex.RUnlock()

	for key := range h.channels {
		select {
		case h.channels[key] <- message:
		default:
			deleteClients = append(deleteClients, key)
		}
	}
}

func (h *Handler) TriggerInfo(ctx context.Context) {
	if h.pubsub == nil {
		vByte, _ := json.Marshal(h.getInfo())
		h.BroadcastInfo(string(vByte))

		return
	}

	if err := h.RequestPublishInfo(ctx); err != nil {
		h.client.Logger().Error("failed to publish info", "error", err)
	}
}

func (h *Handler) BroadcastInfo(data string) {
	h.broadcastMessage(MessageChannel{
		Type:  "info",
		Value: data,
	})
}

func (h *Handler) BroadcastDelete(data string) {
	h.broadcastMessage(MessageChannel{
		Type:  "delete",
		Value: data,
	})
}
