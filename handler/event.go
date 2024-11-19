package handler

import (
	"encoding/json"
)

func (h *Handler) addClient() (uint64, <-chan string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	messageChan := make(chan string, 64)
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

func (h *Handler) broadcastMessage(message string) {
	deleteClients := make([]uint64, 0, 10)
	defer func() {
		h.deleteClient(deleteClients...)
	}()

	h.mutex.RLock()
	defer h.mutex.RUnlock()

	h.lastMessageInfo = message

	for key := range h.channels {
		select {
		case h.channels[key] <- message:
		default:
			deleteClients = append(deleteClients, key)
		}
	}
}

func (h *Handler) TriggerInfo() {
	vByte, _ := json.Marshal(h.getInfo())

	h.broadcastMessage(string(vByte))
}
