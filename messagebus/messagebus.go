package messagebus

import (
	"fmt"
	"sync"
)

type AudioResponseData struct {
	Type         string   `json:"type"`
	RVFid        string   `json:"rv_fid"`
	Certainty    int      `json:"certainty"`
	History      []string `json:"history"`
	Label        string   `json:"label"`
	Organization string   `json:"organization"`
	DeviceID     string   `json:"device_id"`
	Categories   string   `json:"categories"`
}

type Message struct {
	DeviceID string            `json:"deviceID"`
	Data     AudioResponseData `json:"data"`
}

type MessageBus struct {
	subscribers map[string]map[chan Message]struct{}
	mu          sync.RWMutex
}

func NewMessageBus() *MessageBus {
	return &MessageBus{
		subscribers: make(map[string]map[chan Message]struct{}),
	}
}

func (mb *MessageBus) Subscribe(deviceID string) chan Message {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if _, ok := mb.subscribers[deviceID]; !ok {
		mb.subscribers[deviceID] = make(map[chan Message]struct{})
	}

	ch := make(chan Message)
	mb.subscribers[deviceID][ch] = struct{}{}
	return ch
}

func (mb *MessageBus) IsSubscribed(deviceID string) bool {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	_, ok := mb.subscribers[deviceID]
	return ok
}

func (mb *MessageBus) GetSubscribedChannel(deviceID string) chan Message {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	for ch := range mb.subscribers[deviceID] {
		return ch
	}
	return nil
}

func (mb *MessageBus) Unsubscribe(deviceID string) {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if subscribers, ok := mb.subscribers[deviceID]; ok {
		for ch := range subscribers {
			close(ch)
			delete(mb.subscribers[deviceID], ch)
		}
		if len(mb.subscribers[deviceID]) == 0 {
			delete(mb.subscribers, deviceID)
		}
	}
}

func (mb *MessageBus) Publish(message Message) {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	fmt.Printf("Publishing message: %v\n", message)
	fmt.Printf("Subscribers: %v\n", mb.subscribers)
	if subscribers, ok := mb.subscribers[message.DeviceID]; ok {
		for ch := range subscribers {
			ch <- message
		}
	}
}
