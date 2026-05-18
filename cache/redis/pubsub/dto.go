package pubsub

import "context"

// Handler — обработчик входящего сообщения.
// Если возвращает ошибку — она логируется, подписка продолжается.
type Handler func(ctx context.Context, msg Message) error

// Message — входящее pub/sub сообщение.
type Message struct {
	Channel      string
	Pattern      string
	Payload      string
	PayloadSlice []string
}
