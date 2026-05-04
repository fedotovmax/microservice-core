package redis

type Message struct {
	Channel      string
	Pattern      string
	Payload      string
	PayloadSlice []string
}
