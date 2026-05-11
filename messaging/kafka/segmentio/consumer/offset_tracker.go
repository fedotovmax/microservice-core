package consumer

import (
	"sync"

	skafka "github.com/segmentio/kafka-go"
)

type onMarkFunc func(msg skafka.Message)

type offsetTrackerKey struct {
	topic     string
	partition int
}

// offsetTracker накапливает офсеты и сбрасывает их батчем — аналог MarkMessage в Sarama
type offsetTracker struct {
	mu      sync.Mutex
	offsets map[offsetTrackerKey]skafka.Message // partition -> последнее помеченное сообщение
	onMark  onMarkFunc                          // колбэк на марк

}

func newOffsetTracker(onMark onMarkFunc) *offsetTracker {
	return &offsetTracker{
		offsets: make(map[offsetTrackerKey]skafka.Message),
		onMark:  onMark,
	}
}

// mark помечает сообщение как обработанное (аналог MarkMessage)
func (t *offsetTracker) mark(msg skafka.Message) {
	t.mu.Lock()
	var notify bool
	key := offsetTrackerKey{msg.Topic, msg.Partition}
	if cur, ok := t.offsets[key]; !ok || msg.Offset > cur.Offset {
		t.offsets[key] = msg
		notify = true
	}
	t.mu.Unlock()

	if notify && t.onMark != nil {
		t.onMark(msg)
	}
}

// flush сбрасывает все помеченные офсеты и возвращает их
func (t *offsetTracker) flush() []skafka.Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.offsets) == 0 {
		return nil
	}
	msgs := make([]skafka.Message, 0, len(t.offsets))
	for _, msg := range t.offsets {
		msgs = append(msgs, msg)
	}
	t.offsets = make(map[offsetTrackerKey]skafka.Message)
	return msgs
}
