package sarama

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type h struct {
	r               kafka.Reader
	commitInterval  time.Duration
	commitBatchSize int
}

func (h *h) Setup(s sarama.ConsumerGroupSession) error {
	//TODO: adapter s to OnSetup (later)
	h.r.OnSetup()
	return nil
}

func (h *h) Cleanup(s sarama.ConsumerGroupSession) error {
	//TODO: adapter s to OnCleanUp (later)
	h.r.OnCleanUp()

	return nil
}

func (h *h) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {

	buffer := 0
	ticker := time.NewTicker(h.commitInterval)
	defer ticker.Stop()

	commit := func() {
		s.Commit()
		buffer = 0
	}

ConsumerLoop:
	for {
		select {
		case <-ticker.C:
			if buffer > 0 {
				commit()
			}

		case <-s.Context().Done():
			//TODO: log
			if buffer > 0 {
				commit()
			}

			break ConsumerLoop

		case msg, ok := <-c.Messages():

			if !ok {
				//TODO: log
				break ConsumerLoop
			}

			mark := func(meta string) {
				s.MarkMessage(msg, meta)
				buffer++
			}

			var eventID string
			var eventType string

			for _, header := range msg.Headers {
				key := string(header.Key)
				switch key {
				case kafka.HeaderEventType:
					eventType = string(header.Value)
				case kafka.HeaderEventID:
					eventID = string(header.Value)
				}
			}

			if eventID == "" {
				//TODO: log
				mark("")
				continue
			}

			if eventType == "" {
				//TODO: log
				mark("")
				continue
			}

			payload := msg.Value

			ev := kafka.NewConsumeEvent(eventID, eventType, payload, msg.Key, msg.Offset, msg.Topic, msg.Partition)

			h.r.OnRead(s.Context(), ev, mark)

			// commit по batchSize
			if h.commitBatchSize > 0 && buffer >= h.commitBatchSize {
				commit()
				//TODO: log how many msgs commited
			}
		}
	}

	if buffer > 0 {
		commit()
		//TODO: log how many msgs commited in final batch after func destroy
	}

	return nil

}
