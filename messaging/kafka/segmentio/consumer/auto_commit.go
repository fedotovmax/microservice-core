package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func (c *group) autoCommit(ctx context.Context, tracker *offsetTracker) {

	const op = "core.messaging.kafka.segmentio.consumer.group.autoCommit"

	ticker := time.NewTicker(c.config.CommitInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			msgs := tracker.flush()
			if len(msgs) == 0 {
				continue
			}
			if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				if c.withMetrics {
					kafka.RecordCommitError(ctx)
				}
				select {
				case c.errCh <- fmt.Errorf("%s: auto-commit error: %w", op, err):
				case <-ctx.Done():
					return
				}
			}
		case <-ctx.Done():
			// Финальный коммит при завершении
			msgs := tracker.flush()
			if len(msgs) > 0 {
				_ = c.reader.CommitMessages(context.Background(), msgs...)
			}
			return
		}
	}
}
