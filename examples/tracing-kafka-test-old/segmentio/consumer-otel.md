```go
package consumer

import (
	"context"

	otelkafka "github.com/Trendyol/otel-kafka-konsumer"
	skafka "github.com/segmentio/kafka-go"
)

type otel struct {
	r *otelkafka.Reader
}

func newOtel(r *otelkafka.Reader) *otel {
	return &otel{r: r}
}

func (o *otel) FetchMessage(ctx context.Context) (skafka.Message, error) {
	var msg skafka.Message
	err := o.r.FetchMessage(ctx, &msg)
	return msg, err
}

func (o *otel) CommitMessages(ctx context.Context, msgs ...skafka.Message) error {
	return o.r.CommitMessages(ctx, msgs...)
}

func (o *otel) Close() error {
	return o.r.Close()
}

```

```go
	var reader segmentioReader

	if config.Tracing {
		otelReader, err := otelkafka.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		reader = newOtel(otelReader)
	} else {
		reader = r
	}
```
