```go


//"github.com/dnwe/otelsarama"

	ap, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)

	if config.Tracing {
		ap = otelsarama.WrapAsyncProducer(saramaConfig, ap)
	}
```
