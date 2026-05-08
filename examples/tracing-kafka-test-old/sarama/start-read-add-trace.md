```go


//"github.com/dnwe/otelsarama"

	gh := newGroupHandler(c.log, readParams, c.config.MaxProcessingTime)

		if c.config.Tracing {
			gh = otelsarama.WrapConsumerGroupHandler(gh)
		}

```
