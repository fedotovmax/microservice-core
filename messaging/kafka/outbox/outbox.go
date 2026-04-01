package outbox

import (
	"context"
	"sync/atomic"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Outbox struct {
	log       logger.Logger
	producer  kafka.AsyncProducer
	adapter   Adapter
	config    Config
	ctx       context.Context
	stop      func()
	isStopped chan struct{}
	inProcess atomic.Bool
}

func New(config Config, p kafka.AsyncProducer, a Adapter, log logger.Logger) *Outbox {

	ctx, cancel := context.WithCancel(context.Background())

	return &Outbox{
		log:       log,
		producer:  p,
		adapter:   a,
		config:    config,
		ctx:       ctx,
		stop:      cancel,
		isStopped: make(chan struct{}),
	}

}
