package outbox

import (
	"sync/atomic"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Outbox struct {
	log                logger.Logger
	producer           kafka.AsyncProducer
	adapter            Adapter
	config             Config
	isStopped          chan struct{}
	stopProcessSignal  chan struct{}
	processingFinished chan struct{}
	inProcess          atomic.Bool
}

func New(config Config, p kafka.AsyncProducer, a Adapter, log logger.Logger) *Outbox {

	return &Outbox{
		log:                log,
		producer:           p,
		adapter:            a,
		config:             config,
		stopProcessSignal:  make(chan struct{}),
		isStopped:          make(chan struct{}),
		processingFinished: make(chan struct{}),
	}

}
