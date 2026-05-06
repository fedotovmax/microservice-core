package outbox

import (
	"sync/atomic"

	"github.com/fedotovmax/microservice-core/logger"
)

type Outbox struct {
	log                logger.Logger
	producer           AsyncPublisher
	adapter            DataSource
	config             Config
	isStopped          chan struct{}
	stopProcessSignal  chan struct{}
	processingFinished chan struct{}
	inProcess          atomic.Bool
}

func New(config *Config, p AsyncPublisher, a DataSource, log logger.Logger) *Outbox {

	return &Outbox{
		log:                log,
		producer:           p,
		adapter:            a,
		config:             *config,
		stopProcessSignal:  make(chan struct{}),
		isStopped:          make(chan struct{}),
		processingFinished: make(chan struct{}),
	}

}
