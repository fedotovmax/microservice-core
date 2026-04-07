package outbox

import "sync"

func (p *Outbox) Start() {

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		p.producer.HandleErrors(p.config.HandleErrorTimeout, p.adapter.MarkAsFailed)
	})

	wg.Go(func() {
		p.producer.HandleSuccesses(p.config.HandleSuccessTimeout, p.adapter.Confirm)
	})

	wg.Go(func() {
		p.process()
	})

	go func() {
		wg.Wait()
		close(p.isStopped)
	}()

}
