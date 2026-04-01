package outbox

import "sync"

func (p *Outbox) Start() {
	wg := &sync.WaitGroup{}

	wg.Go(func() {
		p.producer.HandleErrors(p.ctx, p.adapter.MarkAsFailed)
	})

	wg.Go(func() {
		p.producer.HandleSuccesses(p.ctx, p.adapter.Confirm)
	})

	wg.Go(func() {
		p.process(p.ctx)
	})

	go func() {
		wg.Wait()
		close(p.isStopped)
	}()

}
