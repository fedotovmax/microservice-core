package conc

import (
	"context"
	"fmt"
	"sync"
)

type errGroupToken struct{}

type ErrGroup struct {
	cancel func(error)

	wg sync.WaitGroup

	sem chan errGroupToken

	errOnce sync.Once
	err     error
}

func (g *ErrGroup) done() {
	if g.sem != nil {
		<-g.sem
	}
	g.wg.Done()
}

func WithContext(ctx context.Context) (*ErrGroup, context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)
	return &ErrGroup{cancel: cancel}, ctx
}

func (g *ErrGroup) Wait() error {
	g.wg.Wait()
	if g.cancel != nil {
		g.cancel(g.err)
	}
	return g.err
}

func (g *ErrGroup) Go(f func() error) {
	if g.sem != nil {
		g.sem <- errGroupToken{}
	}

	g.wg.Add(1)
	go func() {
		defer g.done()

		if err := f(); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if g.cancel != nil {
					g.cancel(g.err)
				}
			})
		}
	}()
}

func (g *ErrGroup) TryGo(f func() error) bool {
	if g.sem != nil {
		select {
		case g.sem <- errGroupToken{}:
		default:
			return false
		}
	}

	g.wg.Add(1)
	go func() {
		defer g.done()

		if err := f(); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				if g.cancel != nil {
					g.cancel(g.err)
				}
			})
		}
	}()
	return true
}

func (g *ErrGroup) SetLimit(n int) {
	if n < 0 {
		g.sem = nil
		return
	}
	if active := len(g.sem); active != 0 {
		panic(fmt.Errorf("errgroup: modify limit while %v goroutines in the group are still active", active))
	}
	g.sem = make(chan errGroupToken, n)
}
