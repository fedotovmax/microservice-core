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

// done уменьшает счетчик WaitGroup и освобождает место в семафоре.
func (g *ErrGroup) done() {
	if g.sem != nil {
		<-g.sem
	}
	g.wg.Done()
}

func NewErrGroup() *ErrGroup {
	return &ErrGroup{}
}

func NewErrGroupWithContext(ctx context.Context) (*ErrGroup, context.Context) {
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
	// 1. Если лимит установлен, пытаемся положить токен в канал-семафор
	if g.sem != nil {
		g.sem <- errGroupToken{}
	}

	g.wg.Add(1)
	go func() {
		defer g.done() // 2. Освобождаем ресурсы при выходе

		if err := f(); err != nil {
			g.errOnce.Do(func() { // 3. Через once вызываем отмену контекста и сохраняем ошибку
				g.err = err
				if g.cancel != nil {
					g.cancel(g.err)
				}
			})
		}
	}()
}

// TryGo запускает функцию только в том случае, если лимит горутин не превышен.
// Возвращает true, если горутина была запущена, и false в противном случае.
func (g *ErrGroup) TryGo(f func() error) bool {
	if g.sem != nil {
		select {
		case g.sem <- errGroupToken{}:
			// Успешно заняли место в семафоре
		default:
			// Мест нет, выходим немедленно
			return false
		}
	}

	// Если лимита нет (g.sem == nil) ИЛИ место в семафоре занято успешно
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

// SetLimit ограничивает количество горутин, работающих одновременно.
// Если n <= 0, лимит отсутствует.
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
