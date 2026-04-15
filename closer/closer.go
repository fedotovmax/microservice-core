package closer

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

// CloseFunc — сигнатура твоего метода Stop(ctx)
type CloseFunc func(ctx context.Context) error

type Closer struct {
	mu    sync.Mutex
	funcs []CloseFunc
}

func New() *Closer {
	return &Closer{}
}

// Add добавляет одну или несколько функций в список
func (c *Closer) Add(f ...CloseFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.funcs = append(c.funcs, f...)
}

// Close закрывает ресурсы последовательно в порядке LIFO (Last In, First Out).
// Идеально для остановки серверов перед закрытием БД.
func (c *Closer) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []string
	// Идем с конца очереди к началу
	for i := len(c.funcs) - 1; i >= 0; i-- {
		if err := c.funcs[i](ctx); err != nil {
			errs = append(errs, err.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("sequential close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

// CloseParallel закрывает все ресурсы одновременно.
// Идеально для независимых ресурсов (Redis, DB, Kafka), чтобы сэкономить время.
func (c *Closer) CloseParallel(ctx context.Context) error {
	c.mu.Lock()
	funcs := c.funcs // Копируем слайс, чтобы не держать мьютекс долго
	c.mu.Unlock()

	var (
		wg    sync.WaitGroup
		errMu sync.Mutex
		errs  []string
		done  = make(chan struct{})
	)

	for _, f := range funcs {
		wg.Add(1)
		go func(fn CloseFunc) {
			defer wg.Done()
			if err := fn(ctx); err != nil {
				errMu.Lock()
				errs = append(errs, err.Error())
				errMu.Unlock()
			}
		}(f)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Все закрылось вовремя
	case <-ctx.Done():
		return fmt.Errorf("parallel close: context deadline exceeded")
	}

	if len(errs) > 0 {
		return fmt.Errorf("parallel close errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
