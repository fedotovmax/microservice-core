package closer

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// CloseFunc — функция корректного завершения ресурса.
// ВАЖНО: функция обязана уважать ctx (отмену/таймаут).
type CloseFunc func(ctx context.Context) error

type namedFunc struct {
	name string
	fn   CloseFunc
}

type Closer struct {
	mu     sync.Mutex
	funcs  []namedFunc
	closed bool
}

func New() *Closer {
	return &Closer{}
}

func (c *Closer) Add(f ...CloseFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		panic("closer: Add called after Close")
	}

	for _, fn := range f {
		c.funcs = append(c.funcs, namedFunc{
			name: "unnamed",
			fn:   fn,
		})
	}
}

func (c *Closer) AddNamed(name string, fn CloseFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		panic("closer: AddNamed called after Close")
	}

	c.funcs = append(c.funcs, namedFunc{
		name: name,
		fn:   fn,
	})
}

// safeCall — оборачивает вызов CloseFunc с защитой от panic
func safeCall(ctx context.Context, nf namedFunc) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s: panic: %v", nf.name, r)
		}
	}()

	if e := nf.fn(ctx); e != nil {
		return fmt.Errorf("%s: %w", nf.name, e)
	}

	return nil
}

// Close закрывает ресурсы последовательно (LIFO)
func (c *Closer) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	funcs := make([]namedFunc, len(c.funcs))
	copy(funcs, c.funcs)
	c.mu.Unlock()

	var errs []error

	for i := len(funcs) - 1; i >= 0; i-- {
		f := funcs[i]

		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
			break
		}

		if err := safeCall(ctx, f); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// CloseParallel закрывает ресурсы параллельно (LIFO порядок запуска)
func (c *Closer) CloseParallel(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true

	funcs := make([]namedFunc, len(c.funcs))
	copy(funcs, c.funcs)
	c.mu.Unlock()

	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for i := len(funcs) - 1; i >= 0; i-- {
		f := funcs[i]

		wg.Go(func() {
			if err := safeCall(ctx, f); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:

		mu.Lock()
		err := errors.Join(errs...)
		mu.Unlock()
		return err

	case <-ctx.Done():
		return fmt.Errorf("parallel close: %w", ctx.Err())
	}
}
