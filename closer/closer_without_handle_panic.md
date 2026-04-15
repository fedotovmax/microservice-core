[Обсуждение](https://chatgpt.com/c/69dfd20d-f7d0-832b-aaa2-a33310435b03)

```go
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

// Add добавляет функции без имени (fallback)
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

// AddNamed — основной метод (рекомендуется использовать его)
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

		if err := f.fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
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

		wg.Add(1)
		go func(f namedFunc) {
			defer wg.Done()

			if err := f.fn(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
				mu.Unlock()
			}
		}(f)
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
		// mu.Lock()
		// defer mu.Unlock()

		// if len(errs) > 0 {
		// 	return errors.Join(errs...)
		// }
		// return nil

	case <-ctx.Done():
		return fmt.Errorf("parallel close: %w", ctx.Err())
	}
}
```
