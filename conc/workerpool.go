package conc

import (
	"context"
	"sync"
)

type WorkerpoolResult[R any] struct {
	Value R
	Err   error
}

func Workerpool[T, R any](
	ctx context.Context,
	in <-chan T,
	workersNum int,
	f func(ctx context.Context, e T) (R, error),
) <-chan WorkerpoolResult[R] {
	resultChan := make(chan WorkerpoolResult[R], workersNum)
	wg := &sync.WaitGroup{}

	for range workersNum {
		wg.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case value, ok := <-in:
					if !ok {
						return
					}

					val, err := f(ctx, value)

					res := WorkerpoolResult[R]{Value: val, Err: err}

					select {
					case resultChan <- res:
					case <-ctx.Done():
						return
					}
				}
			}
		})
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	return resultChan
}
