package conc

import (
	"context"
	"sync"
)

// Result упаковывает ответ, чтобы вызывающий код мог обработать ошибку
type WorkerpoolResult[R any] struct {
	Value R
	Err   error
}

// Workerpool запускает N воркеров для обработки данных из канала `in`
func Workerpool[T, R any](
	ctx context.Context,
	in <-chan T,
	workersNum int,
	f func(ctx context.Context, e T) (R, error),
) <-chan WorkerpoolResult[R] {
	// Буферизация выходного канала позволяет воркерам не блокироваться сразу,
	// если чтение результатов идет чуть медленнее
	//resultChan := make(chan WorkerpoolResult[R], workersNum)
	resultChan := make(chan WorkerpoolResult[R])
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
