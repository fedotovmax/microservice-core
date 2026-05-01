package conc

import (
	"container/list"
	"context"
	"sync"
)

// semaphoreWaiter представляет собой "заявку" на получение ресурса.
type semaphoreWaiter struct {
	n     int64           // Количество запрошенных единиц ресурса.
	ready chan<- struct{} // Канал для оповещения горутины о том, что ресурс выделен.
}

// NewWeighted создает новый семафор с максимальным весом n.
func NewWeighted(n int64) *SemaphoreWeighted {
	w := &SemaphoreWeighted{size: n}
	return w
}

// SemaphoreWeighted реализует взвешенный семафор.
type SemaphoreWeighted struct {
	size    int64      // Максимально допустимый вес.
	cur     int64      // Текущий используемый вес.
	mu      sync.Mutex // Мьютекс для защиты внутренних полей.
	waiters list.List  // Очередь ожидающих горутин (FIFO).
}

// Acquire пытается захватить n единиц ресурса. Блокируется, пока ресурс не станет доступен или не сработает контекст.
func (s *SemaphoreWeighted) Acquire(ctx context.Context, n int64) error {
	done := ctx.Done()

	s.mu.Lock()
	// Быстрая проверка: если контекст уже отменен, выходим сразу.
	select {
	case <-done:
		s.mu.Unlock()
		return ctx.Err()
	default:
	}

	// Fast path: если есть место и никто не стоит в очереди перед нами — забираем ресурс.
	if s.size-s.cur >= n && s.waiters.Len() == 0 {
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	// Если запрос больше, чем общая емкость семафора, выполнение никогда не завершится успешно.
	// Блокируемся до отмены контекста.
	if n > s.size {
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}

	// Slow path: создаем канал готовности, добавляем себя в очередь и разблокируем мьютекс.
	ready := make(chan struct{})
	w := semaphoreWaiter{n: n, ready: ready}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	// Ждем либо готовности ресурса, либо отмены контекста.
	select {
	case <-done:
		s.mu.Lock()
		select {
		case <-ready:
			// Если мы получили сигнал о готовности одновременно с отменой контекста,
			// считаем, что ресурс захвачен, но тут же его освобождаем.
			s.cur -= n
			s.notifyWaiters()
		default:
			// Если ресурс еще не был выделен, просто удаляем себя из очереди.
			isFront := s.waiters.Front() == elem
			s.waiters.Remove(elem)
			// Если мы были первыми в очереди, возможно, теперь другие могут пройти.
			if isFront && s.size > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return ctx.Err()

	case <-ready:
		// Ресурс успешно выделен через notifyWaiters.
		select {
		case <-done:
			// Если контекст закрылся в момент получения ресурса, освобождаем его.
			s.Release(n)
			return ctx.Err()
		default:
		}
		return nil
	}
}

// TryAcquire — неблокирующая попытка захватить ресурс.
func (s *SemaphoreWeighted) TryAcquire(n int64) bool {
	s.mu.Lock()
	// Проверяем, хватает ли места и нет ли очереди (соблюдаем порядок).
	success := s.size-s.cur >= n && s.waiters.Len() == 0
	if success {
		s.cur += n
	}
	s.mu.Unlock()
	return success
}

// Release освобождает n единиц ресурса.
func (s *SemaphoreWeighted) Release(n int64) {
	s.mu.Lock()
	s.cur -= n
	// Защита от неправильного использования (освободили больше, чем взяли).
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	// Пытаемся разбудить тех, кто ждет в очереди.
	s.notifyWaiters()
	s.mu.Unlock()
}

// notifyWaiters проходит по очереди и выделяет ресурс ожидающим, пока это возможно.
func (s *SemaphoreWeighted) notifyWaiters() {
	for {
		next := s.waiters.Front()
		if next == nil {
			break // Очередь пуста.
		}

		w := next.Value.(semaphoreWaiter)
		// Если первому в очереди не хватает места, мы не идем дальше,
		// чтобы не "перепрыгивать" через него (строгий FIFO).
		if s.size-s.cur < w.n {
			break
		}

		// Выделяем ресурс, удаляем из очереди и закрываем канал ready.
		s.cur += w.n
		s.waiters.Remove(next)
		close(w.ready)
	}
}
