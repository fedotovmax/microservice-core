package conc

import (
	"container/list"
	"context"
	"sync"
)

type semaphoreWaiter struct {
	n     int64
	ready chan<- struct{}
}

func NewWeighted(n int64) *SemaphoreWeighted {
	w := &SemaphoreWeighted{size: n}
	return w
}

type SemaphoreWeighted struct {
	size    int64
	cur     int64
	mu      sync.Mutex
	waiters list.List
}

func (s *SemaphoreWeighted) Acquire(ctx context.Context, n int64) error {
	done := ctx.Done()

	s.mu.Lock()
	select {
	case <-done:

		s.mu.Unlock()
		return ctx.Err()
	default:
	}
	if s.size-s.cur >= n && s.waiters.Len() == 0 {

		s.cur += n
		s.mu.Unlock()
		return nil
	}

	if n > s.size {
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}

	ready := make(chan struct{})
	w := semaphoreWaiter{n: n, ready: ready}
	elem := s.waiters.PushBack(w)
	s.mu.Unlock()

	select {
	case <-done:
		s.mu.Lock()
		select {
		case <-ready:

			s.cur -= n
			s.notifyWaiters()
		default:
			isFront := s.waiters.Front() == elem
			s.waiters.Remove(elem)
			if isFront && s.size > s.cur {
				s.notifyWaiters()
			}
		}
		s.mu.Unlock()
		return ctx.Err()

	case <-ready:

		select {
		case <-done:
			s.Release(n)
			return ctx.Err()
		default:
		}
		return nil
	}
}

func (s *SemaphoreWeighted) TryAcquire(n int64) bool {
	s.mu.Lock()
	success := s.size-s.cur >= n && s.waiters.Len() == 0
	if success {
		s.cur += n
	}
	s.mu.Unlock()
	return success
}

func (s *SemaphoreWeighted) Release(n int64) {
	s.mu.Lock()
	s.cur -= n
	if s.cur < 0 {
		s.mu.Unlock()
		panic("semaphore: released more than held")
	}
	s.notifyWaiters()
	s.mu.Unlock()
}

func (s *SemaphoreWeighted) notifyWaiters() {
	for {
		next := s.waiters.Front()
		if next == nil {
			break
		}

		w := next.Value.(semaphoreWaiter)
		if s.size-s.cur < w.n {

			break
		}

		s.cur += w.n
		s.waiters.Remove(next)
		close(w.ready)
	}
}
