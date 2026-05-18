package ft

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrServiceUnavailable = errors.New("circuit breaker is open")

type CBState int

const (
	CBStateClosed CBState = iota
	CBStateOpen
	CBStateHalfOpen
)

func (s CBState) String() string {

	switch s {
	case CBStateClosed:
		return "Closed"
	case CBStateOpen:
		return "Open"
	case CBStateHalfOpen:
		return "Half-Open"
	default:
		return "Unsupported curcuit breaker state"
	}

}

type CBSettings struct {
	FailureThreshold int
	SuccessThreshold int
	ResetTimeout     time.Duration
	MaxHalfOpenCalls int
	OnStateChange    func(from, to CBState)
	IsIgnorable      func(err error) bool
}

type CircuitBreaker interface {
	Execute(op func() error) (err error)
}

type circuitBreaker struct {
	settings CBSettings
	mu       sync.Mutex

	state           CBState
	failureCount    int
	successCount    int
	activeRequests  int
	lastFailureTime time.Time
}

func NewCircuitBreaker(st CBSettings) CircuitBreaker {
	if st.MaxHalfOpenCalls <= 0 {
		st.MaxHalfOpenCalls = 1
	}
	return &circuitBreaker{settings: st}
}

func (cb *circuitBreaker) Execute(op func() error) (err error) {
	if err = cb.beforeCall(); err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered: %v", r)
			cb.afterCall(err)
			panic(r)
		}
	}()

	err = op()
	cb.afterCall(err)
	return err
}

func (cb *circuitBreaker) setState(newState CBState) {
	if cb.state == newState {
		return
	}
	oldState := cb.state
	cb.state = newState
	if cb.settings.OnStateChange != nil {
		cb.settings.OnStateChange(oldState, newState)
	}
}

func (cb *circuitBreaker) beforeCall() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	switch cb.state {
	case CBStateOpen:
		if now.Sub(cb.lastFailureTime) > cb.settings.ResetTimeout {
			cb.setState(CBStateHalfOpen)
			cb.failureCount = 0
			cb.successCount = 0
			cb.activeRequests = 1
			return nil
		}
		return ErrServiceUnavailable
	case CBStateHalfOpen:
		if cb.activeRequests >= cb.settings.MaxHalfOpenCalls {
			return ErrServiceUnavailable
		}
		cb.activeRequests++
		return nil
	default:
		return nil
	}
}

func (cb *circuitBreaker) afterCall(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	// Освобождаем слот конкурентности для состояния Half-Open
	if cb.state == CBStateHalfOpen {
		cb.activeRequests--
	}

	// 1. Проверяем, является ли ошибка системным сбоем
	isFailure := err != nil
	if isFailure && cb.settings.IsIgnorable != nil && cb.settings.IsIgnorable(err) {
		// Ошибка есть, но мы её прощаем (например, 400 Bad Request или 404 Not Found).
		// Для предохранителя это означает, что целевой сервис ЖИВ и работает штатно.
		isFailure = false
	}

	// 2. Логика СБОЯ
	if isFailure {
		cb.failureCount++
		cb.lastFailureTime = time.Now()

		// Если мы тестировали сервис (Half-Open) и он снова упал,
		// ИЛИ если мы превысили лимит ошибок в нормальном состоянии (Closed)
		if cb.state == CBStateHalfOpen || cb.failureCount >= cb.settings.FailureThreshold {
			cb.setState(CBStateOpen)
		}
		return
	}

	// 3. Логика УСПЕХА (включая "прощенные" ошибки)
	if cb.state == CBStateHalfOpen {
		cb.successCount++
		// Если сервис доказал свою стабильность нужным количеством ответов
		if cb.successCount >= cb.settings.SuccessThreshold {
			cb.setState(CBStateClosed)
			cb.failureCount = 0
			cb.successCount = 0
		}
	} else {
		// Если мы в Closed и запрос успешен (или прощен) - сбрасываем счетчик ошибок,
		// так как мы считаем ошибки "подряд" (Consecutive)
		cb.failureCount = 0
	}
}
