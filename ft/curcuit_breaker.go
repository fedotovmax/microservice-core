package ft

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrServiceUnavailable = errors.New("circuit breaker is open")

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {

	switch s {
	case StateClosed:
		return "Closed"
	case StateOpen:
		return "Open"
	case StateHalfOpen:
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
	OnStateChange    func(from, to State)
}

type CircuitBreaker interface {
	Execute(op func() error) (err error)
}

type circuitBreaker struct {
	settings CBSettings
	mu       sync.Mutex

	state           State
	failureCount    int
	successCount    int
	activeRequests  int
	lastFailureTime time.Time
}

func NewCircuitBreaker(st CBSettings) *circuitBreaker {
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

func (cb *circuitBreaker) setState(newState State) {
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
	case StateOpen:
		if now.Sub(cb.lastFailureTime) > cb.settings.ResetTimeout {
			cb.setState(StateHalfOpen)
			cb.failureCount = 0
			cb.successCount = 0
			cb.activeRequests = 1
			return nil
		}
		return ErrServiceUnavailable
	case StateHalfOpen:
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

	if cb.state == StateHalfOpen {
		cb.activeRequests--
	}

	if err != nil {
		cb.failureCount++
		cb.lastFailureTime = time.Now()
		if cb.state == StateHalfOpen || cb.failureCount >= cb.settings.FailureThreshold {
			cb.setState(StateOpen)
		}
		return
	}

	if cb.state == StateHalfOpen {
		cb.successCount++
		if cb.successCount >= cb.settings.SuccessThreshold {
			cb.setState(StateClosed)
			cb.failureCount = 0
			cb.successCount = 0
		}
	} else {
		cb.failureCount = 0
	}
}
