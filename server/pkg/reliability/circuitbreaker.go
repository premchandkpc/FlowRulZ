package reliability

import (
	"context"
	"errors"
)

type CircuitState int

const (
	CircuitClosed CircuitState = iota
	CircuitHalfOpen
	CircuitOpen
)

type CircuitBreaker interface {
	Execute(ctx context.Context, name string, fn func(context.Context) error) error
	State(name string) CircuitState
	Reset(name string)
}

var ErrCircuitOpen = errors.New("circuit breaker is open")
