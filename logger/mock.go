package logger

import "context"

type Mock struct{}

func NewMock() *Mock {
	return &Mock{}
}

func (n *Mock) Info(msg string, fields ...Field)  {}
func (n *Mock) Error(msg string, fields ...Field) {}
func (n *Mock) Debug(msg string, fields ...Field) {}
func (n *Mock) Warn(msg string, fields ...Field)  {}
func (n *Mock) Stop()                             {}
func (n *Mock) SetLevel(level string) error {
	return nil
}

func (n *Mock) With(f ...Field) Logger {
	return n
}

func (n *Mock) Ctx(ctx context.Context) Logger {
	return n
}
