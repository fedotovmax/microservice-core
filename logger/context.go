package logger

import "context"

type loggerCtxKey struct{}

var loggerCtxKeyValue = loggerCtxKey{}

func FromContext(ctx context.Context) Logger {
	log, ok := ctx.Value(loggerCtxKeyValue).(Logger)

	if !ok {
		panic("no logger in context")
	}

	return log
}

func ToContext(ctx context.Context, log Logger) context.Context {
	return context.WithValue(
		ctx,
		loggerCtxKeyValue,
		log,
	)
}
