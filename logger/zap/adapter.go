package zap

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (l *zapLogger) Debug(m string, f ...logger.Field) { l.Logger.Debug(m, toZap(f)...) }
func (l *zapLogger) Info(m string, f ...logger.Field)  { l.Logger.Info(m, toZap(f)...) }
func (l *zapLogger) Warn(m string, f ...logger.Field)  { l.Logger.Warn(m, toZap(f)...) }
func (l *zapLogger) Error(m string, f ...logger.Field) { l.Logger.Error(m, toZap(f)...) }
func (l *zapLogger) Fatal(m string, f ...logger.Field) { l.Logger.Fatal(m, toZap(f)...) }
func (l *zapLogger) With(f ...logger.Field) logger.Logger {
	return &zapLogger{
		Logger: l.Logger.With(toZap(f)...),
		file:   l.file,
		atom:   l.atom,
	}
}

func (l *zapLogger) Ctx(ctx context.Context) logger.Logger {
	spanCtx := trace.SpanContextFromContext(ctx)

	// 1. Если трейса нет, мы всё равно можем прокинуть ctx (иногда OTel
	// хранит там Baggage или другие метаданные), но строковые поля не добавляем.
	if !spanCtx.IsValid() {
		return &zapLogger{
			Logger: l.Logger.With(zap.Any("ctx", ctx)),
			file:   l.file,
			atom:   l.atom,
		}
	}

	// 2. Если трейс есть, добавляем полный "боекомплект"
	return &zapLogger{
		Logger: l.Logger.With(
			// Эти поля пройдут через твой фильтр и красиво напечатаются
			// в консоли и запишутся в файл:
			zap.String("trace_id", spanCtx.TraceID().String()),
			zap.String("span_id", spanCtx.SpanID().String()),

			// Это поле твой фильтр вырежет из консоли/файла,
			// но otelCore (который без фильтра) его получит и обработает:
			zap.Any("ctx", ctx),
		),
		file: l.file,
		atom: l.atom,
	}
}

func toZap(fields []logger.Field) []zap.Field {
	res := make([]zap.Field, len(fields))
	for i, f := range fields {
		switch f.Type {
		case logger.TypeString:
			res[i] = zap.String(f.Key, f.Value.(string))
		case logger.TypeInt:
			res[i] = zap.Int(f.Key, f.Value.(int))
		case logger.TypeInt64: // ДОБАВЛЕНО
			res[i] = zap.Int64(f.Key, f.Value.(int64))
		case logger.TypeFloat64: // ДОБАВЛЕНО
			res[i] = zap.Float64(f.Key, f.Value.(float64))
		case logger.TypeBool:
			res[i] = zap.Bool(f.Key, f.Value.(bool))
		case logger.TypeError:
			res[i] = zap.Error(f.Value.(error))
		case logger.TypeTime:
			res[i] = zap.Time(f.Key, f.Value.(time.Time))
		case logger.TypeDuration:
			res[i] = zap.Duration(f.Key, f.Value.(time.Duration))
		default:
			res[i] = zap.Any(f.Key, f.Value)
		}
	}
	return res
}

type filterCtxCore struct {
	zapcore.Core
}

func (c *filterCtxCore) With(fields []zapcore.Field) zapcore.Core {
	return &filterCtxCore{c.Core.With(filterCtx(fields))}
}

func (c *filterCtxCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return c.Core.Check(entry, ce)
}

func (c *filterCtxCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(entry, filterCtx(fields))
}

func filterCtx(fields []zapcore.Field) []zapcore.Field {
	// 1. Быстро проверяем, есть ли вообще "ctx" в полях
	hasCtx := false
	for i := range fields {
		if fields[i].Key == "ctx" {
			hasCtx = true
			break
		}
	}

	// 2. Если контекста нет (а так будет в 99% случаев),
	// возвращаем оригинальный слайс. НОЛЬ аллокаций памяти!
	if !hasCtx {
		return fields
	}

	// 3. Создаем новый слайс только если "ctx" реально найден
	filtered := make([]zapcore.Field, 0, len(fields)-1)
	for _, f := range fields {
		if f.Key != "ctx" {
			filtered = append(filtered, f)
		}
	}
	return filtered
}
