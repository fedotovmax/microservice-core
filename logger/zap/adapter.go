package zap

import (
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"go.uber.org/zap"
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
