package logger

import "time"

// Field — универсальная структура для передачи данных в лог
type Field struct {
	Key   string
	Value any
	Type  FieldType
}

type FieldType int

const (
	TypeString FieldType = iota
	TypeInt
	TypeFloat64
	TypeBool
	TypeError
	TypeAny
	TypeTime
	TypeDuration
)

// Logger — основной интерфейс, который будет использовать бизнес-логика
type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Debug(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	With(f ...Field) Logger
	Stop()
}

// Конструкторы полей (Helpers)
func String(key, val string) Field {
	return Field{Key: key, Value: val, Type: TypeString}
}

func Int(key string, val int) Field {
	return Field{Key: key, Value: val, Type: TypeInt}
}

func Float64(key string, val float64) Field {
	return Field{Key: key, Value: val, Type: TypeFloat64}
}

func Err(err error) Field {
	return Field{Key: "error", Value: err, Type: TypeError}
}

func Any(key string, val any) Field {
	return Field{Key: key, Value: val, Type: TypeAny}
}

func Time(key string, val time.Time) Field {
	return Field{Key: key, Value: val, Type: TypeTime}
}

func Duration(key string, val time.Duration) Field {
	return Field{Key: key, Value: val, Type: TypeDuration}
}
