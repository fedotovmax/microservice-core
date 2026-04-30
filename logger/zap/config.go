package zap

import (
	"fmt"
)

type Level string

func (l Level) String() string { return string(l) }

func (l Level) Validate() error {

	const op = "core.logger.zap.Level.Validate"

	switch l {
	case LevelDebug, LevelInfo, LevelWarning, LevelError, LevelPanic, LevelFatal:
		return nil
	default:
		return fmt.Errorf("%s: %w", op, InvalidLogLevelError(l))
	}
}

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
	LevelError   Level = "error"
	LevelPanic   Level = "panic"
	LevelFatal   Level = "fatal"
)

type Encoding string

func (e Encoding) String() string { return string(e) }

func (e Encoding) Validate() error {

	const op = "core.logger.zap.Encoding.Validate"

	switch e {
	case EncodingJSON, EncodingPlainText:
		return nil
	default:
		return fmt.Errorf("%s: %w", op, InvalidEncodingError(e))
	}
}

const (
	EncodingPlainText Encoding = "plain-text"
	EncodingJSON      Encoding = "json"
)

type LogFolder struct {
	Enable bool
	Path   string
}

func (f LogFolder) Validate() error {

	const op = "core.logger.zap.LogFolder.Validate"

	if f.Enable && f.Path == "" {
		return fmt.Errorf("%s: log folder path is required when log folder is enabled", op)
	}
	return nil
}

type Config struct {
	LogFolder LogFolder
	Level     Level
	Encoding  Encoding
}

func (c Config) Validate() error {

	const op = "core.logger.zap.Config.Validate"

	err := c.LogFolder.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.Level.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.Encoding.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}
