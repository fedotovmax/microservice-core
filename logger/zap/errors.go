package zap

import "fmt"

func ErrInvalidLogLevel() error {
	return fmt.Errorf(
		"invalid log level, supported levels: %s, %s, %s, %s, %s, %s",
		LevelDebug,
		LevelInfo,
		LevelWarning,
		LevelError,
		LevelPanic,
		LevelFatal,
	)
}

func ErrInvalidEnv() error {
	return fmt.Errorf(
		"invalid env, supported env: %s, %s",
		EnvDevelopment,
		EnvProduction,
	)
}
