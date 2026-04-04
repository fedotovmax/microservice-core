package zap

import "fmt"

type InvalidEnvError string

func (ie InvalidEnvError) Error() string {
	return fmt.Sprintf(
		"invalid env: %q, supported env: %s, %s",
		string(ie),
		EnvDevelopment,
		EnvProduction,
	)
}

type InvalidLogLevelError string

func (ill InvalidLogLevelError) Error() string {
	return fmt.Sprintf(
		"invalid log level: %q, supported levels: %s, %s, %s, %s, %s, %s",
		string(ill),
		LevelDebug,
		LevelInfo,
		LevelWarning,
		LevelError,
		LevelPanic,
		LevelFatal,
	)
}
