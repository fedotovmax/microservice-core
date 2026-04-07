package zap

import "fmt"

type InvalidEncodingError string

func (ie InvalidEncodingError) Error() string {
	return fmt.Sprintf(
		"invalid encoding: %q, supported encoding: %s, %s",
		string(ie),
		EncodingJSON,
		EncodingPlainText,
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
