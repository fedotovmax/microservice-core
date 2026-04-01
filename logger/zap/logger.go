package zap

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Logger struct {
	*zap.Logger

	file *os.File
}

func New(config Config) (logger.Logger, error) {

	zapLevel := zap.NewAtomicLevel()

	if err := zapLevel.UnmarshalText([]byte(config.Level)); err != nil {
		return nil, fmt.Errorf("invalid log level: %w: %v", ErrInvalidLevel, err)
	}

	if err := os.MkdirAll(config.LogFolderPath, 0755); err != nil {
		return nil, fmt.Errorf("error when try to create log folder: %w", err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05.000000")

	logFilePath := filepath.Join(config.LogFolderPath, fmt.Sprintf("%s.log", timestamp))

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("error when create log file: %w", err)
	}

	var (
		encoder       zapcore.Encoder
		encoderConfig zapcore.EncoderConfig
	)

	switch config.Env {
	case EnvDevelopment:
		encoderConfig = zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	case EnvProduction:
		encoderConfig = zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("unsopported env value in logger config: %s", config.Env)
	}

	//consoleDebugging := zapcore.Lock(os.Stdout)
	consoleDebugging := zapcore.AddSync(os.Stdout)
	fileWriter := zapcore.AddSync(logFile)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, consoleDebugging, zapLevel),
		zapcore.NewCore(encoder, fileWriter, zapLevel),
	)

	l := zap.New(core)

	return &Logger{
		Logger: l,
		file:   logFile,
	}, nil

}

func (l *Logger) Stop() {

	err := l.file.Close()

	if err != nil {
		fmt.Println("error when close log file, please, do not use this instance for log, use stdlib after that, error: %w", err)
	}
}
