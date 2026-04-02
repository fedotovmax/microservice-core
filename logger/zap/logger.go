package zap

import (
	"fmt"
	"os"
	"time"

	"github.com/fedotovmax/microservice-core/filesystem"
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
		return nil, ErrInvalidLogLevel()
	}

	if config.LogFolder.Enable {
		return initWithLogFolder(config, zapLevel)
	}

	return initWithoutLogFolder(config, zapLevel)

}

func (l *Logger) Stop() {

	if l.file == nil {
		return
	}

	err := l.file.Close()

	if err != nil {
		fmt.Printf("error when close log file, please, do not use this instance for log, use stdlib after that, error: %v\n", err)
	}
}

func initWithLogFolder(config Config, zapLevel zap.AtomicLevel) (logger.Logger, error) {

	const op = "core.logger.zap.initWithLogFolder"

	if config.LogFolder.Path == "" {
		return nil, fmt.Errorf("%s: log folder path is empty", op)
	}

	if err := os.MkdirAll(config.LogFolder.Path, 0755); err != nil {
		return nil, fmt.Errorf("%s: error when try to create log folder: %w", op, err)
	}

	timestamp := time.Now().UTC().Format("2006-01-02T15-04-05.000000")

	logFilePath, err := filesystem.SafeJoin(config.LogFolder.Path, fmt.Sprintf("%s.log", timestamp))

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("%s: error when create log file: %w", op, err)
	}

	encoder, err := chooseEnv(config.Env)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
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

func initWithoutLogFolder(config Config, zapLevel zap.AtomicLevel) (logger.Logger, error) {

	const op = "core.logger.zap.initWithoutLogFolder"

	encoder, err := chooseEnv(config.Env)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	//consoleDebugging := zapcore.Lock(os.Stdout)
	consoleDebugging := zapcore.AddSync(os.Stdout)

	core := zapcore.NewTee(zapcore.NewCore(encoder, consoleDebugging, zapLevel))

	l := zap.New(core)

	return &Logger{Logger: l}, nil
}

func chooseEnv(env Env) (zapcore.Encoder, error) {

	const op = "core.logger.zap.chooseEnv"

	var (
		encoder zapcore.Encoder
	)

	switch env {
	case EnvDevelopment:
		encoderConfig := zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	case EnvProduction:
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	default:
		return nil, fmt.Errorf("%s: %w", op, ErrInvalidEnv())
	}

	return encoder, nil

}
