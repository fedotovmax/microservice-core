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

type zapLogger struct {
	*zap.Logger
	atom zap.AtomicLevel
	file *os.File
}

func New(config Config) (logger.Logger, error) {

	const op = "core.logger.zap.New"

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%s: error when validate config: %w", op, err)
	}

	zapLevel := zap.NewAtomicLevel()

	if err := zapLevel.UnmarshalText([]byte(config.Level)); err != nil {
		return nil, fmt.Errorf("%s: %w", op, InvalidLogLevelError(config.Level))
	}

	if config.LogFolder.Enable {
		l, err := initWithLogFolder(config, zapLevel)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		return l, nil
	}

	l, err := initWithoutLogFolder(config, zapLevel)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return l, nil
}

func (l *zapLogger) SetLevel(level string) error {
	const op = "core.logger.zap.Logger.SetLevel"

	loggerLevel := Level(level)

	if err := loggerLevel.Validate(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if err := l.atom.UnmarshalText([]byte(level)); err != nil {
		return fmt.Errorf("%s: invalid zap level: %w", op, err)
	}
	return nil
}

func (l *zapLogger) Stop() {
	const op = "core.logger.zap.Logger.Stop"

	// Сначала сбрасываем буфер.
	// Ошибку игнорируем через _, так как для stdout она будет всегда.
	_ = l.Logger.Sync()

	if l.file != nil {
		if err := l.file.Close(); err != nil {
			// Логируем в стандартный вывод ошибок,
			// так как сам логер может быть уже нестабилен.
			fmt.Fprintf(os.Stderr, "%s: failed to close log file: %v\n", op, err)
		}
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

	logFileName := fmt.Sprintf("%s.log", timestamp)

	logFilePath := filepath.Join(config.LogFolder.Path, logFileName)

	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("%s: error when create log file: %w", op, err)
	}

	encoder, err := chooseEncoding(config.Encoding)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	consoleDebugging := zapcore.AddSync(os.Stdout)
	fileWriter := zapcore.AddSync(logFile)

	core := zapcore.NewTee(
		zapcore.NewCore(encoder, consoleDebugging, zapLevel),
		zapcore.NewCore(encoder, fileWriter, zapLevel),
	)

	l := zap.New(core)

	return &zapLogger{
		Logger: l,
		atom:   zapLevel,
		file:   logFile,
	}, nil
}

func initWithoutLogFolder(config Config, zapLevel zap.AtomicLevel) (logger.Logger, error) {

	const op = "core.logger.zap.initWithoutLogFolder"

	encoder, err := chooseEncoding(config.Encoding)

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	consoleDebugging := zapcore.AddSync(os.Stdout)

	core := zapcore.NewTee(zapcore.NewCore(encoder, consoleDebugging, zapLevel))

	l := zap.New(core)

	return &zapLogger{Logger: l, atom: zapLevel}, nil
}

func chooseEncoding(encoding Encoding) (zapcore.Encoder, error) {

	const op = "core.logger.zap.chooseEncoding"

	var (
		encoder zapcore.Encoder
	)

	switch encoding {
	case EncodingJSON:
		encoderConfig := zap.NewProductionEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewJSONEncoder(encoderConfig)

	case EncodingPlainText:
		encoderConfig := zap.NewDevelopmentEncoderConfig()
		encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		encoder = zapcore.NewConsoleEncoder(encoderConfig)

	default:
		return nil, fmt.Errorf("%s: %w", op, InvalidEncodingError(encoding))
	}

	return encoder, nil

}
