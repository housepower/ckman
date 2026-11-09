package log

import (
	"fmt"
	"strings"

	"github.com/housepower/ckman/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Logger *zap.SugaredLogger
var ZapLog *zap.Logger

func InitLogger(path string, config *config.CKManLogConfig) {
	errPath := strings.TrimSuffix(path, ".log") + ".err.log"
	writeSyncer := getLogWriter(path, config)
	errSyncer := getLogWriter(errPath, config)
	encoder := getEncoder()
	level := zapcore.InfoLevel
	_ = level.UnmarshalText([]byte(config.Level))
	infocore := zapcore.NewCore(encoder, writeSyncer, level)
	errcore := zapcore.NewCore(encoder, errSyncer, zapcore.ErrorLevel)
	core := zapcore.NewTee(infocore, errcore)
	ZapLog = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	Logger = ZapLog.Sugar()
}

func InitLoggerDefault(level string, paths []string) error {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	zaplevel := zapcore.InfoLevel
	zaplevel.UnmarshalText([]byte(level))
	cfg.Level.SetLevel(zaplevel)
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	if len(paths) == 0 {
		paths = []string{"stdout"}
	}
	cfg.OutputPaths = paths
	logger, err := cfg.Build()
	if err != nil {
		// Command-line tools should remain usable when their configured log path
		// is not writable (for example, when a non-root user runs ckmanctl and
		// cannot create /var/log/ckmanctl.log). Keep logging available on stderr
		// and let the caller report the original error.
		cfg.OutputPaths = []string{"stderr"}
		logger, fallbackErr := cfg.Build()
		if fallbackErr != nil {
			ZapLog = zap.NewNop()
			Logger = ZapLog.Sugar()
			return fmt.Errorf("initialize log output %v: %w; initialize stderr fallback: %v", paths, err, fallbackErr)
		}
		ZapLog = logger
		Logger = ZapLog.Sugar()
		return fmt.Errorf("initialize log output %v: %w", paths, err)
	}
	ZapLog = logger
	Logger = ZapLog.Sugar()
	return nil
}

func InitLoggerConsole() {
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	cfg.OutputPaths = []string{"stdout"}
	ZapLog, _ = cfg.Build()
	Logger = ZapLog.Sugar()
}

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter(path string, config *config.CKManLogConfig) zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   path,
		MaxSize:    config.MaxSize,
		MaxBackups: config.MaxCount,
		MaxAge:     config.MaxAge,
		LocalTime:  true,
	}
	return zapcore.AddSync(lumberJackLogger)
}
