package log

import (
	"github.com/housepower/ckman/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"strings"
)

var Logger *zap.SugaredLogger

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
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))
	Logger = logger.Sugar()
}

func InitLoggerConsole(){
	cfg := zap.NewProductionConfig()
	cfg.Encoding = "console"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	cfg.OutputPaths = []string{"stdout"}
	logger, _ := cfg.Build()
	Logger = logger.Sugar()
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
