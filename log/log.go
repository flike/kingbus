// Copyright 2018 The kingbus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package log

import (
	"path"
	"strings"

	"github.com/flike/kingbus/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	//LogFileName is the log file prefix name
	LogFileName = "kingbus.log"
	//LogFileMaxSize is the max size of log file
	LogFileMaxSize int = 100 //MB
	//LogMaxBackups is the max backup count of log file
	LogMaxBackups = 20
	//LogMaxAge is the max time to save log file
	LogMaxAge = 28 //days
)

//Log is a global var of log
var Log *zap.SugaredLogger

//Logger is a global var of zap log
var Logger *zap.Logger

// CallerEncoder will add caller to log. format is "filename:lineNum:", e.g:"zaplog/zaplog_test.go:15"
func CallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(strings.Join([]string{caller.TrimmedPath()}, ":"))
}

// InitLoggers the global logger.
func InitLoggers(logDir string, logLevel string) {
	var level zapcore.Level

	if logDir == "" {
		logDir = "./logs"
	}

	logFileName := path.Join(logDir, LogFileName)
	loggerConfig := zap.NewProductionConfig()
	loggerConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	loggerConfig.EncoderConfig.EncodeCaller = CallerEncoder

	err := level.Set(logLevel)
	if err != nil {
		panic("set log level error,err:" + err.Error())
	}

	writer := lumberjack.Logger{
		Filename:   logFileName,
		MaxSize:    LogFileMaxSize, // megabytes
		MaxBackups: LogMaxBackups,
		MaxAge:     LogMaxAge, // days
		LocalTime:  true,
	}
	err = writer.Rotate()
	if err != nil {
		panic("writer rotate error,err:" + err.Error())
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(loggerConfig.EncoderConfig),
		zapcore.AddSync(&writer),
		level,
	)

	Logger = zap.New(core,
		zap.Option(zap.AddCaller()),
	)
	zap.RedirectStdLog(Logger)
	//defer undo()
	Log = Logger.Sugar()
}

//UnInitLoggers implements sync log before quit
func UnInitLoggers() {
	err := Log.Sync()
	if err != nil {
		panic(err)
	}
}

//Writer is a interface of the web framework:echo log writer
type Writer struct {
	LogFunc func(msg string, fields ...zapcore.Field)
}

//NewWriter create a Writer
func NewWriter() *Writer {
	return &Writer{LogFunc: Logger.WithOptions(
		zap.AddCallerSkip(2 + 2),
	).Info}
}

//Write implements the Write interface
func (l *Writer) Write(p []byte) (int, error) {
	l.LogFunc(utils.BytesToString(p))
	return len(p), nil
}
