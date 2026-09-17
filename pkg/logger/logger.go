/*
Copyright © 2024 SUSE LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logger

import (
	"sync"

	"github.com/sirupsen/logrus"
)

type LogLevel int

const (
	DebugLevel LogLevel = iota
	InfoLevel
	WarningLevel
	ErrorLevel
)

func ParseLogLevel(level string) LogLevel {
	switch level {
	case "debug":
		return DebugLevel
	case "info":
		return InfoLevel
	case "warn":
		return WarningLevel
	case "error":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

type singletonLogger struct {
	logger *logrus.Logger
}

var (
	instance *singletonLogger
	once     sync.Once
)

// getInstance ensures the logger is initialized only once
func getInstance() *singletonLogger {
	once.Do(func() {
		l := logrus.New()

		// Configure logrus default settings
		l.SetFormatter(&logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006/01/02 15:04:05",
		})
		l.SetLevel(logrus.InfoLevel) // Default level

		instance = &singletonLogger{
			logger: l,
		}
	})
	return instance
}

// SetLevel updates the verbosity level
func SetLevel(level LogLevel) {
	inst := getInstance()
	switch level {
	case DebugLevel:
		inst.logger.SetLevel(logrus.DebugLevel)
	case InfoLevel:
		inst.logger.SetLevel(logrus.InfoLevel)
	case WarningLevel:
		inst.logger.SetLevel(logrus.WarnLevel)
	case ErrorLevel:
		inst.logger.SetLevel(logrus.ErrorLevel)
	}
}

func Debug(format string, v ...any) {
	getInstance().logger.Debugf(format, v...)
}

func Info(format string, v ...any) {
	getInstance().logger.Infof(format, v...)
}

func Warning(format string, v ...any) {
	getInstance().logger.Warnf(format, v...)
}

func Error(format string, v ...any) {
	getInstance().logger.Errorf(format, v...)
}
