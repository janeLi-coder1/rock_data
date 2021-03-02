package main

// initialize three level loggers

import (
	"io"
	"log"
	"os"
)

type customLogger struct {
	debug   *log.Logger
	info    *log.Logger
	warning *log.Logger
	error   *log.Logger
	debugSwitch bool
}

var (
	logger *customLogger
)

func initLogger(logger *customLogger, debugSwitch bool) {
	logDir := "./log"
	_ = os.MkdirAll(logDir, os.ModePerm)
	infoFile, _ := os.OpenFile(logDir+"/service.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	warningFile, _ := os.OpenFile(logDir+"/warning.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	errorFile, _ := os.OpenFile(logDir+"/error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	logger.info = log.New(io.MultiWriter(infoFile, os.Stdout), "INFO: ", log.LstdFlags)
	logger.warning = log.New(io.MultiWriter(infoFile, warningFile), "WARNING: ", log.LstdFlags)
	logger.error = log.New(io.MultiWriter(errorFile, os.Stdout), "ERROR: ", log.LstdFlags)

	if debugSwitch {
		debugFile, _ := os.OpenFile(logDir+"/debug.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		logger.debug = log.New(debugFile, "DEBUG: ", log.LstdFlags)
	}
	logger.debugSwitch = debugSwitch
}

func InitLogger(debugSwitch bool) *customLogger {
	if logger != nil {
		return logger
	}
	logger = &customLogger{}
	initLogger(logger, debugSwitch)
	return logger
}

func ErrorF(format string, i ...interface{}) {
	logger.error.Printf(format, i...)
}

func DebugF(format string, i ...interface{}) {
	if logger.debugSwitch {
		logger.debug.Printf(format, i...)
	}
}

func WarningF(format string, i ...interface{}) {
	logger.warning.Printf(format, i...)
}

func InfoF(format string, i ...interface{}) {
	logger.info.Printf(format, i...)
}

func FatalF(format string, i ...interface{}) {
	logger.error.Fatalf(format, i...)
}
