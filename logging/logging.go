package logging

import (
	"encoding/json"
	"github.com/afex/hystrix-go/hystrix"
	"log"
	"os"
	"strings"

	"gopkg.in/natefinch/lumberjack.v2"
)

//AppenderType identifies nature of logs
type AppenderType string

const (
	Console AppenderType = "console"
	File AppenderType = "file"
)

//LogConf holds logging configuration
type LogConf struct {
	Type   AppenderType `json:"type"`
	Config interface{}  `json:"config"`
}

type ConsoleBasedConf struct {
	EnableDebug bool       `json:"enable_debug"`
}

type FileBasedConf struct {
	Path        string     `json:"path"`
	EnableDebug bool       `json:"enable_debug"`
	Rotate      RotateConf `json:"rotation"`
}

//RotateConf holds logging Rotation configuration
type RotateConf struct {
	FileSize     int  `json:"size_in_mb"`
	RotationDays int  `json:"retention_days"`
	NoOfFiles    int  `json:"retention_count"`
	Compress     bool `json:"compress"`
}

//DMuxLogging logging
type DMuxLogging struct{
	EnableDebug bool `json:"enable_debug"`
}

//Overriding the print function for circuit breaker logging
func (c *DMuxLogging) Printf(format string, items ...interface{}) {
	if res:= strings.HasSuffix(format,"capacity");!res{
		log.Printf(format, items...)
	}
}

//Start starting logging
func (c *DMuxLogging) Start(logConf LogConf) {
	switch logConf.Type {
	case Console:
		// handling for console
		log.SetOutput(os.Stdout)
		data, _ := json.Marshal(logConf.Config)
		var config *ConsoleBasedConf
		json.Unmarshal(data, &config)
		c.EnableDebug = config.EnableDebug
	case File:
		data, _ := json.Marshal(logConf.Config)
		var config *FileBasedConf
		json.Unmarshal(data, &config)
		log.SetOutput(&lumberjack.Logger{
			Filename:   config.Path,
			MaxSize:    config.Rotate.FileSize, // megabytes
			MaxBackups: config.Rotate.NoOfFiles,
			MaxAge:     config.Rotate.RotationDays, //days
			Compress:   config.Rotate.Compress,     // disabled by default
		})
		c.EnableDebug = config.EnableDebug
	default:
		panic("Invalid logger type")
	}

	//set the logger for circuit breaker
	hystrix.SetLogger(c)
}
