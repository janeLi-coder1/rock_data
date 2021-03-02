package main

// read configuration file

import (
	"github.com/BurntSushi/toml"
)

type Config struct {
	PipelineNumber int       `toml:"pipelineNumber"`
	PipeCapacity   int       `toml:"pipeCapacity"`
	DebugSwitch    bool      `toml:"debugSwitch"`
	Database       *Database `toml:"database"`
}

type Database struct {
	Ip                   string `toml:"ip"`
	Port                 int    `toml:"port"`
	Db                   string `toml:"db"`
	User                 string `toml:"user"`
	Password             string `toml:"password"`
	Schema               string `toml:"schema"`
	Table                string `toml:"table"`
	MaxMultiInsertNumber int    `toml:"maxMultiInsertNumber"`
}

var config *Config

func GetConfig() (*Config, error) {
	if config != nil {
		return config, nil
	}

	_, err := toml.DecodeFile("./config.toml", &config)
	return config, err
}
