/*
Copyright © 2020 Marvin

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
package config

import (
	"encoding/json"
	"fmt"

	"github.com/BurntSushi/toml"
)

type Config struct {
	AppConfig   AppConfig   `toml:"app" json:"app"`
	TiDBConfig  TiDBConfig  `toml:"tidb" json:"tidb"`
	AlertConfig AlertConfig `toml:"alert" json:"alert"`
	LogConfig   LogConfig   `toml:"log" json:"log"`
}

type AppConfig struct {
	InsertBatchSize       int    `toml:"insert-batch-size" json:"insert-batch-size"`
	SlowLogThreshold      int    `toml:"slowlog-threshold" json:"slowlog-threshold"`
	PprofPort             string `toml:"pprof-port" json:"pprof-port"`
	AnalyzeTimeout        int    `toml:"analyze-timeout" json:"analyze-timeout"`
	TableThreads          int    `toml:"table-threads" json:"table-threads"`
	StatsOutDir           string `toml:"stats-outdir" json:"stats-outdir"`
	StatsGCDays           int    `toml:"stats-gc-days" json:"stats-gc-days"`
	SmallTableSamplerate  string `toml:"small-table-samplerate" json:"small-table-samplerate"`
	MediumTableSamplerate string `toml:"medium-table-samplerate" json:"medium-table-samplerate"`
	BigTableSamplerate    string `toml:"big-table-samplerate" json:"small-table-big-table-samplerate"`
	Crontab               string `toml:"crontab" json:"crontab"`
}

type TiDBConfig struct {
	Username      string `toml:"username" json:"username"`
	Password      string `toml:"password" json:"password"`
	Host          string `toml:"host" json:"host"`
	Port          int    `toml:"port" json:"port"`
	StatusPort    int    `toml:"status-port" json:"status-port"`
	ConnectParams string `toml:"connect-params" json:"connect-params"`
	MetaSchema    string `toml:"meta-schema" json:"meta-schema"`
}

type AlertConfig struct {
	SmtpHost         string   `toml:"smtp-host" json:"smtp-host"`
	SmtpPort         int      `toml:"smtp-port" json:"smtp-port"`
	SendEmail        string   `toml:"send-email" json:"send-email"`
	SendEmailAuthPWD string   `toml:"send-email-auth-pwd" json:"send-email-auth-pwd"`
	ReceiveEmails    []string `toml:"receive-emails" json:"receive-emails"`
}

type LogConfig struct {
	LogLevel   string `toml:"log-level" json:"log-level"`
	LogFile    string `toml:"log-file" json:"log-file"`
	MaxSize    int    `toml:"max-size" json:"max-size"`
	MaxDays    int    `toml:"max-days" json:"max-days"`
	MaxBackups int    `toml:"max-backups" json:"max-backups"`
}

// 读取配置文件
func ReadConfigFile(file string) (*Config, error) {
	cfg := &Config{}
	if err := cfg.configFromFile(file); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// 加载配置文件并解析
func (c *Config) configFromFile(file string) error {
	if _, err := toml.DecodeFile(file, c); err != nil {
		return fmt.Errorf("failed decode toml config file %s: %v", file, err)
	}
	return nil
}

func (c *Config) String() string {
	cfg, err := json.Marshal(c)
	if err != nil {
		return "<nil>"
	}
	return string(cfg)
}
