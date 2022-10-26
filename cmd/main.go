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
package main

import (
	"flag"
	"log"
	"net/http"
	"os"

	"github.com/pkg/errors"
	"github.com/wentaojin/tistat/config"
	"github.com/wentaojin/tistat/logger"
	"github.com/wentaojin/tistat/server"
	"go.uber.org/zap"
)

var (
	conf    = flag.String("config", "config.toml", "specify the configuration file, default is config.toml")
	version = flag.Bool("version", false, "view transferdb version info")
)

func main() {
	flag.Parse()

	// 获取程序版本
	server.GetAppVersion(*version)

	// 读取配置文件
	cfg, err := config.ReadConfigFile(*conf)
	if err != nil {
		log.Fatalf("read config file [%s] failed: %v", *conf, err)
	}

	go func() {
		if err = http.ListenAndServe(cfg.AppConfig.PprofPort, nil); err != nil {
			zap.L().Fatal("listen and serve pprof failed", zap.Error(errors.Cause(err)))
		}
		os.Exit(0)
	}()

	// 初始化日志 logger
	logger.NewZapLogger(cfg)
	server.RecordAppVersion("tistat", zap.L(), cfg)

	// 程序运行
	if err = server.Run(cfg); err != nil {
		zap.L().Fatal("server run failed", zap.Error(errors.Cause(err)))
	}
}
