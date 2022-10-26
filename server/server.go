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
package server

import (
	"github.com/robfig/cron/v3"
	"github.com/wentaojin/tistat/config"
)

func Run(mysqlCfg *config.Config) error {
	engine, err := NewMySQLEngineDB(mysqlCfg)
	if err != nil {
		return err
	}

	if err = engine.InitMySQLEngineDB(); err != nil {
		return err
	}

	// 固定时间统计信息收集
	c := cron.New(cron.WithSeconds())

	c.AddFunc(mysqlCfg.AppConfig.Crontab, func() {
		analyzeTasks, err := AnalyzeTaskFilter(mysqlCfg, engine)
		if err != nil {
			if err = NewEmail(mysqlCfg.AlertConfig.ReceiveEmails, `TiStat Analyze Program Panic`, `Please Check TiStat Analyze Program`).SendEmail(mysqlCfg.AlertConfig); err != nil {
				panic(err)
			}
			panic(err)
		}
		if len(analyzeTasks) > 0 {
			job := NewScheduleJob(mysqlCfg.AppConfig.TableThreads)
			job.Push(analyzeTasks)

			job.Run()

			if err = job.Wait(); err != nil {
				if err = NewEmail(mysqlCfg.AlertConfig.ReceiveEmails, `TiStat Analyze Program Panic`, `Please Check TiStat Analyze Program`).SendEmail(mysqlCfg.AlertConfig); err != nil {
					panic(err)
				}
				panic(err)
			}
		}
	})

	c.Start()

	select {}
}
