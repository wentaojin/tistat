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
limitations under the License.∏
*/
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/wentaojin/tistat/config"
	"github.com/wentaojin/tistat/server"
	"github.com/wentaojin/tistat/utils"
	"golang.org/x/sync/errgroup"
)

func main() {
	engine, err := server.NewMySQLEngineDB(&config.Config{
		TiDBConfig: config.TiDBConfig{
			Username:   "root",
			Password:   "",
			Host:       "120.92.106.82",
			Port:       4000,
			MetaSchema: "tistat",
		},
	})
	if err != nil {
		panic(err)
	}

	//columns := []string{"`PROPOSALNO`", "`ITEMNO`", "`RANGE`", "`REMARK`", "`FLAG`"}
	//column := utils.StringsBuilder(" (", strings.Join(columns, ","), ")")
	//
	//prefixSQL := utils.StringsBuilder(`REPLACE INTO `, `marvin`, ".", `PRPTITEMPLANE`, column, ` VALUES ('fg',7,'周末宋\\','ru','df')`)
	//
	//fmt.Println(prefixSQL)
	//
	//if _, err = engine.TiDB.Exec(prefixSQL); err != nil {
	//	panic(err)
	//}

	threads := 4

	if err = Run(threads, engine); err != nil {
		log.Fatal(err)
	}

}

func Run(threads int, engine *server.Engine) error {
	for {

		strArr := []string{"1", "2", "10", "15", "30", "20", "25", "5"}
		strCh := make(chan string, threads)
		g := &errgroup.Group{}

		//for _, str := range strArr {
		//	s := str
		//	g.Go(func() error {
		//		if err := QueryContext(engine, utils.StringsBuilder(`SELECT SLEEP(`, s, `)`)); err != nil {
		//			return err
		//		}
		//		return nil
		//	})
		//}

		g.Go(func() error {
			for _, str := range strArr {
				strCh <- str
			}
			close(strCh)
			return nil
		})

		for i := 0; i < threads; i++ {
			workerID := i
			g.Go(func() error {
				for str := range strCh {
					if err := QueryContext(workerID, engine, utils.StringsBuilder(`SELECT SLEEP(`, str, `)`), time.Duration(3)*time.Second); err != nil {
						return err
					}
				}
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}
	}
}

func QueryContext(workerID int, engine *server.Engine, sql string, analyzeTimeout time.Duration) error {
	startTime := time.Now()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), analyzeTimeout*time.Second)
	defer cancel()
	if _, err := engine.TiDB.ExecContext(timeoutCtx, sql); err != nil {
		//检查返回的错误是否等于context.Canceled，如果相等，记录告警。
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			fmt.Printf("workerID: %v, time cost: %v, sql: %v ,error: %v, exit\n", workerID, time.Since(startTime).String(), sql, err)
			//_, r, er := e.Query("show analyze status")
			//if er != nil {
			//	fmt.Println(er)
			//}
			return nil
		default:
			return err
		}
	}
	fmt.Printf("workerID: %v, time cost: %v, sql: %v, success!!!\n", workerID, time.Since(startTime).String(), sql)

	return nil
}
