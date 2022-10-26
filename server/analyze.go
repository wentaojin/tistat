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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/tistat/config"
	"github.com/wentaojin/tistat/utils"
	"go.uber.org/zap"
)

func AnalyzeTaskFilter(mysqlCfg *config.Config, engine *Engine) ([]AnalyzeTask, error) {
	var analyzeTasks []AnalyzeTask
	allTables, err := engine.GetMySQLDBALLTables(mysqlCfg.TiDBConfig.MetaSchema)
	if err != nil {
		return analyzeTasks, err
	}

	partitionTables, partitionTablesDetail, err := engine.GetMySQLDBPartitionTables(mysqlCfg.TiDBConfig.MetaSchema)
	if err != nil {
		return analyzeTasks, err
	}

	// 上次收集超时未完成的表列表 analyze-timeout
	nonPartitionTablesWait, partitionTablesWait, allWaitTables, err := engine.GetWaitStatsTableALLRows()
	if err != nil {
		return analyzeTasks, err
	}

	// 存在 analyze-timeout 超时表记录，发送告警
	if len(allWaitTables) > 0 {
		if err = NewEmail(
			mysqlCfg.AlertConfig.ReceiveEmails,
			`Analyze Table Stats Timeout List`,
			RenderAlertTemplateMSG(allWaitTables)).SendEmail(mysqlCfg.AlertConfig); err != nil {
			return analyzeTasks, err
		}
		zap.L().Info("analyze table timeout send email",
			zap.String("send", mysqlCfg.AlertConfig.SendEmail),
			zap.Strings("receivers", mysqlCfg.AlertConfig.ReceiveEmails),
			zap.String("status", "success"))
	}

	/*
		1、获取待收集队列任务表 -> 满足统计信息收集触发条件的表
		2、排除上次 analyze-timeout 表，需要人工排查为什么收集失败
		- 是否是因为 analyze-timeout 设置过小或者其他原因
		- 如果是因为 analyze-timeout 设置过小，可人为调大，手工将下游元数据库内 wait_stats_table 对应表记录删除，下次任务将自动加入收集队列
	*/
	var (
		needTables []string
	)

	// 非分区表 - 排除上次 analyze-timeout 失败任务表
	nonPartitionAllTables := utils.FilterDifferenceStringItems(allTables, partitionTables)
	if len(utils.FilterDifferenceStringItems(nonPartitionAllTables, nonPartitionTablesWait)) > 0 {
		needTables = append(needTables, utils.FilterDifferenceStringItems(nonPartitionAllTables, nonPartitionTablesWait)...)
	}

	// 分区表 - 排除上次 analyze-timeout 失败任务表
	if len(utils.FilterDifferenceStringItems(partitionTablesDetail, partitionTablesWait)) > 0 {
		needTables = append(needTables, utils.FilterDifferenceStringItems(partitionTablesDetail, partitionTablesWait)...)
	}

	if len(needTables) > 0 {
		zap.L().Info("analyze table task filter",
			zap.Strings("analyze task tables", needTables),
			zap.Int("analyze task table counts", len(needTables)),
			zap.String("status", "running"))
		analyzeTasks, err = AnalyzeTableAdjust(engine, mysqlCfg, needTables)
		if err != nil {
			return analyzeTasks, err
		}
	} else {
		zap.L().Warn("analyze table task filter",
			zap.Strings("analyze task tables", needTables),
			zap.Int("analyze task table counts", len(needTables)),
			zap.String("status", "skip"))
	}

	return analyzeTasks, nil
}

func AnalyzeTableAdjust(engine *Engine, mysqlCfg *config.Config, needTables []string) ([]AnalyzeTask, error) {
	var (
		analyzeTasks []AnalyzeTask
		statsTables  []string
	)

	dbVersion, err := engine.GetMySQLDBVersion()
	if err != nil {
		return analyzeTasks, err
	}

	tableHealthyMap, err := engine.GetTableStatsHealthy()
	if err != nil {
		return analyzeTasks, err
	}

	tableModifyMap, tableModifyThresholdMap, tableRowsTotalMap, err := engine.GetTableStatsMeta()
	if err != nil {
		return analyzeTasks, err
	}

	// 统计信息自定义规则加载
	statsHealthyMap, statsModifyMap, err := engine.GetStatsTriggerRuleTableALLRows()
	if err != nil {
		return analyzeTasks, err
	}

	statsComplexMap, statsSamplerateMap, statsOutdirMap, err := engine.GetStatsAnalyzeRuleTableALLRows()
	if err != nil {
		return analyzeTasks, err
	}

	// 统计信息触发规则 -> 表统计信息收集清单
	for _, t := range needTables {
		if StatsTableHealthyRule(t, tableHealthyMap, statsHealthyMap) || StatsTableModifyZeroRule(t, tableModifyMap) || StatsTableModifyCountRule(t, tableModifyThresholdMap, statsModifyMap) {
			statsTables = append(statsTables, t)
		}
	}

	for _, table := range statsTables {
		var (
			analyzeSQL       string
			analyzeSQLSuffix string
			statsOutdir      string
		)
		// SAMPLERATE表采样率
		analyzeSQLSuffix = StatsTableAnalyzeSamplerateRule(table, dbVersion, mysqlCfg.AppConfig.SmallTableSamplerate, mysqlCfg.AppConfig.MediumTableSamplerate, mysqlCfg.AppConfig.BigTableSamplerate, statsSamplerateMap, tableRowsTotalMap)

		// 统计信息备份目录
		statsOutdir = StatsTableBackupOutdirRule(table, statsOutdirMap, mysqlCfg.AppConfig.StatsOutDir)

		// 统计信息收集设置
		statsThreadsSQL, analyzeTimeout := StatsTableAnalyzeConcurrencyRule(table, statsComplexMap, mysqlCfg.AppConfig.AnalyzeTimeout)

		if strings.Contains(table, "PARTITION") {
			splitPartition := strings.Split(table, "PARTITION")
			schemaName := strings.Split(splitPartition[0], ".")[0]
			tableName := strings.Split(splitPartition[1], ".")[1]
			partitionName := splitPartition[1]

			analyzeSQL = utils.StringsBuilder("ANALYZE TABLE ", schemaName, ".", tableName, " PARTITION ", partitionName, analyzeSQLSuffix)
			statsThreadsSQL = append(statsThreadsSQL, analyzeSQL)

			analyzeTasks = append(analyzeTasks, AnalyzeTask{
				RunningUser:    mysqlCfg.TiDBConfig.Username,
				Host:           mysqlCfg.TiDBConfig.Host,
				Port:           mysqlCfg.TiDBConfig.Port,
				StatusPort:     mysqlCfg.TiDBConfig.StatusPort,
				DBVersion:      dbVersion,
				SchemaName:     schemaName,
				TableName:      tableName,
				PartitionName:  partitionName,
				RowCounts:      tableRowsTotalMap[tableName],
				StatsOutdir:    statsOutdir,
				StatsGCDays:    mysqlCfg.AppConfig.StatsGCDays,
				AnalyzeTimeout: analyzeTimeout,
				Engine:         engine,
				AnalyzeSQL:     statsThreadsSQL,
			})
		} else {
			schemaName := strings.Split(table, ".")[0]
			tableName := strings.Split(table, ".")[1]

			analyzeSQL = utils.StringsBuilder("ANALYZE TABLE ", schemaName, ".", tableName, analyzeSQLSuffix)
			statsThreadsSQL = append(statsThreadsSQL, analyzeSQL)

			analyzeTasks = append(analyzeTasks, AnalyzeTask{
				RunningUser:    mysqlCfg.TiDBConfig.Username,
				Host:           mysqlCfg.TiDBConfig.Host,
				Port:           mysqlCfg.TiDBConfig.Port,
				StatusPort:     mysqlCfg.TiDBConfig.StatusPort,
				DBVersion:      dbVersion,
				SchemaName:     schemaName,
				TableName:      tableName,
				PartitionName:  "",
				RowCounts:      tableRowsTotalMap[tableName],
				StatsOutdir:    statsOutdir,
				StatsGCDays:    mysqlCfg.AppConfig.StatsGCDays,
				AnalyzeTimeout: analyzeTimeout,
				Engine:         engine,
				AnalyzeSQL:     statsThreadsSQL,
			})
		}
	}
	return analyzeTasks, nil
}

func AnalyzeTableStats(workerID int, task AnalyzeTask) error {
	startTime := time.Now()
	zap.L().Info("analyze task started", zap.String("task", task.String()), zap.String("start time", startTime.String()))

	// 统计信息相关记录收集以及备份 - before
	if err := task.Engine.UpsertSingleTableStatsMeta(workerID, task.SchemaName, task.TableName, task.PartitionName, utils.BeforeUpsertStatus); err != nil {
		return err
	}
	if err := task.Engine.UpsertSingleTableStatsHealthy(workerID, task.SchemaName, task.TableName, task.PartitionName, utils.BeforeUpsertStatus); err != nil {
		return err
	}
	if err := task.Engine.UpsertSingleTableStatsHistograms(workerID, task.SchemaName, task.TableName, task.PartitionName, utils.BeforeUpsertStatus); err != nil {
		return err
	}

	// 统计信息备份
	if err := StatsTableDumpFile(
		workerID,
		task.Host,
		task.StatusPort,
		task.SchemaName,
		task.TableName,
		task.PartitionName,
		filepath.Join(
			task.StatsOutdir,
			task.SchemaName,
			task.TableName,
		)); err != nil {
		return err
	}

	// 统计信息收集
	err := task.Engine.RunAnalyzeTask(workerID, task)
	if err != nil {
		return err
	}
	// 统计信息相关记录收集以及备份 - after
	if err = task.Engine.UpsertSingleTableStatsMeta(workerID, task.SchemaName, task.TableName, task.PartitionName, utils.AfterUpsertStatus); err != nil {
		return err
	}
	if err = task.Engine.UpsertSingleTableStatsHealthy(workerID, task.SchemaName, task.TableName, task.PartitionName, utils.AfterUpsertStatus); err != nil {
		return err
	}
	if err = task.Engine.UpsertSingleTableStatsHistograms(workerID, task.SchemaName, task.TableName, task.PartitionName, utils.AfterUpsertStatus); err != nil {
		return err
	}

	endTime := time.Now()
	zap.L().Info("analyze task finished", zap.String("task", task.String()),
		zap.String("finished time", endTime.String()),
		zap.String("cost", endTime.Sub(startTime).String()))

	return nil
}

func ClearTableStatsDumpFile(schemaName, tableName, statsOutdir string, statsGCDays int) error {
	baseDIR := filepath.Join(statsOutdir, schemaName, tableName)
	rd, err := ioutil.ReadDir(baseDIR)
	if err != nil {
		return fmt.Errorf("error on read dir [%s] failed: %v", baseDIR, err)
	}
	currentTime := time.Now()
	subGCDayTimeStr := currentTime.Add(-time.Duration(statsGCDays*24) * time.Hour).Format("20060102150405")
	subGCDayTime, err := strconv.ParseInt(subGCDayTimeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("error on strconv.ParseInt subGCDayTimeStr [%v] failed: %v", subGCDayTimeStr, err)
	}

	// 遍历文件目录
	for _, f := range rd {
		if !f.IsDir() {
			fileTime, err := strconv.ParseInt(f.Name(), 10, 64)
			if err != nil {
				return fmt.Errorf("error on strconv.ParseInt [%v] failed: %v", f.Name(), err)
			}
			// 清理 < statsGCDays 文件
			if fileTime <= subGCDayTime {
				removeFile := filepath.Join(baseDIR, f.Name())
				if err := os.Remove(removeFile); err != nil {
					return fmt.Errorf("error on remove file [%v] failed: %v", removeFile, err)
				}
			}
		}
	}
	return nil
}

func StatsTableDumpFile(workerID int, host string, statusPort int, schemaName, tableName, partitionName, filePath string) error {
	api := fmt.Sprintf(`http://%s:%d/stats/dump/%s/%s`, host, statusPort, schemaName, tableName)
	resp, err := http.Get(api)
	if err != nil {
		return fmt.Errorf("error on StatsDump dump falied: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("error on StatsDump read request data failed: %v", err)
	}

	if err = utils.DirExist(filePath); err != nil {
		return fmt.Errorf("error on dir mkdir failed: %v", err)
	}

	file, err := os.OpenFile(filepath.Join(filePath, time.Now().Format("20060102150405")), os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0666)
	if err != nil {
		return fmt.Errorf("error on StatsDump open file failed: %v", err)
	}
	defer file.Close()

	if _, err = file.WriteString(string(body)); err != nil {
		return fmt.Errorf("error on StatsDump write string failed: %v", err)
	}
	zap.L().Info("dump single table stats",
		zap.Int("worker id", workerID),
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("partition", partitionName),
		zap.String("url", api),
		zap.String("filepath", filePath))
	return nil
}
