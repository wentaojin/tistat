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
	"strconv"
	"strings"
	"time"

	"github.com/wentaojin/tistat/utils"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (e *Engine) GetMySQLDBVersion() (string, error) {
	_, results, err := e.Query(`SELECT VERSION() AS VERSION`)
	if err != nil {
		return "", fmt.Errorf("error on FUNC GetMySQLDBVersion query failed: %v", err)
	}
	zap.L().Info("get mysql db version", zap.String("version", results[0]["VERSION"]))
	return strings.Split(results[0]["VERSION"], "-TiDB-")[1], nil
}

func (e *Engine) GetMySQLDBALLTables(metaSchema string) ([]string, error) {
	_, results, err := e.Query(utils.StringsBuilder(`SELECT TABLE_SCHEMA,Table_name FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_SCHEMA) NOT IN ('INFORMATION_SCHEMA','METRICS_SCHEMA','PERFORMANCE_SCHEMA','MYSQL','TEST','`, strings.ToUpper(metaSchema), `')`))
	if err != nil {
		return nil, fmt.Errorf("error on FUNC GetMySQLDBALLTables query failed: %v", err)
	}
	var tables []string
	for _, r := range results {
		tables = append(tables, strings.ToUpper(utils.StringsBuilder(r["TABLE_SCHEMA"], ".", r["Table_name"])))
	}

	zap.L().Info("get mysql db all tables", zap.Int("counts", len(tables)))
	return tables, nil
}

func (e *Engine) GetMySQLDBPartitionTables(metaSchema string) ([]string, []string, error) {
	_, results, err := e.Query(utils.StringsBuilder(`SELECT TABLE_SCHEMA,Table_name,Partition_name FROM INFORMATION_SCHEMA.PARTITIONS WHERE UPPER(TABLE_SCHEMA) NOT IN ('INFORMATION_SCHEMA','METRICS_SCHEMA','PERFORMANCE_SCHEMA','MYSQL','TEST','`, strings.ToUpper(metaSchema), `') AND PARTITION_NAME IS NOT NULL`))
	if err != nil {
		return nil, nil, fmt.Errorf("error on FUNC GetMySQLDBPartitionTables query failed: %v", err)
	}
	var (
		tables          []string
		partitionTables []string
	)
	for _, r := range results {
		partitionTables = append(partitionTables, strings.ToUpper(
			utils.StringsBuilder(r["TABLE_SCHEMA"],
				".",
				r["Table_name"],
				" PARTITION ",
				r["Partition_name"])))
		tables = append(tables, strings.ToUpper(utils.StringsBuilder(
			r["TABLE_SCHEMA"],
			".",
			r["Table_name"])))
	}
	zap.L().Info("get mysql db partition tables", zap.Int("counts", len(tables)))

	return tables, partitionTables, nil
}

func (e *Engine) GetTableStatsHealthy() (map[string]float64, error) {
	_, results, err := e.Query(`SHOW STATS_HEALTHY WHERE UPPER(Db_name) NOT IN ('INFORMATION_SCHEMA','METRICS_SCHEMA','PERFORMANCE_SCHEMA','MYSQL','TEST')`)
	if err != nil {
		return nil, fmt.Errorf("error on FUNC GetTableStatsHealthy query failed: %v", err)
	}
	tableHealthyMap := make(map[string]float64)
	for _, r := range results {
		healthy, err := utils.StrconvFloatBitSize(r["Healthy"], 64)
		if err != nil {
			return nil, fmt.Errorf("error on FUNC GetTableStatsHealthy StrconvFloatBitSize [%s] failed: %v", r["Healthy"], err)
		}
		if r["Partition_name"] == "" {
			tableHealthyMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"]))] = healthy
		} else {
			tableHealthyMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"], " PARTITION ", r["Partition_name"]))] = healthy
		}
	}
	return tableHealthyMap, nil
}

func (e *Engine) GetTableStatsMeta() (map[string]float64, map[string]float64, map[string]float64, error) {
	_, results, err := e.Query(`SHOW STATS_META WHERE UPPER(Db_name) NOT IN ('INFORMATION_SCHEMA','METRICS_SCHEMA','PERFORMANCE_SCHEMA','MYSQL','TEST')`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error on FUNC GetTableStatsMeta query failed: %v", err)
	}

	tableModifyMap := make(map[string]float64)
	tableModifyThresholdMap := make(map[string]float64)
	tableRowsTotalMap := make(map[string]float64)

	for _, r := range results {
		modifyCount, err := utils.StrconvFloatBitSize(r["Modify_count"], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error on FUNC GetTableStatsMeta StrconvFloatBitSize ModifyCount failed: %v", err)
		}
		rowCount, err := utils.StrconvFloatBitSize(r["Row_count"], 64)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("error on FUNC GetTableStatsMeta StrconvFloatBitSize RowCount failed: %v", err)
		}
		if r["Partition_name"] == "" {
			tableModifyMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"]))] = modifyCount
			tableModifyThresholdMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"]))] = modifyCount / rowCount
			tableRowsTotalMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"]))] = rowCount
		} else {
			tableModifyMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"], " PARTITION ", r["Partition_name"]))] = modifyCount
			tableModifyThresholdMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"], " PARTITION ", r["Partition_name"]))] = modifyCount / rowCount
			tableRowsTotalMap[strings.ToUpper(utils.StringsBuilder(r["Db_name"], ".", r["Table_name"], " PARTITION ", r["Partition_name"]))] = rowCount
		}

	}
	return tableModifyMap, tableModifyThresholdMap, tableRowsTotalMap, nil
}

func (e *Engine) UpsertSingleTableStatsHealthy(workerID int, schemaName, tableName, partitionTable, status string) error {
	querySQL := utils.StringsBuilder(`SHOW STATS_HEALTHY WHERE UPPER(Db_name) = '`,
		strings.ToUpper(schemaName),
		`' AND UPPER(Table_name) = '`,
		strings.ToUpper(tableName),
		`' AND UPPER(Partition_name) = '`,
		strings.ToUpper(partitionTable),
		`'`)
	_, results, err := e.Query(querySQL)
	if err != nil {
		return fmt.Errorf("error on FUNC UpsertSingleTableStatsHealthy query failed: %v", err)
	}

	if status == utils.BeforeUpsertStatus {
		var (
			statsBefore []StatsHealthyBefore
		)
		for _, r := range results {
			statsBefore = append(statsBefore, StatsHealthyBefore{StatsHealthy{
				SchemaName:    strings.ToUpper(schemaName),
				TableName:     strings.ToUpper(tableName),
				PartitionName: strings.ToUpper(partitionTable),
				Healthy:       r["Healthy"],
			}})
		}

		if err = e.upsertStatsTable([]string{
			"healthy",
		}, statsBefore, utils.BatchUpsertCommit); err != nil {
			return err
		}
	}

	if status == utils.AfterUpsertStatus {
		var (
			statsAfter []StatsHealthyAfter
		)
		for _, r := range results {
			statsAfter = append(statsAfter, StatsHealthyAfter{StatsHealthy{
				SchemaName:    strings.ToUpper(schemaName),
				TableName:     strings.ToUpper(tableName),
				PartitionName: strings.ToUpper(partitionTable),
				Healthy:       r["Healthy"],
			}})
		}

		if err = e.upsertStatsTable([]string{
			"healthy",
		}, statsAfter, utils.BatchUpsertCommit); err != nil {
			return err
		}
	}

	zap.L().Info("upsert single table stats healthy",
		zap.Int("worker id", workerID),
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("partition", partitionTable),
		zap.String("status", status))
	return nil
}

func (e *Engine) UpsertSingleTableStatsMeta(workerID int, schemaName, tableName, partitionTable, status string) error {
	querySQL := utils.StringsBuilder(`SHOW STATS_META WHERE UPPER(Db_name) = '`,
		strings.ToUpper(schemaName),
		`' AND UPPER(Table_name) = '`,
		strings.ToUpper(tableName),
		`' AND UPPER(Partition_name) = '`,
		strings.ToUpper(partitionTable),
		`'`)
	_, results, err := e.Query(querySQL)
	if err != nil {
		return fmt.Errorf("error on FUNC UpsertSingleTableStatsMeta query failed: %v", err)
	}

	if status == utils.BeforeUpsertStatus {
		var (
			statsBefore []StatsMetaBefore
		)
		for _, r := range results {
			statsBefore = append(statsBefore, StatsMetaBefore{StatsMeta{
				SchemaName:    strings.ToUpper(schemaName),
				TableName:     strings.ToUpper(tableName),
				PartitionName: strings.ToUpper(partitionTable),
				UpdateTime:    r["Update_time"],
				ModifyCount:   r["Modify_count"],
				RowCount:      r["Row_count"],
			}})
		}

		if err = e.upsertStatsTable([]string{
			"update_time",
			"modify_count",
			"row_count",
		}, statsBefore, utils.BatchUpsertCommit); err != nil {
			return err
		}
	}

	if status == utils.AfterUpsertStatus {
		var (
			statsAfter []StatsMetaAfter
		)
		for _, r := range results {
			statsAfter = append(statsAfter, StatsMetaAfter{StatsMeta{
				SchemaName:    strings.ToUpper(schemaName),
				TableName:     strings.ToUpper(tableName),
				PartitionName: strings.ToUpper(partitionTable),
				UpdateTime:    r["Update_time"],
				ModifyCount:   r["Modify_count"],
				RowCount:      r["Row_count"],
			}})
		}

		if err = e.upsertStatsTable([]string{
			"update_time",
			"modify_count",
			"row_count",
		}, statsAfter, utils.BatchUpsertCommit); err != nil {
			return err
		}
	}

	zap.L().Info("upsert single table stats meta",
		zap.Int("worker id", workerID),
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("partition", partitionTable),
		zap.String("status", status))
	return nil
}

func (e *Engine) UpsertSingleTableStatsHistograms(workerID int, schemaName, tableName, partitionTable, status string) error {
	querySQL := utils.StringsBuilder(`SHOW STATS_HISTOGRAMS WHERE UPPER(Db_name) = '`,
		strings.ToUpper(schemaName),
		`' AND UPPER(Table_name) = '`,
		strings.ToUpper(tableName),
		`' AND UPPER(Partition_name) = '`,
		strings.ToUpper(partitionTable),
		`'`)
	_, results, err := e.Query(querySQL)
	if err != nil {
		return fmt.Errorf("error on FUNC UpsertSingleTableStatsHistograms query failed: %v", err)
	}

	if status == utils.BeforeUpsertStatus {
		var (
			statsBefore []StatsHistogramsBefore
		)
		for _, r := range results {
			statsBefore = append(statsBefore, StatsHistogramsBefore{StatsHistograms{
				SchemaName:    strings.ToUpper(schemaName),
				TableName:     strings.ToUpper(tableName),
				PartitionName: strings.ToUpper(partitionTable),
				ColumnName:    r["Column_name"],
				IsIndex:       r["Is_index"],
				UpdateTime:    r["Update_time"],
				DistinctCount: r["Distinct_count"],
				NullCount:     r["Null_count"],
				AvgColSize:    r["Avg_col_size"],
				Correlation:   r["Correlation"],
			}})
		}

		if err = e.upsertStatsTable([]string{
			"column_name",
			"is_index",
			"update_time",
			"distinct_count",
			"null_count",
			"avg_col_size",
			"correlation",
		}, statsBefore, utils.BatchUpsertCommit); err != nil {
			return err
		}
	}

	if status == utils.AfterUpsertStatus {
		var (
			statsAfter []StatsHistogramsAfter
		)
		for _, r := range results {
			statsAfter = append(statsAfter, StatsHistogramsAfter{StatsHistograms{
				SchemaName:    strings.ToUpper(schemaName),
				TableName:     strings.ToUpper(tableName),
				PartitionName: strings.ToUpper(partitionTable),
				ColumnName:    r["Column_name"],
				IsIndex:       r["Is_index"],
				UpdateTime:    r["Update_time"],
				DistinctCount: r["Distinct_count"],
				NullCount:     r["Null_count"],
				AvgColSize:    r["Avg_col_size"],
				Correlation:   r["Correlation"],
			}})
		}

		if err = e.upsertStatsTable([]string{
			"column_name",
			"is_index",
			"update_time",
			"distinct_count",
			"null_count",
			"avg_col_size",
			"correlation",
		}, statsAfter, utils.BatchUpsertCommit); err != nil {
			return err
		}
	}

	zap.L().Info("upsert single table stats histograms",
		zap.Int("worker id", workerID),
		zap.String("schema", schemaName),
		zap.String("table", tableName),
		zap.String("partition", partitionTable),
		zap.String("status", status))
	return nil
}

func (e *Engine) RunAnalyzeTask(workerID int, task AnalyzeTask) error {
	startTime := time.Now()
	workDone := make(chan struct{}, 1)

	g := &errgroup.Group{}

	g.Go(func() error {
		txn, err := e.TiDB.Begin()
		if err != nil {
			return fmt.Errorf("error on transaction begin failed: %v", err)
		}
		for _, sql := range task.AnalyzeSQL {
			if _, err := txn.Exec(sql); err != nil {
				return fmt.Errorf("error on transaction exec sql [%v] total sql %v failed: %v", sql, task.AnalyzeSQL, err)
			}
		}
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("error on transaction commit failed: %v", err)
		}
		workDone <- struct{}{}
		return nil
	})

	select {
	case <-workDone:
		zap.L().Info("analyze table stats success",
			zap.Int("worker id", workerID),
			zap.String("schema", task.SchemaName),
			zap.String("table", task.TableName),
			zap.String("partition", task.PartitionName),
			zap.Strings("sql", task.AnalyzeSQL),
			zap.Int("analyze timeout", task.AnalyzeTimeout),
			zap.String("cost", time.Since(startTime).String()))

	case <-time.After(time.Duration(task.AnalyzeTimeout) * time.Second):
		zap.L().Warn("analyze table stats timeout",
			zap.Int("worker id", workerID),
			zap.String("schema", task.SchemaName),
			zap.String("table", task.TableName),
			zap.String("partition", task.PartitionName),
			zap.Strings("sql", task.AnalyzeSQL),
			zap.Int("analyze timeout", task.AnalyzeTimeout),
			zap.String("cost", time.Since(startTime).String()))

		// 统计信息收集失败信息记录
		statsWait := []WaitStatsTable{
			{
				SchemaName:     strings.ToUpper(task.SchemaName),
				TableName:      strings.ToUpper(task.TableName),
				PartitionName:  strings.ToUpper(task.PartitionName),
				ServerAddr:     utils.StringsBuilder(task.Host, `:`, strconv.Itoa(task.Port)),
				DBVersion:      task.DBVersion,
				AnalyzeSQL:     strings.Join(task.AnalyzeSQL, "\n"),
				AnalyzeTimeout: task.AnalyzeTimeout,
				RowCounts:      task.RowCounts,
				StatsOutdir:    task.StatsOutdir,
			},
		}

		if err := e.upsertStatsTable([]string{
			"schema_name",
			"table_name",
			"partition_name",
		}, statsWait, utils.BatchUpsertCommit); err != nil {
			return err
		}

		// 查杀收集表任务，等待整个任务收集完，下次任务收集时，一次性发送告警
		if err := e.KillAnalyzeTimeoutSQL(task.RunningUser, task.AnalyzeTimeout, utils.StringsBuilder("ANALYZE TABLE ", task.SchemaName, ".", task.TableName)); err != nil {
			return err
		}
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) KillAnalyzeTimeoutSQL(runningUser string, analyzeTimeout int, runningMatchingSQL string) error {
	sql := fmt.Sprintf(`SELECT CONCAT_WS(" ",'KILL TiDB ',ID) AS KILLSQL,INFO FROM INFORMATION_SCHEMA.PROCESSLIST WHERE UPPER(USER) = '%s' AND DB IS NULL AND TIME >= %d AND COMMAND='Query' AND INFO LIKE '%%%s%%'`, runningUser, analyzeTimeout, runningMatchingSQL)

	_, results, err := e.Query(sql)
	if err != nil {
		return err
	}

	for _, v := range results {
		if strings.Contains(v["INFO"], runningMatchingSQL) {
			if _, err := e.TiDB.Exec(v["KILLSQL"]); err != nil {
				return err
			}
		}
	}
	return nil
}
