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
	"strings"

	"github.com/wentaojin/tistat/utils"
)

type StatsTriggerRule struct {
	ID            uint    `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaName    string  `gorm:"not null;index:idx_schema_table,unique;comment:'数据库名'" json:"schema_name"`
	TableName     string  `gorm:"not null;index:idx_schema_table,unique;comment:'数据表名'" json:"table_name"`
	PartitionName string  `gorm:"not null;index:idx_schema_table,unique;comment:'分区名'" json:"partition_name"`
	StatsHealthy  float64 `gorm:"not null;comment:'表健康度阈值'" json:"server_addr"`
	StatsModify   float64 `gorm:"not null;comment:'表 modify_count 阈值'" json:"db_version"`
	BaseModel
}

type StatsAnalyzeRule struct {
	ID                uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaName        string `gorm:"not null;index:idx_schema_table,unique;comment:'数据库名'" json:"schema_name"`
	TableName         string `gorm:"not null;index:idx_schema_table,unique;comment:'数据表名'" json:"table_name"`
	PartitionName     string `gorm:"not null;index:idx_schema_table,unique;comment:'分区名'" json:"partition_name"`
	BuildStatsThread  int    `gorm:"comment:'tidb_build_stats_concurrency 参数设置'" json:"build_stats_thread"`
	DistsqlScanThread int    `gorm:"comment:'tidb_distsql_scan_concurrency 参数设置'" json:"distsql_scan_thread"`
	SerialScanThread  int    `gorm:"comment:'tidb_index_serial_scan_concurrency 参数设置'" json:"serial_scan_thread"`
	SampleRate        string `gorm:"comment:'采样率 v5.3 版本以上支持'" json:"sample_rate"`
	AnalyzeTimeout    int    `gorm:"comment:'收集超时设置'" json:"analyze_timeout"`
	StatsOutdir       string `gorm:"comment:'统计信息备份目录'" json:"stats_outdir"`
	BaseModel
}

func (e *Engine) GetStatsTriggerRuleTableALLRows() (map[string]float64, map[string]float64, error) {
	var allRows []StatsTriggerRule
	if err := e.GormDB.Find(&allRows).Error; err != nil {
		return nil, nil, fmt.Errorf("error on get stats_trigger_rule all rows failed: %v", err)
	}

	statsHealthyMap := make(map[string]float64)
	statsModifyMap := make(map[string]float64)

	for _, r := range allRows {
		if r.PartitionName == "" {
			statsHealthyMap[strings.ToUpper(utils.StringsBuilder(r.SchemaName, ".", r.TableName))] = r.StatsHealthy
			statsModifyMap[strings.ToUpper(utils.StringsBuilder(r.SchemaName, ".", r.TableName))] = r.StatsModify
		} else {
			statsHealthyMap[strings.ToUpper(
				utils.StringsBuilder(r.SchemaName, ".", r.TableName, " PARTITION ", r.PartitionName))] = r.StatsHealthy
			statsModifyMap[strings.ToUpper(
				utils.StringsBuilder(r.SchemaName, ".", r.TableName, " PARTITION ", r.PartitionName))] = r.StatsModify
		}
	}
	return statsHealthyMap, statsModifyMap, nil
}

func (e *Engine) GetStatsAnalyzeRuleTableALLRows() (map[string]map[string]int, map[string]string, map[string]string, error) {
	var allRows []StatsAnalyzeRule
	if err := e.GormDB.Find(&allRows).Error; err != nil {
		return nil, nil, nil, fmt.Errorf("error on get stats_analyze_rule all rows failed: %v", err)
	}

	statsComplexMap := make(map[string]map[string]int)
	statsSamplerateMap := make(map[string]string)
	statsOutdirMap := make(map[string]string)

	for _, r := range allRows {
		if r.PartitionName == "" {
			statsComplexMap[strings.ToUpper(utils.StringsBuilder(r.SchemaName, ".", r.TableName))] = map[string]int{
				utils.BuildStatsThread:  r.BuildStatsThread,
				utils.DistsqlScanThread: r.DistsqlScanThread,
				utils.SerialScanThread:  r.SerialScanThread,
				utils.AnalyzeTimeout:    r.AnalyzeTimeout,
			}
			statsSamplerateMap[strings.ToUpper(utils.StringsBuilder(r.SchemaName, ".", r.TableName))] = r.SampleRate
			statsOutdirMap[strings.ToUpper(utils.StringsBuilder(r.SchemaName, ".", r.TableName))] = r.StatsOutdir
		} else {
			statsComplexMap[strings.ToUpper(
				utils.StringsBuilder(r.SchemaName, ".", r.TableName, " PARTITION ", r.PartitionName))] = map[string]int{
				utils.BuildStatsThread:  r.BuildStatsThread,
				utils.DistsqlScanThread: r.DistsqlScanThread,
				utils.SerialScanThread:  r.SerialScanThread,
				utils.AnalyzeTimeout:    r.AnalyzeTimeout,
			}
			statsSamplerateMap[strings.ToUpper(
				utils.StringsBuilder(r.SchemaName, ".", r.TableName, " PARTITION ", r.PartitionName))] = r.SampleRate
			statsOutdirMap[strings.ToUpper(
				utils.StringsBuilder(r.SchemaName, ".", r.TableName, " PARTITION ", r.PartitionName))] = r.StatsOutdir
		}
	}
	return statsComplexMap, statsSamplerateMap, statsOutdirMap, nil
}
