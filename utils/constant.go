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
package utils

import "time"

const (
	// 表健康度
	StatsHealth          = "StatsHealth"
	StatsHealthThreshold = 0.95

	// 表变化行数 modify_count
	StatsModifyZero           = "StatsModifyZero"
	StatsModifyZeroThreshold  = 0.0
	StatsModifyCount          = "StatsModifyCount"
	StatsModifyCountThreshold = 0.1

	// 表行数
	SmallTableRows           = "SmallTableRows"
	SmallTableRowsThreshold  = 1000000
	MediumTableRows          = "MediumTableRows"
	MediumTableRowsThreshold = 10000000
)

// 默认统计信息任务收集规则
var DefaultStatsRuleMap = map[string]float64{
	StatsHealth:      StatsHealthThreshold,
	StatsModifyZero:  StatsModifyZeroThreshold,
	StatsModifyCount: StatsModifyCountThreshold,
	SmallTableRows:   SmallTableRowsThreshold,
	MediumTableRows:  MediumTableRowsThreshold,
}

const (
	MySQLDBVersion = "5.3"

	BeforeUpsertStatus = "Before"
	AfterUpsertStatus  = "After"
	BatchUpsertCommit  = 300

	MySQLMaxIdleConns    = 100
	MySQLMaxOpenConns    = 100
	MySQLConnMaxLifetime = 1 * time.Second

	BuildStatsThread  = "tidb_build_stats_concurrency"
	DistsqlScanThread = "tidb_distsql_scan_concurrency"
	SerialScanThread  = "tidb_index_serial_scan_concurrency"
	AnalyzeTimeout    = "analyze_timeout"
)
