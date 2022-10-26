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
	"github.com/wentaojin/tistat/utils"
	"go.uber.org/zap"
	"gorm.io/gorm/clause"
	"strings"
)

/*
	统计信息
*/
type WaitStatsTable struct {
	ID             uint    `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaName     string  `gorm:"not null;index:idx_schema_table,unique;comment:'数据库名'" json:"schema_name"`
	TableName      string  `gorm:"not null;index:idx_schema_table,unique;comment:'数据表名'" json:"table_name"`
	PartitionName  string  `gorm:"not null;index:idx_schema_table,unique;comment:'分区名'" json:"partition_name"`
	ServerAddr     string  `gorm:"not null;comment:'数据库地址'" json:"server_addr"`
	DBVersion      string  `gorm:"not null;comment:'数据库版本'" json:"db_version"`
	AnalyzeSQL     string  `gorm:"not null;comment:'收集 SQL'" json:"analyze_sql"`
	AnalyzeTimeout int     `gorm:"not null;comment:'收集超时设置'" json:"analyze_timeout"`
	RowCounts      float64 `gorm:"not null;comment:'表行数'" json:"row_counts"`
	StatsOutdir    string  `gorm:"not null;comment:'统计信息备份目录'" json:"stats_outdir"`
	BaseModel
}

type StatsHistogramsBefore struct {
	StatsHistograms
}

type StatsHistogramsAfter struct {
	StatsHistograms
}

type StatsMetaBefore struct {
	StatsMeta
}

type StatsMetaAfter struct {
	StatsMeta
}

type StatsHealthyBefore struct {
	StatsHealthy
}

type StatsHealthyAfter struct {
	StatsHealthy
}

type StatsHistograms struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaName    string `gorm:"not null;index:idx_schema_table,unique;comment:'数据库名'" json:"schema_name"`
	TableName     string `gorm:"not null;index:idx_schema_table,unique;comment:'数据表名'" json:"table_name"`
	PartitionName string `gorm:"index:idx_schema_table,unique;comment:'分区表名'" json:"partition_name"`
	ColumnName    string `gorm:"not null;comment:'字段名'" json:"column_name"`
	IsIndex       string `gorm:"not null;comment:'是否索引'" json:"is_index"`
	UpdateTime    string `gorm:"not null;comment:'统计信息更新时间'" json:"update_time"`
	DistinctCount string `gorm:"not null;comment:'唯一值数'" json:"distinct_count"`
	NullCount     string `gorm:"not null;comment:'Null 值数'" json:"null_count"`
	AvgColSize    string `gorm:"not null;comment:'平均字段大小'" json:"avg_col_size"`
	Correlation   string `gorm:"not null;comment:'correlation 值'" json:"correlation"`
	BaseModel
}

type StatsMeta struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaName    string `gorm:"not null;index:idx_schema_table,unique;comment:'数据库名'" json:"schema_name"`
	TableName     string `gorm:"not null;index:idx_schema_table,unique;comment:'数据表名'" json:"table_name"`
	PartitionName string `gorm:"index:idx_schema_table,unique;comment:'分区表名'" json:"partition_name"`
	UpdateTime    string `gorm:"not null;comment:'统计信息更新时间'" json:"update_time"`
	ModifyCount   string `gorm:"not null;comment:'唯一值数'" json:"modify_count"`
	RowCount      string `gorm:"not null;comment:'Null 值数'" json:"row_count"`
	BaseModel
}

type StatsHealthy struct {
	ID            uint   `gorm:"primary_key;autoIncrement;comment:'自增编号'" json:"id"`
	SchemaName    string `gorm:"not null;index:idx_schema_table,unique;comment:'数据库名'" json:"schema_name"`
	TableName     string `gorm:"not null;index:idx_schema_table,unique;comment:'数据表名'" json:"table_name"`
	PartitionName string `gorm:"index:idx_schema_table,unique;comment:'分区表名'" json:"partition_name"`
	Healthy       string `gorm:"not null;comment:'表健康度'" json:"healthy"`
	BaseModel
}

func (e *Engine) GetWaitStatsTableALLRows() ([]string, []string, []WaitStatsTable, error) {
	var allRows []WaitStatsTable
	if err := e.GormDB.Find(&allRows).Error; err != nil {
		return nil, nil, allRows, fmt.Errorf("error on get wait_stats_table all rows failed: %v", err)
	}
	var (
		nonPartitionTables []string
		partitionTables    []string
	)
	for _, r := range allRows {
		if r.PartitionName == "" {
			nonPartitionTables = append(nonPartitionTables, strings.ToUpper(utils.StringsBuilder(r.SchemaName, ".", r.TableName)))
		} else {
			partitionTables = append(partitionTables, strings.ToUpper(
				utils.StringsBuilder(r.SchemaName, ".", r.TableName, " PARTITION ", r.PartitionName)))
		}
	}

	zap.L().Info("get last analyze timeout tables",
		zap.Strings("non partition tables", nonPartitionTables),
		zap.Strings("partition tables", partitionTables))
	return nonPartitionTables, partitionTables, allRows, nil
}

func (e *Engine) upsertStatsTable(columns []string, val interface{}, batchSize int) error {
	if err := e.GormDB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "schema_name"},
			{Name: "table_name"},
			{Name: "partition_name"},
		},
		DoUpdates: clause.AssignmentColumns(columns),
	}).CreateInBatches(val, batchSize).Error; err != nil {
		return err
	}
	return nil
}
