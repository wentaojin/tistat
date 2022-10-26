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
	"strconv"

	"github.com/wentaojin/tistat/utils"
)

// 表健康度规则
func StatsTableHealthyRule(tableName string, tableHealthyMap, statsRuleHealthyMap map[string]float64) bool {
	if _, ok := statsRuleHealthyMap[tableName]; ok {
		if statsRuleHealthyMap[tableName] > 0.0 && tableHealthyMap[tableName] < statsRuleHealthyMap[tableName] {
			return true
		}
	}

	if _, ok := tableHealthyMap[tableName]; ok {
		if tableHealthyMap[tableName] < utils.DefaultStatsRuleMap[utils.StatsHealth] {
			return true
		}
	}
	return false
}

// 表 modify_count 规则
func StatsTableModifyZeroRule(tableName string, tableModifyMap map[string]float64) bool {
	if _, ok := tableModifyMap[tableName]; ok {
		if tableModifyMap[tableName] == utils.DefaultStatsRuleMap[utils.StatsModifyZero] {
			return true
		}
	}
	return false
}

func StatsTableModifyCountRule(tableName string, tableModifyMap, statsRuleModifyMap map[string]float64) bool {
	if _, ok := statsRuleModifyMap[tableName]; ok {
		if statsRuleModifyMap[tableName] > 0.0 && tableModifyMap[tableName] > statsRuleModifyMap[tableName] {
			return true
		}
	}

	if _, ok := tableModifyMap[tableName]; ok {
		if tableModifyMap[tableName] > utils.DefaultStatsRuleMap[utils.StatsModifyCount] {
			return true
		}
	}
	return false
}

// 表采样率
func StatsTableAnalyzeSamplerateRule(tableName, dbVersion string,
	smallTableSamplerate, mediumTableSamplerate, bigTableSamplerate string,
	statsRuleSamplerateMap map[string]string, tableRowsTotalMap map[string]float64) string {
	// 自定义表采样率且满足版本要求，返回自定义采样率，否则忽略自定义采样率，以默认规则采样
	if _, ok := statsRuleSamplerateMap[tableName]; ok {
		if statsRuleSamplerateMap[tableName] != "" && utils.VersionOrdinal(utils.StringPrefixTrunc(dbVersion, "v")) > utils.VersionOrdinal(utils.MySQLDBVersion) {
			return utils.StringsBuilder(" WITH ", statsRuleSamplerateMap[tableName], " SAMPLERATE")
		}
		return ""
	}

	// 默认规则
	tableRowCount := tableRowsTotalMap[tableName]
	if utils.VersionOrdinal(utils.StringPrefixTrunc(dbVersion, "v")) > utils.VersionOrdinal(utils.MySQLDBVersion) {
		if tableRowCount <= utils.DefaultStatsRuleMap[utils.SmallTableRows] {
			return utils.StringsBuilder(" WITH ", smallTableSamplerate, " SAMPLERATE")
		} else if tableRowCount > utils.DefaultStatsRuleMap[utils.SmallTableRows] && tableRowCount <= utils.DefaultStatsRuleMap[utils.MediumTableRows] {
			return utils.StringsBuilder(" WITH ", mediumTableSamplerate, " SAMPLERATE")
		} else {
			return utils.StringsBuilder(" WITH ", bigTableSamplerate, " SAMPLERATE")
		}
	}
	return ""
}

// 表统计信息备份目录
func StatsTableBackupOutdirRule(tableName string, statsOutdirRuleMap map[string]string, statsDefaultOutdir string) string {
	if _, ok := statsOutdirRuleMap[tableName]; ok {
		if statsOutdirRuleMap[tableName] != "" {
			return statsOutdirRuleMap[tableName]
		}
	}
	return statsDefaultOutdir
}

// 表统计信息收集以及超时
func StatsTableAnalyzeConcurrencyRule(tableName string, statsComplexRuleMap map[string]map[string]int, analyzeDefaultTimeout int) ([]string, int) {
	var (
		statsThreadsSQL []string
		analyzeTimeout  int
	)

	if _, ok := statsComplexRuleMap[tableName]; ok {
		if statsComplexRuleMap[tableName][utils.AnalyzeTimeout] != 0 {
			analyzeTimeout = statsComplexRuleMap[tableName][utils.AnalyzeTimeout]
		} else {
			analyzeTimeout = analyzeDefaultTimeout
		}
		if statsComplexRuleMap[tableName][utils.BuildStatsThread] != 0 {
			statsThreadsSQL = append(statsThreadsSQL, utils.StringsBuilder(`SET `, utils.BuildStatsThread, ` = `, strconv.Itoa(statsComplexRuleMap[tableName][utils.BuildStatsThread])))
		}
		if statsComplexRuleMap[tableName][utils.DistsqlScanThread] != 0 {
			statsThreadsSQL = append(statsThreadsSQL, utils.StringsBuilder(`SET `, utils.DistsqlScanThread, ` = `, strconv.Itoa(statsComplexRuleMap[tableName][utils.DistsqlScanThread])))
		}
		if statsComplexRuleMap[tableName][utils.SerialScanThread] != 0 {
			statsThreadsSQL = append(statsThreadsSQL, utils.StringsBuilder(`SET `, utils.SerialScanThread, ` = `, strconv.Itoa(statsComplexRuleMap[tableName][utils.SerialScanThread])))
		}
	}

	if analyzeTimeout == 0 {
		analyzeTimeout = analyzeDefaultTimeout
	}

	return statsThreadsSQL, analyzeTimeout
}
