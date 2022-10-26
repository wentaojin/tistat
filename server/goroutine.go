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
	"encoding/json"

	"golang.org/x/sync/errgroup"
)

type AnalyzeTask struct {
	Host           string
	Port           int
	StatusPort     int
	DBVersion      string
	RunningUser    string
	SchemaName     string
	TableName      string
	PartitionName  string
	RowCounts      float64
	StatsOutdir    string
	StatsGCDays    int
	AnalyzeTimeout int
	AnalyzeSQL     []string
	Engine         *Engine `json:"-"`
}

func (t *AnalyzeTask) String() string {
	str, _ := json.Marshal(&t)
	return string(str)
}

type Job struct {
	workers int
	task    chan AnalyzeTask
	wg      *errgroup.Group
}

func NewScheduleJob(workers int) *Job {
	return &Job{
		workers: workers,
		task:    make(chan AnalyzeTask, workers),
		wg:      &errgroup.Group{},
	}
}

func (j *Job) Push(analyzeTasks []AnalyzeTask) {
	j.wg.Go(func() error {
		for _, t := range analyzeTasks {
			j.task <- t
		}
		j.Close()
		return nil
	})
}

func (j *Job) Close() {
	close(j.task)
}

func (j *Job) Run() {
	for i := 0; i < j.workers; i++ {
		workerID := i
		j.wg.Go(func() error {
			for task := range j.task {
				if err := AnalyzeTableStats(workerID, task); err != nil {
					return err
				}
				if err := ClearTableStatsDumpFile(task.SchemaName, task.TableName, task.StatsOutdir, task.StatsGCDays); err != nil {
					return err
				}
			}
			return nil
		})
	}
}

func (j *Job) Wait() error {
	if err := j.wg.Wait(); err != nil {
		return err
	}
	return nil
}
