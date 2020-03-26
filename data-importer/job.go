// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	//"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

/*
func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}
*/

func doSqls(table *table, db *sql.DB, batch, count int64) {
	sqlPrefix, datas, err := genInsertSqls(table, count)
	if err != nil {
		log.S().Error(errors.ErrorStack(err))
		return
	}

	t := time.Now()
	defer func() {
		log.S().Infof("%s.%s insert %d datas, cost %v", table.schema, table.name, len(datas), time.Since(t))
	}()

	for begin := 0; begin < len(datas); begin += int(batch) {
		end := begin + int(batch)
		if end > len(datas) {
			end = len(datas)
		}

		sql := fmt.Sprintf("%s %s;", sqlPrefix, strings.Join(datas[begin:end], ","))
		_, err = db.Exec(sql)
		if err == nil {
			continue
		}

		log.S().Errorf("%s.%s execute sql %s failed, %d rows is not inserted, error %v", table.schema, table.name, sqlPrefix, errors.ErrorStack(err))
		if !strings.Contains(err.Error(), "Duplicate entry") {
			continue
		}

		log.S().Warnf("%s.%s insert data have duplicate key, insert for every row", table.schema, table.name)
		for _, data := range datas[begin:end] {
			_, err = db.Exec(fmt.Sprintf("%s %s;", sqlPrefix, data))
			if err != nil {
				log.S().Error(errors.ErrorStack(err))
			}
		}
	}
}

func execSqls(db *sql.DB, schema string, sqls []string, args [][]interface{}) {
	t := time.Now()
	defer func() {
		log.S().Infof("execute %d sqls, cost %v", len(sqls), time.Since(t))
	}()
	txn, err := db.Begin()
	if err != nil {
		log.S().Fatalf(errors.ErrorStack(err))
	}

	for i := range sqls {
		_, err = txn.Exec(sqls[i], args[i]...)
		if err != nil {
			log.S().Errorf("sql: %s, args: %v, err: %v", sqls[i], args[i], errors.ErrorStack(err))
		}
	}

	err = txn.Commit()
	if err != nil {
		log.S().Warn(errors.ErrorStack(err))
	}
}

func generateJob(ctx context.Context, table *table, db *sql.DB, jobCount int64, batch int64, ratio float64, qps int64, jobChan chan job) {

	if len(table.columns) <= 2 {
		log.S().Fatal("column count must > 2, and the first and second column are for primary key")
	}

	interval := int64(1)
	speed := int64(float64(qps) * ratio)
	if speed == 0 {
		log.S().Infof("table %s.%s 's qps is too low, will ignore it", table.schema, table.name)
		return
	}
	if speed < 1 {
		interval = int64(1 / ratio)
		speed = speed
	}

	log.S().Infof("table %s.%s will insert %d rows every %d seconds", table.schema, table.name, speed, interval)
	sc := NewSpeedControl(speed, interval)
	count := int64(0)

	defer func() {
		log.S().Infof("generate %d rows for table %s.%s to insert", count, table.schema, table.name)
	}()

	for count < jobCount {
		num := sc.ApplyTokenSync()

		if count+num > jobCount {
			num = jobCount - count
		}
		count += num

		sqlPrefix, datas, err := genInsertSqls(table, num)
		if err != nil {
			log.S().Error(errors.ErrorStack(err))
			return
		}

		newJob := job{
			sqlPrefix: sqlPrefix,
			datas:     datas,
		}

		select {
		case <-ctx.Done():
			return
		case jobChan <- newJob:
		}

		/*


			var wg sync.WaitGroup

			// one table's max thread is 5
			threadBatch := 5*batch
			threadNum := num / threadBatch
			if threadNum > 5 {
				threadBatch = num/5
			}
			for i := int64(0); i < num; i += threadBatch {
				end := i + threadBatch
				if end > num {
					end = num
				}

				if end-i <= 0 {
					continue
				}

				wg.Add(1)
				go func(doNum int64) {
					doSqls(table, db, batch, doNum)
					wg.Done()
				}(end-i)
			}

			wg.Wait()

			select {
			case <- ctx.Done():
				break
			default:
			}
		*/
	}
}

type job struct {
	sqlPrefix string
	datas     []string
}

func doJobs(ctx context.Context, db *sql.DB, batch int64, workerCount int, allJobChan chan job) {
	t := time.Now()
	defer func() {
		log.S().Infof("all rows inserted, cost time %v", time.Since(t))
	}()
	jobChans := make([]chan job, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		jobChan := make(chan job, 10)
		jobChans = append(jobChans, jobChan)
		go doJob(ctx, db, i, batch, jobChan)
	}

	i := 0
	for {
		j, ok := <-allJobChan
		if !ok {
			return
		}

		jobs := splitJob(j, batch)
		for _, newJob := range jobs {
			jobChans[i%workerCount] <- newJob
			i++
		}
	}
}

func splitJob(j job, batch int64) []job {
	jobs := make([]job, 0, 5)

	sqlPrefix := j.sqlPrefix
	datas := j.datas
	for begin := 0; begin < len(datas); begin += int(batch) {
		end := begin + int(batch)
		if end > len(datas) {
			end = len(datas)
		}

		jobs = append(jobs, job{
			sqlPrefix: sqlPrefix,
			datas:     datas[begin:end],
		})
	}

	return jobs
}

func doJob(ctx context.Context, db *sql.DB, id int, batch int64, jobChan chan job) {
	count := 0
	t := time.Now()
	defer func() {
		log.S().Infof("thread %d insert %d rows, total cost time %v", count, time.Since(t))
	}()

	for {
		job, ok := <-jobChan
		if !ok {
			return
		}

		sqlPrefix := job.sqlPrefix
		datas := job.datas
		count += len(datas)

		sql := fmt.Sprintf("%s %s;", sqlPrefix, strings.Join(datas, ","))
		_, err := db.Exec(sql)
		if err == nil {
			continue
		}

		log.S().Errorf("execute sql %s failed, %d rows is not inserted, error %v", sqlPrefix, errors.ErrorStack(err))
		if !strings.Contains(err.Error(), "Duplicate entry") {
			continue
		}

		log.S().Warnf("%s insert data have duplicate key, insert for every row", sqlPrefix)
		for _, data := range datas {
			_, err = db.Exec(fmt.Sprintf("%s %s;", sqlPrefix, data))
			if err != nil {
				log.S().Error(errors.ErrorStack(err))
			}
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}
