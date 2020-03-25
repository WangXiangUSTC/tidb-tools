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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	//"github.com/pingcap/parser/mysql"
	//"github.com/pingcap/tidb/types"
)

func addJobs(jobCount int, jobChan chan struct{}) {
	for i := 0; i < jobCount; i++ {
		jobChan <- struct{}{}
	}

	close(jobChan)
}

func doSqls(table *table, db *sql.DB, count int) {
	sqlPrefix, datas, err := genInsertSqls(table, count)
	if err != nil {
		log.S().Error(errors.ErrorStack(err))
		return
	}

	t := time.Now()
	defer func() {
		log.S().Infof("insert %d datas, cost %v", len(datas), time.Since(t))
	}()

	values := strings.Join(datas, ",")
	_, err = db.Exec(fmt.Sprintf("%s %s;", sqlPrefix, values))
	if err == nil {
		return
	}

	log.S().Error(errors.ErrorStack(err))
	if !strings.Contains(err.Error(), "Duplicate entry") {
		return
	}

	log.S().Error("have duplicate key, insert for every row")
	for _, data := range datas {
		_, err = db.Exec(fmt.Sprintf("%s %s;", sqlPrefix, data))
		if err != nil {
			log.S().Error(errors.ErrorStack(err))
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

	/*
		_, err = txn.Exec(fmt.Sprintf("use %s;", schema))
		if err != nil {
			log.S().Error(errors.ErrorStack(err))
		}
	*/

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

func doJob(table *table, db *sql.DB, batch int, jobChan chan struct{}, doneChan chan struct{}) {
	count := 0
	for range jobChan {
		count++
		if count == batch {
			doSqls(table, db, count)
			count = 0
		}
	}

	if count > 0 {
		doSqls(table, db, count)
	}

	doneChan <- struct{}{}
}

func doWait(doneChan chan struct{}, start time.Time, jobCount int, workerCount int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}

	close(doneChan)
}

func doDMLProcess(table *table, db *sql.DB, jobCount int, workerCount int, batch int) {
	jobChan := make(chan struct{}, 16*workerCount)
	doneChan := make(chan struct{}, workerCount)

	start := time.Now()
	go addJobs(jobCount, jobChan)

	for i := 0; i < workerCount; i++ {
		go doJob(table, db, batch, jobChan, doneChan)
	}

	doWait(doneChan, start, jobCount, workerCount)

}

func doProcess(table *table, db *sql.DB, jobCount int, workerCount int, batch int) {
	if len(table.columns) <= 2 {
		log.S().Fatal("column count must > 2, and the first and second column are for primary key")
	}

	doDMLProcess(table, db, jobCount, workerCount, batch)
}
