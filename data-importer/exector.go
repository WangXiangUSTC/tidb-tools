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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
)

// RunDailyTest generates insert/update/delete sqls and execute
func RunDailyTest(db *sql.DB, tableSQLs []string, workerCount int, jobCount int, batch int) {
	var wg sync.WaitGroup
	wg.Add(len(tableSQLs))

	for i := range tableSQLs {
		go func(i int) {
			defer wg.Done()

			table := newTable()
			err := parseTableSQL(table, tableSQLs[i])
			if err != nil {
				log.S().Errorf("parse sql %s failed, error %v", tableSQLs[i], errors.Trace(err))
				return
			}

			err = execSQL(db, "", tableSQLs[i])
			if err != nil {
				log.S().Fatal(tableSQLs[i], err)
			}

			doProcess(table, db, jobCount, workerCount, batch)
		}(i)
	}

	wg.Wait()
}

// TruncateTestTable truncates test data
func TruncateTestTable(db *sql.DB, schema string, tableSQLs []string) {
	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.S().Fatal(err)
		}

		err = execSQL(db, schema, fmt.Sprintf("truncate table %s.%s", table.schema, table.name))
		if err != nil {
			log.S().Fatal(err)
		}
	}
}

// DropTestTable drops test table
func DropTestTable(db *sql.DB, schema string, tableSQLs []string) {
	for i := range tableSQLs {
		table := newTable()
		err := parseTableSQL(table, tableSQLs[i])
		if err != nil {
			log.S().Fatal(err)
		}

		err = execSQL(db, schema, fmt.Sprintf("drop table %s", table.name))
		if err != nil {
			log.S().Fatal(err)
		}
	}
}
