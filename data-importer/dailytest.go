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

	"github.com/pingcap/log"
)

// Run runs the daily test
func Run(sourceDB *sql.DB, schema string, workerCount int, jobCount int, batch int) {

	TableSQLs := []string{`
create table test.ptest(
	a int primary key,
	b double NOT NULL DEFAULT 2.0,
	c varchar(10) NOT NULL,
	d time unique
);
`,
		`
create table test.itest(
	a int,
	b double NOT NULL DEFAULT 2.0,
	c varchar(10) NOT NULL,
	d time unique,
	PRIMARY KEY(a, b)
);
`,
		`
create table test.ntest(
	a int,
	b double NOT NULL DEFAULT 2.0,
	c varchar(10) NOT NULL,
	d time unique
);
`}

	// run the simple test case
	//RunCase(sourceDB, targetDB, schema)

	RunTest(sourceDB, schema, func(src *sql.DB) {
		// generate insert/update/delete sqls and execute
		RunDailyTest(sourceDB, schema, TableSQLs, workerCount, jobCount, batch)
	})

	/*
	RunTest(sourceDB, schema, func(src *sql.DB) {
		// truncate test data
		TruncateTestTable(sourceDB, schema, TableSQLs)
	})

	RunTest(sourceDB, schema, func(src *sql.DB) {
		// drop test table
		DropTestTable(sourceDB, schema, TableSQLs)
	})
	*/

	log.S().Info("test pass!!!")

}
