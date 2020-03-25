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
	"bufio"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	//"github.com/pingcap/tidb-binlog/tests/dailytest"
	//"github.com/pingcap/tidb-binlog/tests/util"
)

func main() {
	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.S().Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	sourceDB, err := dbutil.OpenDB(cfg.SourceDBCfg)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if err := dbutil.CloseDB(sourceDB); err != nil {
			log.S().Errorf("Failed to close source database: %s\n", err)
		}
	}()

	files, err := ioutil.ReadDir(cfg.TableSQLDir)
	if err != nil {
		log.S().Fatal(err)
	}

	tableSQLs := make([]string, 0, len(files))
	for _, f := range files {
		sql, err := analyzeSQLFile(filepath.Join(cfg.TableSQLDir, f.Name()))
		if err != nil {
			log.S().Fatal(err)
		}
		if len(sql) == 0 {
			log.S().Errorf("parse file %s get empty sql", f.Name())
			os.Exit(1)
		}
		tableSQLs = append(tableSQLs, sql)
	}

	//dailytest.RunMultiSource(sourceDBs, targetDB, cfg.SourceDBCfg.Name)
	Run(sourceDB, tableSQLs, cfg.WorkerCount, cfg.JobCount, cfg.Batch)
}

func analyzeSQLFile(file string) (string, error) {
	f, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer f.Close()

	//tctx := l.logCtx.WithContext(ctx)

	data := make([]byte, 0, 1024*1024)
	br := bufio.NewReader(f)
	for {
		line, err := br.ReadString('\n')
		if err == io.EOF {
			break
		}

		realLine := strings.TrimSpace(line[:len(line)-1])
		if len(realLine) == 0 {
			continue
		}

		data = append(data, []byte(realLine)...)
		if data[len(data)-1] == ';' {
			query := string(data)
			//data = data[0:0]
			if strings.HasPrefix(query, "/*") && strings.HasSuffix(query, "*/;") {
				continue
			}

			/*
				var sqls []string
				dstSchema, dstTable := fetchMatchedLiteral(tctx, l.tableRouter, schema, table)
				// for table
				if table != "" {
					sqls = append(sqls, fmt.Sprintf("USE `%s`;", dstSchema))
					query = renameShardingTable(query, table, dstTable)
				} else {
					query = renameShardingSchema(query, schema, dstSchema)
				}
			*/

			//l.logCtx.L().Debug("schema create statement", zap.String("sql", query))

			/*
				sqls = append(sqls, query)
				err = conn.executeSQL(tctx, sqls)
				if err != nil {
					return terror.WithScope(err, terror.ScopeDownstream)
				}
			*/
		}

	}

	return string(data), nil
}
