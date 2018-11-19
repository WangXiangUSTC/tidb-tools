// Copyright 2018 PingCAP, Inc.
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

package diff

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	log "github.com/sirupsen/logrus"
)

var (
	equal string = "="
	lt    string = "<"
	lte   string = "<="
	gt    string = ">"
	gte   string = ">="
)

// chunkRange represents chunk range
type chunkRange struct {
	columns []string
	bounds 	[][]string
	symbols  [][]string
}

// newChunkRange return a range struct
func newChunkRange() chunkRange {
	return chunkRange {
		columns: make([]string, 0, 2),
		bounds:  make([][]string, 0, 2),
		symbols: make([][]string, 0, 2),
	}
}

type Spliter interface {
	// Split splits a table's data to several chunks.
	Split(table *TableInstance, chunkSize int, limits string, collation string) ([]chunkRange, error)
}

type NormalSpliter struct {
	table     *TableInstance
	chunkSize int
	limits    string
	collation string
}

func (s *NormalSpliter) Split(table *TableInstance, chunkSize int, limits string, collation string) {
	s.table = table
	s.chunkSzie = chunkSize
	s.limits = limits
	s.collation = collation

	
}


/*
func newChunkRange(col string, begin, end interface{}, containBegin, containEnd, noBegin, noEnd bool) chunkRange {
	return chunkRange{
		columns:      []string{col},
		begin:        []interface{}{begin},
		end:          []interface{}{end},
		containEnd:   []bool{containEnd},
		containBegin: []bool{containBegin},
		noBegin:      []bool{noBegin},
		noEnd:        []bool{noEnd},
	}
}
*/

func (c *chunkRange) toString(collation string) (string, []interface{}) {
	condition := make([]string, 0, 2)
	args := make([]interface{}, 0, 2)

	for i, col := range c.columns {
		if !c.noBegin[i] {
			if c.containBegin[i] {
				condition = append(condition, fmt.Sprintf("`%s`%s >= ?", col, collation))
			} else {
				condition = append(condition, fmt.Sprintf("`%s`%s > ?", col, collation))
			}
			args = append(args, c.begin[i])
		}
		if !c.noEnd[i] {
			if c.containEnd[i] {
				condition = append(condition, fmt.Sprintf("`%s`%s <= ?", col, collation))
			} else {
				condition = append(condition, fmt.Sprintf("`%s`%s < ?", col, collation))
			}
			args = append(args, c.end[i])
		}
	}

	return strings.Join(condition, " AND "), args
}

// CheckJob is the struct of job for check
type CheckJob struct {
	Schema string
	Table  string
	Column *model.ColumnInfo
	Where  string
	Args   []interface{}
	Chunk  chunkRange
}

func getChunksForTable(table *TableInstance, column *model.ColumnInfo, chunkSize, sample int, limits string, collation string) ([]chunkRange, error) {
	if column == nil {
		log.Warnf("no suitable index found for %s.%s", table.Schema, table.Table)
		return nil, nil
	}

	// get the chunk count
	cnt, err := dbutil.GetRowCount(context.Background(), table.Conn, table.Schema, table.Table, limits)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if cnt == 0 {
		log.Infof("no data found in %s.%s", table.Schema, table.Table)
		return nil, nil
	}

	chunkCnt := (cnt + int64(chunkSize) - 1) / int64(chunkSize)
	if sample != 100 {
		// use sampling check, can check more fragmented by split to more chunk
		chunkCnt *= 10
	}

	field := column.Name.O

	collationStr := ""
	if collation != "" {
		collationStr = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	// fetch min, max
	query := fmt.Sprintf("SELECT /*!40001 SQL_NO_CACHE */ MIN(`%s`%s) as MIN, MAX(`%s`%s) as MAX FROM `%s`.`%s` WHERE %s",
		field, collationStr, field, collationStr, table.Schema, table.Table, limits)

	var min, max sql.NullString
	err := table.Conn.QueryRow(query).Scan(&min, &max)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !min.Valid || !max.Valid {
		return nil, nil
	}
	chunk := newChunkRange()
	chunk.columns = append(chunk.columns, field)
	chunk.bounds = append(chunk.bounds, []string{min.String, max.String})
	chunk.symbols = append(chunk.symbols, []string(gte, lte))
	chunk = newChunkRange(field, min.String, max.String, true, true, false, false)

	return splitRange(table.Conn, &chunk, chunkCnt, table.Schema, table.Table, column, limits, collation)
}

func getChunksByBucketsInfo(db *sql.DB, schema string, table string, tableInfo *model.TableInfo, chunkSize int) ([]chunkRange, error) {
	buckets, err := dbutil.GetBucketsInfo(context.Background(), db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("buckets: %v", buckets)

	var indexName string
	var columns []string

	for _, index := range tableInfo.Indices {
		if index.Primary || index.Unique {
			indexName = index.Name.O
		} else {
			// TODO: choose a situable index if without pk/uk.
			if indexName == "" {
				indexName = index.Name.O
			} else {
				continue
			}
		}

		columns := make([]string, 0, len(index.Columns))
		for _, column := range index.Columns {
			columns = append(columns, column.Name.O)
		}
		if index.Primary {
			break
		}
	}

	if indexName == "" {
		return nil, nil
	}
	log.Infof("columns: %v", columns)

	return nil, nil
}

/*
func bucketsToChunks(buckets []dbutil.Bucket, columns []string, count, chunkSize int) []chunkRange {
	chunks := make([]chunkRange, 0, count/chunkSize)
	var lower, upper string
	var num int

	// add chunk for data < min and data >= max
	chunks = append(chunks, newChunkRange(columns, nil, upper, true, false, false, false))
	chunks = append(chunks, newChunkRange(columns, lower, upper, true, false, false, false))

	for _, bucket := range buckets {
		if lower == "" {
			lower = bucket.LowerBound
		}
		upper = bucket.UpperBound
		num += bucket.Count
		if count - num > chunkSize {
			chunks = append(chunks, newChunkRange(columns, lower, upper, true, false, false, false))
			lower = upper
		}
	}
}
*/

func splitRange(db *sql.DB, chunk *chunkRange, count int64, Schema string, table string, column *model.ColumnInfo, limitRange string, collation string) ([]chunkRange, error) {
	buckets, err := dbutil.GetBucketsInfo(context.Background(), db, Schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("buckets: %v", buckets)

	var chunks []chunkRange

	// for example, the min and max value in target table is 2-9, but 1-10 in source table. so we need generate chunk for data < 2 and data > 9
	addOutRangeChunk := func() {
		chunks = append(chunks, newChunkRange(chunk.columns[0], struct{}{}, chunk.begin[0], false, false, true, false))
		chunks = append(chunks, newChunkRange(chunk.columns[0], chunk.end[0], struct{}{}, false, false, false, true))
	}

	if count <= 1 {
		chunks = append(chunks, *chunk)
		addOutRangeChunk()
		return chunks, nil
	}

	if reflect.TypeOf(chunk.begin).Kind() == reflect.Int64 && false {
		min, ok1 := chunk.begin[0].(int64)
		max, ok2 := chunk.end[0].(int64)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin[0], chunk.end[0])
		}
		step := (max - min + count - 1) / count
		cutoff := min
		for cutoff <= max {
			r := newChunkRange(chunk.columns[0], cutoff, cutoff+step, true, false, false, false)
			chunks = append(chunks, r)
			cutoff += step
		}

		log.Debugf("getChunksForTable cut table: cnt=%d min=%v max=%v step=%v chunk=%d", count, min, max, step, len(chunks))
	} else if reflect.TypeOf(chunk.begin).Kind() == reflect.Float64 && false {
		min, ok1 := chunk.begin[0].(float64)
		max, ok2 := chunk.end[0].(float64)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin[0], chunk.end[0])
		}
		step := (max - min + float64(count-1)) / float64(count)
		cutoff := min
		for cutoff <= max {
			r := newChunkRange(chunk.columns[0], cutoff, cutoff+step, true, false, false, false)
			chunks = append(chunks, r)
			cutoff += step
		}

		log.Debugf("getChunksForTable cut table: cnt=%d min=%v max=%v step=%v chunk=%d",
			count, min, max, step, len(chunks))
	} else {
		max, ok1 := chunk.end[0].(string)
		min, ok2 := chunk.begin[0].(string)
		if !ok1 || !ok2 {
			return nil, errors.Errorf("can't parse chunk's begin: %v, end: %v", chunk.begin, chunk.end)
		}

		// get random value as split value
		splitValues, err := dbutil.GetRandomValues(context.Background(), db, Schema, table, column.Name.O, count-1, min, max, limitRange, collation)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var minTmp, maxTmp string
		var i int64
		for i = 0; i < int64(len(splitValues)+1); i++ {
			if i == 0 {
				minTmp = min
			} else {
				minTmp = fmt.Sprintf("%s", splitValues[i-1])
			}
			if i == int64(len(splitValues)) {
				maxTmp = max
			} else {
				maxTmp = fmt.Sprintf("%s", splitValues[i])
			}
			r := newChunkRange(chunk.columns[0], minTmp, maxTmp, true, false, false, false)
			chunks = append(chunks, r)
		}

		log.Debugf("getChunksForTable cut table: cnt=%d min=%s max=%s chunk=%d", count, min, max, len(chunks))
	}

	chunks[len(chunks)-1].end = chunk.end
	chunks[0].containBegin = chunk.containBegin
	chunks[len(chunks)-1].containEnd = chunk.containEnd

	addOutRangeChunk()

	return chunks, nil
}

func findSuitableField(db *sql.DB, Schema string, table *model.TableInfo) (*model.ColumnInfo, error) {
	// first select the index, and number type index first
	column, err := dbutil.FindSuitableIndex(context.Background(), db, Schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if column != nil {
		return column, nil
	}

	// use the first column
	log.Infof("%s.%s don't have index, will use the first column as split field", Schema, table.Name.O)
	return table.Columns[0], nil
}

// GenerateCheckJob generates some CheckJobs.
func GenerateCheckJob(table *TableInstance, splitField, limits string, chunkSize, sample int, collation string) ([]*CheckJob, error) {
	jobBucket := make([]*CheckJob, 0, 10)
	var jobCnt int
	var column *model.ColumnInfo
	var err error

	if splitField == "" {
		column, err = findSuitableField(table.Conn, table.Schema, table.info)
		if err != nil {
			return nil, errors.Trace(err)
		}
	} else {
		column = dbutil.FindColumnByName(table.info.Columns, splitField)
		if column == nil {
			return nil, errors.NotFoundf("column %s in table %s", splitField, table.Table)
		}
	}

	chunks, err := getChunksForTable(table, column, chunkSize, sample, limits, collation)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if chunks == nil {
		return nil, nil
	}
	log.Debugf("chunks: %+v", chunks)

	jobCnt += len(chunks)

	if collation != "" {
		collation = fmt.Sprintf(" COLLATE \"%s\"", collation)
	}

	for {
		length := len(chunks)
		if length == 0 {
			break
		}

		chunk := chunks[0]
		chunks = chunks[1:]

		conditions, args := chunk.toString(collation)
		where := fmt.Sprintf("(%s AND %s)", conditions, limits)

		log.Debugf("%s.%s create dump job, where: %s, begin: %v, end: %v", table.Schema, table.Table, where, chunk.begin, chunk.end)
		jobBucket = append(jobBucket, &CheckJob{
			Schema: table.Schema,
			Table:  table.Table,
			Column: column,
			Where:  where,
			Args:   args,
			Chunk:  chunk,
		})
	}

	return jobBucket, nil
}
