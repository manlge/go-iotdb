/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/manlge/go-iotdb/rpc"
)

const (
	TIMESTAMP_STR = "Time"
	START_INDEX   = 2
	FLAG          = 0x80
)

type IoTDBRpcDataSet struct {
	columnCount                int
	sessionId                  int64
	queryId                    int64
	lastReadWasNull            bool
	rowsIndex                  int
	queryDataSet               *rpc.TSQueryDataSet
	sql                        string
	fetchSize                  int32
	columnNameList             []string
	columnTypeList             []int32
	columnOrdinalMap           map[string]int32
	columnTypeDeduplicatedList []int32
	columnNameIndexMap         map[string]int32
	typeMap                    map[string]int32
	currentBitmap              []byte
	time                       []byte
	value                      [][]byte
	client                     *rpc.TSIServiceClient
	emptyResultSet             bool
	ignoreTimeStamp            bool
}

func (s *IoTDBRpcDataSet) isNull(columnIndex int, rowIndex int) bool {
	bitmap := s.currentBitmap[columnIndex]
	shift := rowIndex % 8
	return ((FLAG >> shift) & (bitmap & 0xff)) == 0
}

func bytesToInt32(bys []byte) int32 {
	bytebuff := bytes.NewBuffer(bys)
	var data int32
	binary.Read(bytebuff, binary.BigEndian, &data)
	return int32(data)
}

func (s *IoTDBRpcDataSet) constructOneRow() error {
	// simulating buffer, read 8 bytes from data set and discard first 8 bytes which have been read.
	s.time = s.queryDataSet.Time[:8]
	s.queryDataSet.Time = s.queryDataSet.Time[8:]

	for i := 0; i < len(s.queryDataSet.BitmapList); i++ {
		bitmapBuffer := s.queryDataSet.BitmapList[i]
		if s.rowsIndex%8 == 0 {
			s.currentBitmap[i] = bitmapBuffer[0]
			s.queryDataSet.BitmapList[i] = bitmapBuffer[1:]
		}
		if !s.isNull(i, s.rowsIndex) {
			valueBuffer := s.queryDataSet.ValueList[i]
			dataType := s.columnTypeDeduplicatedList[i]
			switch dataType {
			case BOOLEAN:
				s.value[i] = valueBuffer[:1]
				s.queryDataSet.ValueList[i] = valueBuffer[1:]
				break
			case INT32:
				s.value[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
				break
			case INT64:
				s.value[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
				break
			case FLOAT:
				s.value[i] = valueBuffer[:4]
				s.queryDataSet.ValueList[i] = valueBuffer[4:]
				break
			case DOUBLE:
				s.value[i] = valueBuffer[:8]
				s.queryDataSet.ValueList[i] = valueBuffer[8:]
				break
			case TEXT:
				length := bytesToInt32(valueBuffer[:4])
				s.value[i] = valueBuffer[4 : 4+length]
				s.queryDataSet.ValueList[i] = valueBuffer[4+length:]
			default:
				return fmt.Errorf("unsupported data type %s", dataType)
			}
		}

	}
	s.rowsIndex++
	return nil
}

func (s *IoTDBRpcDataSet) getText(columnName string) string {
	if columnName == TIMESTAMP_STR {
		return time.Unix(0, bytesToLong(s.time)*1000000).UTC().Format(time.RFC3339)
	}

	index := s.columnOrdinalMap[columnName] - START_INDEX
	if index < 0 || int(index) >= len(s.value) || s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = true
		return ""
	}
	s.lastReadWasNull = false
	return s.getString(int(index), s.columnTypeDeduplicatedList[index])
}

func (s *IoTDBRpcDataSet) getString(index int, dataType int32) string {
	switch dataType {
	case BOOLEAN:
		if s.value[index][0] != 0 {
			return "true"
		}
		return "false"
	case INT32:
		return fmt.Sprintf("%v", bytesToInt32(s.value[index]))
	case INT64:
		return fmt.Sprintf("%v", bytesToLong(s.value[index]))
	case FLOAT:
		bits := binary.BigEndian.Uint32(s.value[index])
		f := math.Float32frombits(bits)
		return fmt.Sprintf("%v", f)
	case DOUBLE:
		bits := binary.BigEndian.Uint64(s.value[index])
		d := math.Float64frombits(bits)
		return fmt.Sprintf("%v", d)
	case TEXT:
		return string(s.value[index])
	default:
		return ""
	}
}

func (s *IoTDBRpcDataSet) getBool(columnName string) bool {
	index := s.columnOrdinalMap[columnName] - START_INDEX
	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false

		return s.value[index][0] != 0
	}
	s.lastReadWasNull = true
	return false

}

func (s *IoTDBRpcDataSet) getFloat(columnName string) float32 {

	index := s.columnOrdinalMap[columnName] - START_INDEX
	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		bits := binary.BigEndian.Uint32(s.value[index])
		return math.Float32frombits(bits)
	} else {
		s.lastReadWasNull = true
		return 0
	}

}

func (s *IoTDBRpcDataSet) getDouble(columnName string) float64 {
	index := s.columnOrdinalMap[columnName] - START_INDEX

	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		bits := binary.BigEndian.Uint64(s.value[index])
		return math.Float64frombits(bits)
	}
	s.lastReadWasNull = true
	return 0
}

func (s *IoTDBRpcDataSet) getInt32(columnName string) int32 {
	index := s.columnOrdinalMap[columnName] - START_INDEX
	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return bytesToInt32(s.value[index])
	}

	s.lastReadWasNull = true
	return 0
}

func bytesToLong(bys []byte) int64 {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	binary.Read(bytebuff, binary.BigEndian, &data)
	return int64(data)
}

func (s *IoTDBRpcDataSet) getInt64(columnName string) int64 {
	if columnName == TIMESTAMP_STR {
		return bytesToLong(s.time)
	}

	index := s.columnOrdinalMap[columnName] - START_INDEX
	bys := s.value[index]

	if !s.isNull(int(index), s.rowsIndex-1) {
		s.lastReadWasNull = false
		return bytesToLong(bys)
	} else {
		s.lastReadWasNull = true
		return 0
	}
}

func (s *IoTDBRpcDataSet) hasCachedResults() bool {
	return (s.queryDataSet != nil && len(s.queryDataSet.Time) > 0)
}

func (s *IoTDBRpcDataSet) next() (bool, error) {
	if s.hasCachedResults() {
		s.constructOneRow()
		return true, nil
	}
	if s.emptyResultSet {
		return false, nil
	}

	r, err := s.fetchResults()
	if err == nil && r {
		s.constructOneRow()
		return true, nil
	}
	return false, nil
}

func (s *IoTDBRpcDataSet) fetchResults() (bool, error) {
	s.rowsIndex = 0
	req := rpc.TSFetchResultsReq{s.sessionId, s.sql, s.fetchSize, s.queryId, true}
	resp, err := s.client.FetchResults(context.Background(), &req)
	if err != nil {
		return false, err
	}
	//   RpcUtils.verifySuccess(resp.getStatus());
	if !resp.HasResultSet {
		s.emptyResultSet = true
	} else {
		s.queryDataSet = resp.GetQueryDataSet()
	}
	return resp.HasResultSet, nil
}

func NewIoTDBRpcDataSet(sql string, columnNameList []string, columnTypes []string,
	columnNameIndex map[string]int32,
	queryId int64, client *rpc.TSIServiceClient, sessionId int64, queryDataSet *rpc.TSQueryDataSet,
	ignoreTimeStamp bool) *IoTDBRpcDataSet {

	typeMap := map[string]int32{
		"BOOLEAN": BOOLEAN,
		"INT32":   INT32,
		"INT64":   INT64,
		"FLOAT":   FLOAT,
		"DOUBLE":  DOUBLE,
		"TEXT":    TEXT,
	}

	ds := &IoTDBRpcDataSet{
		sql:                sql,
		columnNameList:     columnNameList,
		columnNameIndexMap: columnNameIndex,
		ignoreTimeStamp:    ignoreTimeStamp,
		queryId:            queryId,
		client:             client,
		sessionId:          sessionId,
		queryDataSet:       queryDataSet,
		fetchSize:          1024,
		currentBitmap:      make([]byte, len(columnNameList)),
		value:              make([][]byte, len(columnTypes)),
		columnCount:        len(columnNameList),
	}

	ds.columnTypeList = make([]int32, 0)

	if !ignoreTimeStamp {
		ds.columnNameList = append(ds.columnNameList, TIMESTAMP_STR)
		ds.columnTypeList = append(ds.columnTypeList, INT64)
	}
	// deduplicate and map
	ds.columnOrdinalMap = make(map[string]int32)
	if !ignoreTimeStamp {
		ds.columnOrdinalMap[TIMESTAMP_STR] = 1
	}

	if columnNameIndex != nil {
		ds.columnTypeDeduplicatedList = make([]int32, len(columnNameIndex))
		for i, name := range columnNameList {
			columnTypeString := columnTypes[i]
			ds.columnTypeList = append(ds.columnTypeList, typeMap[columnTypeString])
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				index := columnNameIndex[name]
				ds.columnOrdinalMap[name] = index + START_INDEX
				ds.columnTypeDeduplicatedList[index] = typeMap[columnTypeString]
			}

		}
	} else {
		ds.columnTypeDeduplicatedList = make([]int32, ds.columnCount)
		index := START_INDEX
		for i := 0; i < len(columnNameList); i++ {
			name := columnNameList[i]
			dataType := typeMap[columnTypes[i]]
			ds.columnTypeList = append(ds.columnTypeList, dataType)
			ds.columnTypeDeduplicatedList[i] = dataType
			if _, exists := ds.columnOrdinalMap[name]; !exists {
				ds.columnOrdinalMap[name] = int32(index)
				index++
			}
		}
	}

	return ds
}
