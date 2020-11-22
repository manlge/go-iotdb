/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"fmt"
	"os"
	"time"

	"github.com/manlge/go-iotdb/client"
)

var session client.Session

func main() {
	session = client.NewSession("127.0.0.1", "6667")
	session.Open(false, 0)

	defer session.Close()
	setStorageGroup()
	deleteStorageGroup()

	setStorageGroup()
	deleteStorageGroups()
	createTimeseries()
	createMultiTimeseries()
	deleteData()
	deleteTimeseries()
	insertStringRecord()
	setTimeZone()
	println(getTimeZone())
	session.InsertRecord("root.ln.device1", []string{"description", "price", "tick_count", "status", "restart_count", "temperature"}, []int32{client.TEXT, client.DOUBLE, client.INT64, client.BOOLEAN, client.INT32, client.FLOAT},
		[]interface{}{string("Test Device 1"), float64(1988.20), int64(3333333), true, int32(1), float32(12.10)}, time.Now().UnixNano()/1000000)

	sessionDataSet, err := session.ExecuteQueryStatement("SHOW TIMESERIES")

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	printDataSet(sessionDataSet)

	ds, err := session.ExecuteQueryStatement("select * from root.ln.device1")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	printDataSet(ds)

}

func printDataSet(sessionDataSet *client.SessionDataSet) {
	for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
		fmt.Printf("%s \t", sessionDataSet.GetColumnName(i))
	}
	println()

	for next, err := sessionDataSet.Next(); err == nil && next; next, err = sessionDataSet.Next() {
		for i := 0; i < sessionDataSet.GetColumnCount(); i++ {
			columnName := sessionDataSet.GetColumnName(i)
			// fmt.Printf("%s", sessionDataSet.GetText(columnName))
			switch sessionDataSet.GetColumnDataType(i) {
			case client.BOOLEAN:
				fmt.Print(sessionDataSet.GetBool(columnName))
				break
			case client.INT32:
				fmt.Print(sessionDataSet.GetInt32(columnName))
				break
			case client.INT64:
				fmt.Print(sessionDataSet.GetInt64(columnName))
				break
			case client.FLOAT:
				fmt.Print(sessionDataSet.GetFloat(columnName))
				break
			case client.DOUBLE:
				fmt.Print(sessionDataSet.GetDouble(columnName))
				break
			case client.TEXT:
				fmt.Print(sessionDataSet.GetText(columnName))
			default:
			}
			fmt.Print("\t\t")
		}
		fmt.Println()
	}
}

func setStorageGroup() {
	var storageGroupId = "root.ln1"
	session.SetStorageGroup(storageGroupId)
}

func deleteStorageGroup() {
	var storageGroupId = "root.ln1"
	session.DeleteStorageGroup(storageGroupId)
}

func deleteStorageGroups() {
	var storageGroupId = []string{"root.ln1"}
	session.DeleteStorageGroups(storageGroupId)
}

func createTimeseries() {
	var path = "root.sg1.dev1.status"
	var dataType = client.FLOAT
	var encoding = client.PLAIN
	var compressor = client.SNAPPY
	session.CreateTimeseries(path, dataType, encoding, compressor)
}

func createMultiTimeseries() {
	var paths = []string{"root.sg1.dev1.temperature"}
	var dataTypes = []int32{client.TEXT}
	var encodings = []int32{client.PLAIN}
	var compressors = []int32{client.SNAPPY}
	session.CreateMultiTimeseries(paths, dataTypes, encodings, compressors)
}

func deleteTimeseries() {
	var paths = []string{"root.sg1.dev1.status"}
	session.DeleteTimeseries(paths)
}

func insertStringRecord() {
	var deviceId = "root.ln.wf02.wt02"
	var measurements = []string{"hardware"}
	var values = []string{"123"}
	var timestamp int64 = 12
	session.InsertStringRecord(deviceId, measurements, values, timestamp)
}

func deleteData() {
	var paths = []string{"root.sg1.dev1.status"}
	var startTime int64 = 0
	var endTime int64 = 12
	session.DeleteData(paths, startTime, endTime)
}

func setTimeZone() {
	var timeZone = "GMT"
	session.SetTimeZone(timeZone)
}

func getTimeZone() (string, error) {
	return session.GetTimeZone()
}
