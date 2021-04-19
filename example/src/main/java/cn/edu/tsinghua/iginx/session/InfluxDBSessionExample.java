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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBSessionExample {

	private static Session session;

	private static final String BUCKET_NAME = "demo";

	// bucket measurement tag(key-value) field
	private static final String TS1 = "demo.census.klamath.anderson.ants";
	private static final String TS2 = "demo.census.klamath.anderson.bees";
	private static final String TS3 = "demo.census.portland.mullen.ants";
	private static final String TS4 = "demo.census.portland.mullen.bees";

	public static void main(String[] args) throws SessionException, ExecutionException, TTransportException {
		session = new Session("127.0.0.1", 6324, "root", "root");
		session.openSession();

		createDatabase();

		insertRecords();
		queryData();
		aggregateQuery();
		// TODO 不能做，InfluxDB 删除语句中不能指定 _field
//		deleteDataInColumns();
//		queryData();

		dropDatabase();

		session.closeSession();
	}

	private static void createDatabase() throws SessionException, ExecutionException {
		session.createDatabase(BUCKET_NAME);
	}

	private static void dropDatabase() throws SessionException, ExecutionException {
		session.dropDatabase(BUCKET_NAME);
	}

	private static void insertRecords() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);
		paths.add(TS4);

		long[] timestamps = new long[100];
		for (long i = 0; i < 100; i++) {
			timestamps[(int) i] = i;
		}

		Object[] valuesList = new Object[4];
		for (long i = 0; i < 4; i++) {
			Object[] values = new Object[100];
			for (long j = 0; j < 100; j++) {
				values[(int) j] = i + j;
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			dataTypeList.add(DataType.LONG);
		}

		session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void queryData() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);
		paths.add(TS4);

		long startTime = 0L;
		long endTime = 55L;

		SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
		dataSet.print();
	}

	private static void deleteDataInColumns() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);

		long startTime = 25L;
		long endTime = 30L;

		session.deleteDataInColumns(paths, startTime, endTime);
	}

	private static void aggregateQuery() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);
		paths.add(TS4);

		long startTime = 5L;
		long endTime = 55L;

		SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.SUM);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.AVG);
		dataSet.print();
	}
}
