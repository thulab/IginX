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
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBSessionExample {

	private static Session session;

	private static final String DATABASE_NAME = "sg1";
	private static final String COLUMN_D1_S1 = "sg1.d1.s1";
	private static final String COLUMN_D1_S2 = "sg1.d2.s2";
	private static final String COLUMN_D2_S1 = "sg1.d3.s3";
	private static final String COLUMN_D3_S1 = "sg1.d4.s4";

	private static final long START_TIMESTAMP = 20000L;
	private static final long END_TIMESTAMP = 20500L;
	private static final int INTERVAL = 10;

	public static void main(String[] args) throws SessionException, ExecutionException, TTransportException {
		session = new Session("127.0.0.1", 6324, "root", "root");
		// 打开 Session
		session.openSession();

		// 创建数据库
		session.createDatabase(DATABASE_NAME);

		// 添加列
		addColumns();
		// 列式插入数据
		insertColumnRecords();
		// 行式插入数据
		insertRowRecords();
		// 查询数据
		queryData();
		// 聚合查询数据
		aggregateQuery();
		// 删除数据
		deleteDataInColumns();
		// 再次查询数据
		queryData();
		// 删除列
		deleteColumns();

		// 删除数据库
		session.dropDatabase(DATABASE_NAME);

		// 关闭 Session
		session.closeSession();
	}

	private static void addColumns() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		List<Map<String, String>> attributes = new ArrayList<>();
		Map<String, String> attributesForOnePath = new HashMap<>();

		// 先添加两条数据类型为 Long 的时间序列
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);

		// INT64
		attributesForOnePath.put("DataType", "2");
		// RLE
		attributesForOnePath.put("Encoding", "2");
		// SNAPPY
		attributesForOnePath.put("Compression", "1");

		for (int i = 0; i < 2; i++) {
			attributes.add(attributesForOnePath);
		}

		session.addColumns(paths, attributes);

		// 再添加两条数据类型为 Long 的时间序列
		paths.clear();
		attributes.clear();
		attributesForOnePath.clear();

		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		// TEXT
		attributesForOnePath.put("DataType", "5");
		// PLAIN
		attributesForOnePath.put("Encoding", "0");
		// SNAPPY
		attributesForOnePath.put("Compression", "1");

		for (int i = 0; i < 2; i++) {
			attributes.add(attributesForOnePath);
		}

		session.addColumns(paths, attributes);
	}

	private static void insertColumnRecords() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		long[] timestamps = new long[10500];
		for (long i = 0; i < 10500; i++) {
			timestamps[(int) i] = i;
		}

		Object[] valuesList = new Object[4];
		for (long i = 0; i < 4; i++) {
			Object[] values = new Object[10500];
			if (i < 2) {
				for (long j = 0; j < 10500; j++) {
					values[(int) j] = i + j;
				}
			} else {
				for (long j = 0; j < 10500; j++) {
					values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
				}
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			dataTypeList.add(DataType.LONG);
		}
		for (int i = 0; i < 2; i++) {
			dataTypeList.add(DataType.BINARY);
		}

		session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void insertRowRecords() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		int size = (int)(END_TIMESTAMP - START_TIMESTAMP) / INTERVAL;
		long[] timestamps = new long[size];
		Object[] valuesList = new Object[size];
		for (long i = 0; i < size; i++) {
			timestamps[(int) i] = START_TIMESTAMP + i * INTERVAL;
			Object[] values = new Object[4];
			for (long j = 0; j < 4; j++) {
				if ((i + j) % 2 == 0) {
					values[(int) j] = null;
				} else {
					if (j < 2) {
						values[(int) j] = i + j;
					} else {
						values[(int) j] = RandomStringUtils.randomAlphanumeric(10).getBytes();
					}
				}
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 2; i++) {
			dataTypeList.add(DataType.LONG);
		}
		for (int i = 0; i < 2; i++) {
			dataTypeList.add(DataType.BINARY);
		}

		session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void queryData() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		long startTime = 10000L;
		long endTime = 10050L;

		SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
		dataSet.print();
	}

	private static void aggregateQuery() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);

		long startTime = 10050L;
		long endTime = 10300L;

		// MAX
		SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
		dataSet.print();

		// MIN
		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
		dataSet.print();

		// FIRST
		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST);
		dataSet.print();

		// LAST
		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST);
		dataSet.print();

		// COUNT
		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT);
		dataSet.print();

		// SUM
		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.SUM);
		dataSet.print();

		// AVG
		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.AVG);
		dataSet.print();
	}

	private static void deleteDataInColumns() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		long startTime = 10025L;
		long endTime = 10030L;

		session.deleteDataInColumns(paths, startTime, endTime);
	}

	private static void deleteColumns() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		session.deleteColumns(paths);
	}
}
