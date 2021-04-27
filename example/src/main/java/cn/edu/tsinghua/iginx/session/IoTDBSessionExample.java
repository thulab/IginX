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
import org.apache.iotdb.session.SessionDataSet;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBSessionExample {

	private static Session session;

	private static final String DATABASE_NAME = "sg1";
	private static final String S1 = "sg1.d1.s1";
	private static final String S2 = "sg1.d2.s2";
	private static final String S3 = "sg1.d3.s3";
	private static final String S4 = "sg1.d4.s4";

	private static final long COLUMN_START_TIMESTAMP = 0L;
	private static final long COLUMN_END_TIMESTAMP = 10500L;
	private static final long ROW_START_TIMESTAMP = 10501L;
	private static final long ROW_END_TIMESTAMP = 21000L;
	private static final int ROW_INTERVAL = 10;

	public static void main(String[] args) throws SessionException, ExecutionException, TTransportException {
		session = new Session("127.0.0.1", 6324, "root", "root");
		// 打开 Session
		session.openSession();

		// 创建数据库
		session.createDatabase(DATABASE_NAME);

		// 添加列
		addColumns();
		// 列式插入数据
		//insertColumnRecords();
		// 行式插入数据
		insertRowRecords();
		// 查询数据
		//queryData();
		// 聚合查询数据
		//aggregateQuery();
		// 降采样聚合查询
		downsampleQuery();
		// 删除数据
		//deleteDataInColumns();
		// 再次查询数据
		//queryData();
		// 删除列
		//deleteColumns();

		// 删除数据库
		//session.dropDatabase(DATABASE_NAME);

		// 关闭 Session
		session.closeSession();
	}

	private static void addColumns() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		List<Map<String, String>> attributes = new ArrayList<>();
		Map<String, String> attributesForOnePath = new HashMap<>();

		// 先添加两条数据类型为 Long 的时间序列
		paths.add(S1);
		paths.add(S2);

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

		paths.add(S3);
		paths.add(S4);

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
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		int size = (int) (COLUMN_END_TIMESTAMP - COLUMN_START_TIMESTAMP);
		long[] timestamps = new long[size];
		for (long i = 0; i < size; i++) {
			timestamps[(int) i] = i;
		}

		Object[] valuesList = new Object[4];
		for (long i = 0; i < 4; i++) {
			Object[] values = new Object[size];
			for (long j = 0; j < size; j++) {
				if (j >= size - 50) {
					values[(int) j] = null;
				} else {
					if (i < 2) {
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

		session.insertColumnRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void insertRowRecords() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		int size = (int) (ROW_END_TIMESTAMP - ROW_START_TIMESTAMP) / ROW_INTERVAL;
		long[] timestamps = new long[size];
		Object[] valuesList = new Object[size];
		for (long i = 0; i < size; i++) {
			timestamps[(int) i] = ROW_START_TIMESTAMP + i * ROW_INTERVAL;
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
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		long startTime = COLUMN_END_TIMESTAMP - 100L;
		long endTime = ROW_START_TIMESTAMP + 100L;

		SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
		dataSet.print();
	}

	private static void downsampleQuery() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);

		long startTime = ROW_START_TIMESTAMP;
		long endTime = ROW_END_TIMESTAMP + 1;

		System.out.println("Downsample Query: ");

		// MAX
		SessionQueryDataSet dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MAX, ROW_INTERVAL * 100);
		dataSet.print();

		// MIN
		dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.MIN, ROW_INTERVAL * 100);
		dataSet.print();

		// FIRST
		dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.FIRST, ROW_INTERVAL * 100);
		dataSet.print();

		// LAST
		dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.LAST, ROW_INTERVAL * 100);
		dataSet.print();

		// COUNT
		dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.COUNT, ROW_INTERVAL * 100);
		dataSet.print();

		// SUM
		dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.SUM, ROW_INTERVAL * 100);
		dataSet.print();

		// AVG
		dataSet = session.downsampleQuery(paths, startTime, endTime, AggregateType.AVG, ROW_INTERVAL * 100);
		dataSet.print();

		// 降采样查询结束
		System.out.println("Downsample Query Finished.");
	}

	private static void aggregateQuery() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);

		long startTime = COLUMN_END_TIMESTAMP - 100L;
		long endTime = ROW_START_TIMESTAMP + 100L;

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
		paths.add(S1);
		paths.add(S3);
		paths.add(S4);

		long startTime = COLUMN_END_TIMESTAMP - 50L;
		long endTime = ROW_START_TIMESTAMP + 50L;

		session.deleteDataInColumns(paths, startTime, endTime);
	}

	private static void deleteColumns() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(S1);
		paths.add(S2);
		paths.add(S3);
		paths.add(S4);

		session.deleteColumns(paths);
	}
}
