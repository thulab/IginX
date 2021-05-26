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
package cn.edu.tsinghua.iginx.combine.aggregate;

import cn.edu.tsinghua.iginx.query.result.AvgAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.SingleValueAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.StatisticsAggregateQueryPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.ByteUtils;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AggregateCombinerTest {

	private static AggregateCombiner combiner = null;

	private static AggregateQueryResp resp = null;

	@Before
	public void setUp() {
		combiner = AggregateCombiner.getInstance();
		resp = new AggregateQueryResp(RpcUtils.SUCCESS);
	}

	@After
	public void tearDown() {
		combiner = null;
		resp = null;
	}

	private List<SingleValueAggregateQueryPlanExecuteResult> constructSingleValueAggregateQueryPlanExecuteResults() {
		List<String> paths1 = new ArrayList<>();
		List<DataType> dataTypes1 = new ArrayList<>();
		paths1.add("root.sg1.d1.s1");
		dataTypes1.add(DataType.LONG);
		paths1.add("root.sg1.d1.s2");
		dataTypes1.add(DataType.DOUBLE);

		SingleValueAggregateQueryPlanExecuteResult result1 = new SingleValueAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result1.setPaths(paths1);
		result1.setDataTypes(dataTypes1);
		result1.setTimes(Arrays.asList(11L, 15L));
		result1.setValues(Arrays.asList(22L, 3.14));

		SingleValueAggregateQueryPlanExecuteResult result2 = new SingleValueAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result2.setPaths(paths1);
		result2.setDataTypes(dataTypes1);
		result2.setTimes(Arrays.asList(22L, 26L));
		result2.setValues(Arrays.asList(17L, 6.14));

		List<String> paths2 = new ArrayList<>();
		List<DataType> dataTypes2 = new ArrayList<>();
		paths2.add("root.sg2.d1.s1");
		dataTypes2.add(DataType.INTEGER);
		paths2.add("root.sg2.d1.s2");
		dataTypes2.add(DataType.FLOAT);

		SingleValueAggregateQueryPlanExecuteResult result3 = new SingleValueAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result3.setPaths(paths2);
		result3.setDataTypes(dataTypes2);
		result3.setTimes(Arrays.asList(12L, 16L));
		result3.setValues(Arrays.asList(23, 3.7F));

		SingleValueAggregateQueryPlanExecuteResult result4 = new SingleValueAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result4.setPaths(paths2);
		result4.setDataTypes(dataTypes2);
		result4.setTimes(Arrays.asList(24L, 27L));
		result4.setValues(Arrays.asList(16, 6.6F));

		List<SingleValueAggregateQueryPlanExecuteResult> resultList = new ArrayList<>();
		resultList.add(result1);
		resultList.add(result2);
		resultList.add(result3);
		resultList.add(result4);

		return resultList;
	}

	private List<StatisticsAggregateQueryPlanExecuteResult> constructStatisticsAggregateQueryPlanExecuteResults() {
		List<String> paths1 = new ArrayList<>();
		List<DataType> dataTypes1 = new ArrayList<>();
		paths1.add("root.sg1.d1.s1");
		dataTypes1.add(DataType.LONG);
		paths1.add("root.sg1.d1.s2");
		dataTypes1.add(DataType.DOUBLE);

		StatisticsAggregateQueryPlanExecuteResult result1 = new StatisticsAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result1.setPaths(paths1);
		result1.setDataTypes(dataTypes1);
		result1.setValues(Arrays.asList(22L, 3.14));

		StatisticsAggregateQueryPlanExecuteResult result2 = new StatisticsAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result2.setPaths(paths1);
		result2.setDataTypes(dataTypes1);
		result2.setValues(Arrays.asList(17L, 6.14));

		List<String> paths2 = new ArrayList<>();
		List<DataType> dataTypes2 = new ArrayList<>();
		paths2.add("root.sg2.d1.s1");
		dataTypes2.add(DataType.INTEGER);
		paths2.add("root.sg2.d1.s2");
		dataTypes2.add(DataType.FLOAT);

		StatisticsAggregateQueryPlanExecuteResult result3 = new StatisticsAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result3.setPaths(paths2);
		result3.setDataTypes(dataTypes2);
		result3.setValues(Arrays.asList(23, 3.7F));

		StatisticsAggregateQueryPlanExecuteResult result4 = new StatisticsAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result4.setPaths(paths2);
		result4.setDataTypes(dataTypes2);
		result4.setValues(Arrays.asList(16, 6.6F));

		List<StatisticsAggregateQueryPlanExecuteResult> resultList = new ArrayList<>();
		resultList.add(result1);
		resultList.add(result2);
		resultList.add(result3);
		resultList.add(result4);

		return resultList;
	}

	private List<AvgAggregateQueryPlanExecuteResult> constructAvgAggregateQueryPlanExecuteResult() {
		List<String> paths1 = new ArrayList<>();
		List<DataType> dataTypes1 = new ArrayList<>();
		paths1.add("root.sg1.d1.s1");
		dataTypes1.add(DataType.LONG);
		paths1.add("root.sg1.d1.s2");
		dataTypes1.add(DataType.DOUBLE);

		AvgAggregateQueryPlanExecuteResult result1 = new AvgAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result1.setPaths(paths1);
		result1.setDataTypes(dataTypes1);
		result1.setCounts(Arrays.asList(10L, 10L));
		result1.setSums(Arrays.asList(100L, 100.0));

		AvgAggregateQueryPlanExecuteResult result2 = new AvgAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result2.setPaths(paths1);
		result2.setDataTypes(dataTypes1);
		result2.setCounts(Arrays.asList(5L, 10L));
		result2.setSums(Arrays.asList(50L, 0.0));

		List<String> paths2 = new ArrayList<>();
		List<DataType> dataTypes2 = new ArrayList<>();
		paths2.add("root.sg2.d1.s1");
		dataTypes2.add(DataType.INTEGER);
		paths2.add("root.sg2.d1.s2");
		dataTypes2.add(DataType.FLOAT);

		AvgAggregateQueryPlanExecuteResult result3 = new AvgAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result3.setPaths(paths2);
		result3.setDataTypes(dataTypes2);
		result3.setCounts(Arrays.asList(10L, 1L));
		result3.setSums(Arrays.asList(100, 30.0F));

		AvgAggregateQueryPlanExecuteResult result4 = new AvgAggregateQueryPlanExecuteResult(PlanExecuteResult.SUCCESS, null);
		result4.setPaths(paths2);
		result4.setDataTypes(dataTypes2);
		result4.setCounts(Arrays.asList(10L, 2L));
		result4.setSums(Arrays.asList(100, 15.0F));

		List<AvgAggregateQueryPlanExecuteResult> resultList = new ArrayList<>();
		resultList.add(result1);
		resultList.add(result2);
		resultList.add(result3);
		resultList.add(result4);

		return resultList;
	}

	private void checkRespPathAndDataTypes(AggregateQueryResp resp) {
		Map<String, DataType> pathDataTypeMap = new HashMap<>();
		pathDataTypeMap.put("root.sg1.d1.s1", DataType.LONG);
		pathDataTypeMap.put("root.sg1.d1.s2", DataType.DOUBLE);
		pathDataTypeMap.put("root.sg2.d1.s1", DataType.INTEGER);
		pathDataTypeMap.put("root.sg2.d1.s2", DataType.FLOAT);

		assertEquals(pathDataTypeMap.size(), resp.paths.size());
		assertEquals(pathDataTypeMap.size(), resp.dataTypeList.size());

		for (int i = 0; i < resp.paths.size(); i++) {
			String path = resp.paths.get(i);
			DataType dataType = resp.dataTypeList.get(i);
			assertEquals(pathDataTypeMap.get(path), dataType);
		}

	}

	@Test
	public void testCombineFirstResult() {
		combiner.combineFirstResult(resp, constructSingleValueAggregateQueryPlanExecuteResults());
		checkRespPathAndDataTypes(resp);
		List<String> paths = resp.paths;
		List<DataType> dataTypes = resp.dataTypeList;
		long[] timestamps = ByteUtils.getLongArrayFromByteBuffer(resp.timestamps);
		Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, dataTypes);
		for (int i = 0; i < paths.size(); i++) {
			String path = paths.get(i);
			long timestamp = timestamps[i];
			Object value = values[i];
			switch (path) {
				case "root.sg1.d1.s1":
					assertEquals(11L, timestamp);
					assertEquals(22L, (long) value);
					break;
				case "root.sg1.d1.s2":
					assertEquals(15L, timestamp);
					assertEquals(3.14, (double) value, 0.001);
					break;
				case "root.sg2.d1.s1":
					assertEquals(12L, timestamp);
					assertEquals(23, (int) value);
					break;
				case "root.sg2.d1.s2":
					assertEquals(16L, timestamp);
					assertEquals(3.7F, (float) value, 0.001F);
					break;
				default:
					throw new RuntimeException();
			}
		}
	}

	@Test
	public void testCombineLastResult() {
		combiner.combineLastResult(resp, constructSingleValueAggregateQueryPlanExecuteResults());
		checkRespPathAndDataTypes(resp);
		List<String> paths = resp.paths;
		List<DataType> dataTypes = resp.dataTypeList;
		long[] timestamps = ByteUtils.getLongArrayFromByteBuffer(resp.timestamps);
		Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, dataTypes);
		for (int i = 0; i < paths.size(); i++) {
			String path = paths.get(i);
			long timestamp = timestamps[i];
			Object value = values[i];
			switch (path) {
				case "root.sg1.d1.s1":
					assertEquals(22, timestamp);
					assertEquals(17L, (long) value);
					break;
				case "root.sg1.d1.s2":
					assertEquals(26, timestamp);
					assertEquals(6.14, (double) value, 0.001);
					break;
				case "root.sg2.d1.s1":
					assertEquals(24L, timestamp);
					assertEquals(16, (int) value);
					break;
				case "root.sg2.d1.s2":
					assertEquals(27L, timestamp);
					assertEquals(6.6F, (float) value, 0.001F);
					break;
				default:
					throw new RuntimeException();
			}
		}
	}

	@Test
	public void testCombineMinResult() {
		combiner.combineMinResult(resp, constructSingleValueAggregateQueryPlanExecuteResults());
		checkRespPathAndDataTypes(resp);
		List<String> paths = resp.paths;
		List<DataType> dataTypes = resp.dataTypeList;
		long[] timestamps = ByteUtils.getLongArrayFromByteBuffer(resp.timestamps);
		Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, dataTypes);
		for (int i = 0; i < paths.size(); i++) {
			String path = paths.get(i);
			long timestamp = timestamps[i];
			Object value = values[i];
			switch (path) {
				case "root.sg1.d1.s1":
					assertEquals(22L, timestamp);
					assertEquals(17L, (long) value);
					break;
				case "root.sg1.d1.s2":
					assertEquals(15L, timestamp);
					assertEquals(3.14, (double) value, 0.001);
					break;
				case "root.sg2.d1.s1":
					assertEquals(24L, timestamp);
					assertEquals(16, (int) value);
					break;
				case "root.sg2.d1.s2":
					assertEquals(16L, timestamp);
					assertEquals(3.7F, (float) value, 0.001F);
					break;
				default:
					throw new RuntimeException();
			}
		}
	}

	@Test
	public void testCombineMaxResult() {
		combiner.combineMaxResult(resp, constructSingleValueAggregateQueryPlanExecuteResults());
		checkRespPathAndDataTypes(resp);
		List<String> paths = resp.paths;
		List<DataType> dataTypes = resp.dataTypeList;
		long[] timestamps = ByteUtils.getLongArrayFromByteBuffer(resp.timestamps);
		Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, dataTypes);
		for (int i = 0; i < paths.size(); i++) {
			String path = paths.get(i);
			long timestamp = timestamps[i];
			Object value = values[i];
			switch (path) {
				case "root.sg1.d1.s1":
					assertEquals(11L, timestamp);
					assertEquals(22L, (long) value);
					break;
				case "root.sg1.d1.s2":
					assertEquals(26L, timestamp);
					assertEquals(6.14, (double) value, 0.001);
					break;
				case "root.sg2.d1.s1":
					assertEquals(12L, timestamp);
					assertEquals(23, (int) value);
					break;
				case "root.sg2.d1.s2":
					assertEquals(27L, timestamp);
					assertEquals(6.6F, (float) value, 0.001F);
					break;
				default:
					throw new RuntimeException();
			}
		}
	}

	@Test
	public void testCombineSumOrCountResult() {
		combiner.combineSumOrCountResult(resp, constructStatisticsAggregateQueryPlanExecuteResults());
		checkRespPathAndDataTypes(resp);
		List<String> paths = resp.paths;
		List<DataType> dataTypes = resp.dataTypeList;
		Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, dataTypes);
		for (int i = 0; i < paths.size(); i++) {
			String path = paths.get(i);
			Object value = values[i];
			switch (path) {
				case "root.sg1.d1.s1":
					assertEquals(39L, (long) value);
					break;
				case "root.sg1.d1.s2":
					assertEquals(9.28, (double) value, 0.001);
					break;
				case "root.sg2.d1.s1":
					assertEquals(39, (int) value);
					break;
				case "root.sg2.d1.s2":
					assertEquals(10.3F, (float) value, 0.001F);
					break;
				default:
					throw new RuntimeException();
			}
		}
	}

	@Test
	public void testCombineAvgResult() {
		combiner.combineAvgResult(resp, constructAvgAggregateQueryPlanExecuteResult());
		List<String> paths = resp.paths;
		List<DataType> dataTypes = resp.dataTypeList;
		Object[] values = ByteUtils.getValuesByDataType(resp.valuesList, dataTypes);
		for (int i = 0; i < paths.size(); i++) {
			String path = paths.get(i);
			Object value = values[i];
			switch (path) {
				case "root.sg1.d1.s1":
					assertEquals(10L, (long) value);
					break;
				case "root.sg2.d1.s1":
					assertEquals(10, (int) value);
					break;
				case "root.sg1.d1.s2":
					assertEquals(5.0, (double) value, 0.001);
					break;
				case "root.sg2.d1.s2":
					assertEquals(15.0F, (float) value, 0.001F);
					break;
				default:
					throw new RuntimeException();
			}
		}
	}


}
