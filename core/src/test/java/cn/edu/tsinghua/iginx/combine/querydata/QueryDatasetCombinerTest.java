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
package cn.edu.tsinghua.iginx.combine.querydata;

import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.utils.RpcUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class QueryDatasetCombinerTest {

    private static QueryDataSetCombiner combiner = null;

    private static QueryDataResp resp = null;

    @Before
    public void setUp() throws Exception {
        combiner = QueryDataSetCombiner.getInstance();
        resp = new QueryDataResp(RpcUtils.SUCCESS);
    }

    @After
    public void tearDown() throws Exception {
        combiner = null;
        resp = null;
    }

    private List<QueryDataPlanExecuteResult> constructQueryDataPlanExecuteResults() {
        List<String> paths1 = new ArrayList<>();
        List<DataType> dataTypes1 = new ArrayList<>();
        paths1.add("time");
        dataTypes1.add(DataType.LONG);
        paths1.add("root.sg1.d1.s1");
        dataTypes1.add(DataType.LONG);
        paths1.add("root.sg1.d1.s2");
        dataTypes1.add(DataType.DOUBLE);

        List<List<Object>> valuesList1 = Arrays.asList(Arrays.asList(0L, 1L, 1.0), Arrays.asList(1L, 2L, 2.0));
        QueryExecuteDataSet stub1 = new QueryExecuteDataSetStub(paths1, dataTypes1, valuesList1);
        QueryDataPlanExecuteResult result1 = new QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, null, Collections.singletonList(stub1));

        List<List<Object>> valuesList2 = Arrays.asList(Arrays.asList(10L, 1L, 1.0), Arrays.asList(11L, 2L, 2.0));
        QueryExecuteDataSet stub2 = new QueryExecuteDataSetStub(paths1, dataTypes1, valuesList2);
        QueryDataPlanExecuteResult result2 = new QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, null, Collections.singletonList(stub2));

        List<String> paths2 = new ArrayList<>();
        List<DataType> dataTypes2 = new ArrayList<>();
        paths2.add("time");
        dataTypes2.add(DataType.LONG);
        paths2.add("root.sg2.d1.s1");
        dataTypes2.add(DataType.BOOLEAN);
        paths2.add("root.sg2.d1.s2");
        dataTypes2.add(DataType.STRING);

        List<List<Object>> valuesList3 = Arrays.asList(Arrays.asList(0L, true, "OK1".getBytes(StandardCharsets.UTF_8)), Arrays.asList(1L, true, "OK2".getBytes(StandardCharsets.UTF_8)));
        QueryExecuteDataSet stub3 = new QueryExecuteDataSetStub(paths2, dataTypes2, valuesList3);
        QueryDataPlanExecuteResult result3 = new QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, null, Collections.singletonList(stub3));

        List<List<Object>> valuesList4 = Arrays.asList(Arrays.asList(10L, false, "OK3".getBytes(StandardCharsets.UTF_8)), Arrays.asList(12L, false, "OK4".getBytes(StandardCharsets.UTF_8)));
        QueryExecuteDataSet stub4 = new QueryExecuteDataSetStub(paths2, dataTypes2, valuesList4);
        QueryDataPlanExecuteResult result4 = new QueryDataPlanExecuteResult(PlanExecuteResult.SUCCESS, null, Collections.singletonList(stub4));

        List<QueryDataPlanExecuteResult> resultList = new ArrayList<>();
        resultList.add(result1);
        resultList.add(result2);
        resultList.add(result3);
        resultList.add(result4);
        return resultList;
    }

    @Test
    public void testCombineResult() throws Exception {
        combiner.combineResult(resp, constructQueryDataPlanExecuteResults());
        SessionQueryDataSet sessionQueryDataSet = new SessionQueryDataSet(resp);
        assertArrayEquals(new long[]{0L, 1L, 10L, 11L, 12L}, sessionQueryDataSet.getTimestamps());
        List<String> paths = sessionQueryDataSet.getPaths();
        assertEquals(4, paths.size());
        assertEquals("root.sg1.d1.s1", paths.get(0));
        assertEquals("root.sg1.d1.s2", paths.get(1));
        assertEquals("root.sg2.d1.s1", paths.get(2));
        assertEquals("root.sg2.d1.s2", paths.get(3));

        List<List<Object>> valuesList = sessionQueryDataSet.getValues();

        assertEquals(1L, (long)valuesList.get(0).get(0));
        assertEquals(1.0, (double)valuesList.get(0).get(1), 0.01);
        assertTrue((boolean) valuesList.get(0).get(2));
        assertEquals("OK1", new String((byte[])valuesList.get(0).get(3)));

        assertEquals(2L, (long)valuesList.get(1).get(0));
        assertEquals(2.0, (double)valuesList.get(1).get(1), 0.01);
        assertTrue((boolean) valuesList.get(1).get(2));
        assertEquals("OK2", new String((byte[])valuesList.get(1).get(3)));

        assertEquals(1L, (long)valuesList.get(2).get(0));
        assertEquals(1.0, (double)valuesList.get(2).get(1), 0.01);
        assertFalse((boolean) valuesList.get(2).get(2));
        assertEquals("OK3", new String((byte[])valuesList.get(2).get(3)));

        assertEquals(2L, (long)valuesList.get(3).get(0));
        assertEquals(2.0, (double)valuesList.get(3).get(1), 0.01);
        assertNull(valuesList.get(3).get(2));
        assertNull(valuesList.get(3).get(3));

        assertNull(valuesList.get(4).get(0));
        assertNull(valuesList.get(4).get(1));
        assertFalse((boolean) valuesList.get(4).get(2));
        assertEquals("OK4", new String((byte[])valuesList.get(4).get(3)));
    }
}
