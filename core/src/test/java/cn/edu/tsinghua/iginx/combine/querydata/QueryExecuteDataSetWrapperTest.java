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

import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryExecuteDataSetWrapperTest {

	private static QueryExecuteDataSetStub stub;

	private static QueryExecuteDataSetWrapper wrapper;

	@Before
	public void setUp() throws Exception {
		List<String> columnNames = Arrays.asList("time", "col0", "col1", "col2", "col3");
		List<DataType> columnTypes = Arrays.asList(DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG, DataType.LONG);
		List<List<Object>> valuesList = Arrays.asList(Arrays.asList(0L, 1L, 2L, 3L, 4L), Arrays.asList(1L, 2L, 3L, 4L, 5L));
		stub = new QueryExecuteDataSetStub(columnNames, columnTypes, valuesList);
		wrapper = new QueryExecuteDataSetWrapper(stub);
	}

	@After
	public void tearDown() throws Exception {
		stub = null;
		wrapper = null;
	}

	@Test
	public void testClose() throws Exception {
		wrapper.close();
		assertTrue(stub.isClosed());
	}

	@Test
	public void testGetColumnNames() throws Exception {
		List<String> columnNames = wrapper.getColumnNames();
		for (int i = 0; i < columnNames.size(); i++) {
			assertEquals("col" + i, columnNames.get(i));
		}
	}

	@Test
	public void testGetColumnTypes() throws Exception {
		List<DataType> columnTypes = wrapper.getColumnTypes();
		for (DataType dataType : columnTypes) {
			assertEquals(DataType.LONG, dataType);
		}
	}

	@Test
	public void testHasNext() throws Exception {
		assertTrue(wrapper.hasNext());
		wrapper.next();
		assertTrue(wrapper.hasNext());
		wrapper.next();
		assertFalse(wrapper.hasNext());
	}

	@Test
	public void testNext() throws Exception {
		assertTrue(wrapper.hasNext());
		wrapper.next();
		assertTrue(wrapper.hasNext());
		wrapper.next();
		assertFalse(wrapper.hasNext());
	}

	@Test
	public void testGetTimestamp() throws Exception {
		wrapper.next();
		assertEquals(0L, wrapper.getTimestamp());
		wrapper.next();
		assertEquals(1L, wrapper.getTimestamp());
	}

	@Test
	public void testGetValue() throws Exception {
		for (int i = 0; i < 2; i++) {
			wrapper.next();
			for (int j = 0; j < 4; j++) {
				assertEquals(i + j + 1, (long) wrapper.getValue("col" + j));
			}
		}
	}

}
