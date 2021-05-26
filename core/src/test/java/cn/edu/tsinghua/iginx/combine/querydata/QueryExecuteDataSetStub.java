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

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

public class QueryExecuteDataSetStub implements QueryExecuteDataSet {

	private final List<String> columnNames;

	private final List<DataType> columnTypes;

	private final List<List<Object>> valuesList;

	private int index;

	private boolean closed;

	public QueryExecuteDataSetStub(List<String> columnNames, List<DataType> columnTypes, List<List<Object>> valuesList) {
		this.columnNames = columnNames;
		this.columnTypes = columnTypes;
		this.valuesList = valuesList;
		this.index = -1;
		this.closed = false;
	}

	@Override
	public List<String> getColumnNames() throws ExecutionException {
		return columnNames;
	}

	@Override
	public List<DataType> getColumnTypes() throws ExecutionException {
		return columnTypes;
	}

	@Override
	public boolean hasNext() throws ExecutionException {
		return index < this.valuesList.size() - 1;
	}

	@Override
	public RowRecord next() throws ExecutionException {
		List<Object> values = this.valuesList.get(++index);
		long timestamp = (long) values.get(0);
		return new RowRecord(timestamp, values.subList(1, values.size()));
	}

	@Override
	public void close() throws ExecutionException {
		this.closed = true;
	}

	public boolean isClosed() {
		return closed;
	}
}
