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

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ValueFilterQueryResp;
import cn.edu.tsinghua.iginx.utils.Bitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class SessionQueryDataSet {

	private final long[] timestamps;
	private List<String> paths;
	private List<List<Object>> values;

	public SessionQueryDataSet(QueryDataResp resp) {
		this.paths = resp.getPaths();
		this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
		parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
	}

	public SessionQueryDataSet(ValueFilterQueryResp resp) {
		this.paths = resp.getPaths();
		this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
		parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
	}

	public SessionQueryDataSet(DownsampleQueryResp resp) {
		this.paths = resp.getPaths();
		if (resp.queryDataSet != null) {
			this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
			parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
		} else {
			this.timestamps = new long[0];
			values = new ArrayList<>();
		}
		if (this.paths == null) {
			this.paths = new ArrayList<>();
		}

	}

	private void parseValues(List<DataType> dataTypeList, List<ByteBuffer> valuesList, List<ByteBuffer> bitmapList) {
		values = new ArrayList<>();
		for (int i = 0; i < valuesList.size(); i++) {
			List<Object> tempValues = new ArrayList<>();
			ByteBuffer valuesBuffer = valuesList.get(i);
			ByteBuffer bitmapBuffer = bitmapList.get(i);
			Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapBuffer.array());
			for (int j = 0; j < dataTypeList.size(); j++) {
				if (bitmap.get(j)) {
					tempValues.add(getValueFromByteBufferByDataType(valuesBuffer, dataTypeList.get(j)));
				} else {
					tempValues.add(null);
				}
			}
			values.add(tempValues);
		}
	}

	public List<String> getPaths() {
		return paths;
	}

	public long[] getTimestamps() {
		return timestamps;
	}

	public List<List<Object>> getValues() {
		return values;
	}

	public void print() {
		System.out.println("Start to Print ResultSets:");
		System.out.print("Time\t");
		for (String path : paths) {
			System.out.print(path + "\t");
		}
		System.out.println();

		for (int i = 0; i < timestamps.length; i++) {
			System.out.print(timestamps[i] + "\t");
			for (int j = 0; j < paths.size(); j++) {
				if (values.get(i).get(j) instanceof byte[]) {
					System.out.print(new String((byte[]) values.get(i).get(j)) + "\t");
				} else {
					System.out.print(values.get(i).get(j) + "\t");
				}
			}
			System.out.println();
		}
		System.out.println("Printing ResultSets Finished.");
	}
}
