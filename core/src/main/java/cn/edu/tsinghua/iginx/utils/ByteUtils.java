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
package cn.edu.tsinghua.iginx.utils;

import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ByteUtils {

	public static byte booleanToByte(boolean x) {
		if (x) {
			return 1;
		} else {
			return 0;
		}
	}

	public static Object[] getValuesByDataType(ByteBuffer valuesList, List<DataType> dataTypeList) {
		Object[] values = new Object[dataTypeList.size()];
		for (int i = 0; i < values.length; i++) {
			switch (dataTypeList.get(i)) {
				case BOOLEAN:
					values[i] = valuesList.get() == 1;
					break;
				case INTEGER:
					values[i] = valuesList.getInt();
					break;
				case LONG:
					values[i] = valuesList.getLong();
					break;
				case FLOAT:
					values[i] = valuesList.getFloat();
					break;
				case DOUBLE:
					values[i] = valuesList.getDouble();
					break;
				case STRING:
					int length = valuesList.getInt();
					byte[] bytes = new byte[length];
					valuesList.get(bytes, 0, length);
					values[i] = new String(bytes, 0, length);
//					values[i] = bytes;
					break;
				default:
					throw new UnsupportedDataTypeException(dataTypeList.get(i).toString());
			}
		}
		return values;
	}

	public static Object[] getValuesListByDataType(List<ByteBuffer> valuesList, List<DataType> dataTypeList) {
		Object[] tempValues = new Object[valuesList.size()];
		for (int i = 0; i < valuesList.size(); i++) {
			switch (dataTypeList.get(i)) {
				case BOOLEAN:
					tempValues[i] = getBooleanArrayFromByteBuffer(valuesList.get(i));
					break;
				case INTEGER:
					tempValues[i] = getIntArrayFromByteBuffer(valuesList.get(i));
					break;
				case LONG:
					tempValues[i] = getLongArrayFromByteBuffer(valuesList.get(i));
					break;
				case FLOAT:
					tempValues[i] = getFloatArrayFromByteBuffer(valuesList.get(i));
					break;
				case DOUBLE:
					tempValues[i] = getDoubleArrayFromByteBuffer(valuesList.get(i));
					break;
				case STRING:
					tempValues[i] = getStringArrayFromByteBuffer(valuesList.get(i));
					break;
				default:
					throw new UnsupportedOperationException(dataTypeList.get(i).toString());
			}
		}
		return tempValues;
	}

	public static Object[] getRowValuesListByDataType(List<ByteBuffer> valuesList, List<DataType> dataTypeList, List<ByteBuffer> bitmapList) {
		Object[] tempValues = new Object[valuesList.size()];
		for (int i = 0; i < valuesList.size(); i++) {
			Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapList.get(i).array());
			List<Integer> indexes = new ArrayList<>();
			for (int j = 0; j < dataTypeList.size(); j++) {
				if (bitmap.get(j)) {
					indexes.add(j);
				}
			}
			Object[] tempRowValues = new Object[indexes.size()];
			for (int j = 0; j < indexes.size(); j++) {
				switch (dataTypeList.get(indexes.get(j))) {
					case BOOLEAN:
						tempRowValues[j] = valuesList.get(i).get() == 1;
						break;
					case INTEGER:
						tempRowValues[j] = valuesList.get(i).getInt();
						break;
					case LONG:
						tempRowValues[j] = valuesList.get(i).getLong();
						break;
					case FLOAT:
						tempRowValues[j] = valuesList.get(i).getFloat();
						break;
					case DOUBLE:
						tempRowValues[j] = valuesList.get(i).getDouble();
						break;
					case STRING:
						int length = valuesList.get(i).getInt();
						byte[] bytes = new byte[length];
						valuesList.get(i).get(bytes, 0, length);
						tempRowValues[j] = new String(bytes, 0, length);
						break;
					default:
						throw new UnsupportedOperationException(dataTypeList.get(i).toString());
				}
			}
			tempValues[i] = tempRowValues;
		}
		return tempValues;
	}

	public static byte[] getByteArrayFromLongArray(long[] array) {
		ByteBuffer buffer = ByteBuffer.allocate(array.length * 8);
		buffer.asLongBuffer().put(array);
		return buffer.array();
	}

	public static long[] getLongArrayFromByteArray(byte[] byteArray) {
		ByteBuffer buffer = ByteBuffer.wrap(byteArray);
		long[] longArray = new long[byteArray.length / 8];
		for (int i = 0; i < longArray.length; i++) {
			longArray[i] = buffer.getLong();
		}
		return longArray;
	}

	public static boolean[] getBooleanArrayFromByteBuffer(ByteBuffer buffer) {
		boolean[] array = new boolean[buffer.array().length];
		for (int i = 0; i < array.length; i++) {
			array[i] = buffer.get() == 1;
		}
		return array;
	}

	public static int[] getIntArrayFromByteBuffer(ByteBuffer buffer) {
		int[] array = new int[buffer.array().length / 4];
		for (int i = 0; i < array.length; i++) {
			array[i] = buffer.getInt();
		}
		return array;
	}

	public static long[] getLongArrayFromByteBuffer(ByteBuffer buffer) {
		long[] array = new long[buffer.array().length / 8];
		for (int i = 0; i < array.length; i++) {
			array[i] = buffer.getLong();
		}
		return array;
	}

	public static float[] getFloatArrayFromByteBuffer(ByteBuffer buffer) {
		float[] array = new float[buffer.array().length / 4];
		for (int i = 0; i < array.length; i++) {
			array[i] = buffer.getFloat();
		}
		return array;
	}

	public static double[] getDoubleArrayFromByteBuffer(ByteBuffer buffer) {
		double[] array = new double[buffer.array().length / 8];
		for (int i = 0; i < array.length; i++) {
			array[i] = buffer.getDouble();
		}
		return array;
	}

	public static String[] getStringArrayFromByteBuffer(ByteBuffer buffer) {
		List<String> stringList = new ArrayList<>();
		int cnt = 0;
		while (cnt < buffer.array().length) {
			int length = buffer.getInt();
			byte[] bytes = new byte[length];
			buffer.get(bytes, 0, length);
			stringList.add(new String(bytes, 0, length));
			cnt += length + 4;
		}
		return stringList.toArray(new String[0]);
	}

//	public static byte[][] getStringArrayFromByteBuffer(ByteBuffer buffer) {
//		List<byte[]> bytesList = new ArrayList<>();
//		int cnt = 0;
//		while (cnt < buffer.array().length) {
//			int length = buffer.getInt();
//			byte[] bytes = new byte[length];
//			buffer.get(bytes, 0, length);
//			bytesList.add(bytes);
//			cnt += length + 4;
//		}
//		return null;
////		return bytesList.toArray(new byte[][]);
//	}

	public static List<ByteBuffer> getByteBufferByDataType(Object[] valuesList, List<DataType> dataTypeList) {
		List<ByteBuffer> byteBufferList = new ArrayList<>();
		for (int i = 0; i < valuesList.length; i++) {
			byteBufferList.add(getByteBuffer((Object[]) valuesList[i], dataTypeList.get(i)));
		}
		return byteBufferList;
	}

	public static ByteBuffer getByteBuffer(Object[] values, List<DataType> dataTypes) {
		ByteBuffer buffer = ByteBuffer.allocate(getByteBufferSize(values, dataTypes));
		for (int i = 0; i < dataTypes.size(); i++) {
			DataType dataType = dataTypes.get(i);
			Object value = values[i];
			if (value == null) {
				continue;
			}
			switch (dataType) {
				case BOOLEAN:
					buffer.put(booleanToByte((boolean) value));
					break;
				case INTEGER:
					buffer.putInt((int) value);
					break;
				case LONG:
					buffer.putLong((long) value);
					break;
				case FLOAT:
					buffer.putFloat((float) value);
					break;
				case DOUBLE:
					buffer.putDouble((double) value);
					break;
				case STRING:
					buffer.putInt(((String) value).getBytes().length);
					buffer.put(((String) value).getBytes());
					break;
				default:
					throw new UnsupportedOperationException(dataType.toString());
			}
		}
		buffer.flip();
		return buffer;
	}

	public static int getByteBufferSize(Object[] values, List<DataType> dataTypes) {
		int size = 0;
		for (int i = 0; i < dataTypes.size(); i++) {
			DataType dataType = dataTypes.get(i);
			Object value = values[i];
			if (value == null) {
				continue;
			}
			switch (dataType) {
				case BOOLEAN:
					size += 1;
					break;
				case INTEGER:
				case FLOAT:
					size += 4;
					break;
				case LONG:
				case DOUBLE:
					size += 8;
					break;
				case STRING:
					size += 4 + ((String) value).getBytes().length;
					break;
				default:
					throw new UnsupportedOperationException(dataType.toString());
			}
		}
		return size;
	}

	public static ByteBuffer getByteBuffer(Long[] values) {
		ByteBuffer buffer = ByteBuffer.allocate(8 * values.length);
		for (long value: values) {
			buffer.putLong(value);
		}
		buffer.flip();
		return buffer;
	}

	public static ByteBuffer getByteBuffer(Object[] values, DataType dataType) {
		ByteBuffer buffer = ByteBuffer.allocate(getByteBufferSize(values, dataType));
		switch (dataType) {
			case BOOLEAN:
				for (Object value : values) {
					buffer.put(booleanToByte((boolean) value));
				}
				break;
			case INTEGER:
				for (Object value : values) {
					buffer.putInt((int) value);
				}
				break;
			case LONG:
				for (Object value : values) {
					buffer.putLong((long) value);
				}
				break;
			case FLOAT:
				for (Object value : values) {
					buffer.putFloat((float) value);
				}
				break;
			case DOUBLE:
				for (Object value : values) {
					buffer.putDouble((double) value);
				}
				break;
			case STRING:
				for (Object value : values) {
					buffer.putInt(((String) value).getBytes().length);
					buffer.put(((String) value).getBytes());
				}
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}
		buffer.flip();
		return buffer;
	}

	public static ByteBuffer getByteBufferFromTimestamps(List<Long> timestamps) {
		ByteBuffer buffer = ByteBuffer.allocate(timestamps.size() * 8);
		for (Long timestamp : timestamps) {
			buffer.putLong(timestamp);
		}
		buffer.flip();
		return buffer;
	}

	public static ByteBuffer getByteBuffer(Object value, DataType dataType) {
		ByteBuffer buffer;

		switch (dataType) {
			case BOOLEAN:
				buffer = ByteBuffer.allocate(1);
				buffer.put(booleanToByte((boolean) value));
				break;
			case INTEGER:
				buffer = ByteBuffer.allocate(4);
				buffer.putInt((int) value);
				break;
			case LONG:
				buffer = ByteBuffer.allocate(8);
				buffer.putLong((long) value);
				break;
			case FLOAT:
				buffer = ByteBuffer.allocate(4);
				buffer.putFloat((float) value);
				break;
			case DOUBLE:
				buffer = ByteBuffer.allocate(8);
				buffer.putDouble((double) value);
				break;
			case STRING:
				buffer = ByteBuffer.allocate(4 + ((String) value).getBytes().length);
				buffer.putInt(((String) value).getBytes().length);
				buffer.put(((String) value).getBytes());
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}

		buffer.flip();
		return buffer;
	}

	public static int getByteBufferSize(Object[] values, DataType dataType) {
		int size = 0;
		switch (dataType) {
			case BOOLEAN:
				size = values.length;
				break;
			case INTEGER:
			case FLOAT:
				size = values.length * 4;
				break;
			case LONG:
			case DOUBLE:
				size = values.length * 8;
				break;
			case STRING:
				size += values.length * 4;
				for (Object value : values) {
					size += ((String) value).getBytes().length;
				}
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}
		return size;
	}

	public static Object getValueByDataType(ByteBuffer buffer, DataType dataType) {
		Object value;
		switch (dataType) {
			case BOOLEAN:
				value = buffer.get() == 1;
				break;
			case INTEGER:
				value = buffer.getInt();
				break;
			case LONG:
				value = buffer.getLong();
				break;
			case FLOAT:
				value = buffer.getFloat();
				break;
			case DOUBLE:
				value = buffer.getDouble();
				break;
			case STRING:
				int length = buffer.getInt();
				byte[] bytes = new byte[length];
				buffer.get(bytes, 0, length);
				value = new String(bytes, 0, length);
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}
		return value;
	}
}
