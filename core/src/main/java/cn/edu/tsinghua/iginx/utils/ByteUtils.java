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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.utils.DataType.deserialize;

public class ByteUtils {

	public static ByteBuffer getByteBuffer(List<Object> values, DataType dataType) {
		ByteBuffer buffer = ByteBuffer.allocate(getByteBufferSize(values, dataType));

		switch (dataType) {
			case BOOLEAN:
				for (Object value : values) {
					buffer.put(ByteUtils.booleanToByte((boolean) value));
				}
				break;
			case INT32:
				for (Object value : values) {
					buffer.putInt((int) value);
				}
				break;
			case INT64:
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
			case TEXT:
				for (Object value : values) {
					buffer.putInt(((byte[]) value).length);
					buffer.put((byte[]) value);
				}
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}

		buffer.flip();
		return buffer;
	}

	public static ByteBuffer getByteBuffer(Object value, DataType dataType) {
		ByteBuffer buffer;

		switch (dataType) {
			case BOOLEAN:
				buffer = ByteBuffer.allocate(1);
				buffer.put(ByteUtils.booleanToByte((boolean) value));
				break;
			case INT32:
				buffer = ByteBuffer.allocate(4);
				buffer.putInt((int) value);
				break;
			case INT64:
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
			case TEXT:
				buffer = ByteBuffer.allocate(4 + ((byte[]) value).length);
				buffer.putInt(((byte[]) value).length);
				buffer.put((byte[]) value);
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}

		buffer.flip();
		return buffer;
	}

	public static boolean getBoolean(ByteBuffer buffer) {
		return buffer.get() == 1;
	}

	public static int getInteger(ByteBuffer buffer) {
		return buffer.getInt();
	}

	public static long getLong(ByteBuffer buffer) {
		return buffer.getLong();
	}

	public static float getFloat(ByteBuffer buffer) {
		return buffer.getFloat();
	}

	public static double getDouble(ByteBuffer buffer) {
		return buffer.getDouble();
	}

	public static String getString(ByteBuffer buffer) {
		int length = getInteger(buffer);
		if (length < 0) {
			return null;
		} else if (length == 0) {
			return "";
		}
		byte[] bytes = new byte[length];
		buffer.get(bytes, 0, length);
		return new String(bytes, 0, length);
	}

	public static byte booleanToByte(boolean x) {
		if (x) {
			return 1;
		} else {
			return 0;
		}
	}

	public static Object[] getValuesByDataType(List<ByteBuffer> values, List<Map<String, String>> attributes) {
		Object[] tempValues = new Object[values.size()];
		for (int i = 0; i < attributes.size(); i++) {
			DataType dataType = deserialize(Short.parseShort(attributes.get(i).get("DataType")));
			switch (dataType) {
				case BOOLEAN:
					tempValues[i] = getBoolean(values.get(i));
					break;
				case INT32:
					tempValues[i] = getInteger(values.get(i));
					break;
				case INT64:
					tempValues[i] = getLong(values.get(i));
					break;
				case FLOAT:
					tempValues[i] = getFloat(values.get(i));
					break;
				case DOUBLE:
					tempValues[i] = getDouble(values.get(i));
					break;
				case TEXT:
					tempValues[i] = getString(values.get(i));
					break;
				default:
					throw new UnsupportedOperationException(dataType.toString());
			}
		}
		return tempValues;
	}

	public static int getByteBufferSize(List<Object> values, DataType dataType) {
		int size = 0;
		switch (dataType) {
			case BOOLEAN:
				size = values.size();
				break;
			case INT32:
			case FLOAT:
				size = values.size() * 4;
				break;
			case INT64:
			case DOUBLE:
				size = values.size() * 8;
				break;
			case TEXT:
				size += values.size() * 4;
				for (Object value : values) {
					size += ((byte[]) value).length;
				}
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}
		return size;
	}
}
