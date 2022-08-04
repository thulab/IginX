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

    public static List<List<Object>> getValuesFromBufferAndBitmaps(List<DataType> dataTypeList, List<ByteBuffer> valuesList, List<ByteBuffer> bitmapList) {
        List<List<Object>> values = new ArrayList<>();
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
        return values;
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
                case BINARY:
                    int length = valuesList.getInt();
                    byte[] bytes = new byte[length];
                    valuesList.get(bytes, 0, length);
                    values[i] = bytes;
                    break;
                default:
                    throw new UnsupportedDataTypeException(dataTypeList.get(i).toString());
            }
        }
        return values;
    }

    public static Object[] getColumnValuesByDataType(List<ByteBuffer> valuesList, List<DataType> dataTypeList, List<ByteBuffer> bitmapList, int timestampsSize) {
        Object[] tempValues = new Object[valuesList.size()];
        for (int i = 0; i < valuesList.size(); i++) {
            Bitmap bitmap = new Bitmap(timestampsSize, bitmapList.get(i).array());
            int cnt = 0;
            for (int j = 0; j < timestampsSize; j++) {
                if (bitmap.get(j)) {
                    cnt++;
                }
            }
            ByteBuffer buffer = valuesList.get(i);
            Object[] tempColumnValues = new Object[cnt];
            switch (dataTypeList.get(i)) {
                case BOOLEAN:
                    for (int j = 0; j < cnt; j++) {
                        tempColumnValues[j] = buffer.get() == 1;
                    }
                    break;
                case INTEGER:
                    for (int j = 0; j < cnt; j++) {
                        tempColumnValues[j] = buffer.getInt();
                    }
                    break;
                case LONG:
                    for (int j = 0; j < cnt; j++) {
                        tempColumnValues[j] = buffer.getLong();
                    }
                    break;
                case FLOAT:
                    for (int j = 0; j < cnt; j++) {
                        tempColumnValues[j] = buffer.getFloat();
                    }
                    break;
                case DOUBLE:
                    for (int j = 0; j < cnt; j++) {
                        tempColumnValues[j] = buffer.getDouble();
                    }
                    break;
                case BINARY:
                    for (int j = 0; j < cnt; j++) {
                        int length = buffer.getInt();
                        byte[] bytes = new byte[length];
                        buffer.get(bytes, 0, length);
                        tempColumnValues[j] = bytes;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(dataTypeList.get(i).toString());
            }
            tempValues[i] = tempColumnValues;
        }
        return tempValues;
    }

    public static Object[] getRowValuesByDataType(List<ByteBuffer> valuesList, List<DataType> dataTypeList, List<ByteBuffer> bitmapList) {
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
                    case BINARY:
                        int length = valuesList.get(i).getInt();
                        byte[] bytes = new byte[length];
                        valuesList.get(i).get(bytes, 0, length);
                        tempRowValues[j] = bytes;
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

    public static long[] getLongArrayFromByteBuffer(ByteBuffer buffer) {
        long[] array = new long[buffer.array().length / 8];
        for (int i = 0; i < array.length; i++) {
            array[i] = buffer.getLong();
        }
        return array;
    }

    public static long[] getLongArrayFromByteArray(byte[] array) {
        return getLongArrayFromByteBuffer(ByteBuffer.wrap(array));
    }

    public static List<Long> getLongListFromByteBuffer(ByteBuffer buffer) {
        int size = buffer.array().length / 8;
        List<Long> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(buffer.getLong());
        }
        return list;
    }

    public static List<Long> getLongListFromByteArray(byte[] array) {
        return getLongListFromByteBuffer(ByteBuffer.wrap(array));
    }

    public static ByteBuffer getByteBufferFromLongArray(Long[] array) {
        ByteBuffer buffer = ByteBuffer.allocate(8 * array.length);
        for (long value : array) {
            buffer.putLong(value);
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getRowByteBuffer(Object[] values, List<DataType> dataTypes) {
        ByteBuffer buffer = ByteBuffer.allocate(getRowByteBufferSize(values, dataTypes));
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
                case BINARY:
                    buffer.putInt(((byte[]) value).length);
                    buffer.put((byte[]) value);
                    break;
                default:
                    throw new UnsupportedOperationException(dataType.toString());
            }
        }
        buffer.flip();
        return buffer;
    }

    public static ByteBuffer getColumnByteBuffer(Object[] values, DataType dataType) {
        ByteBuffer buffer = ByteBuffer.allocate(getColumnByteBufferSize(values, dataType));
        switch (dataType) {
            case BOOLEAN:
                for (Object value : values) {
                    if (value == null) {
                        continue;
                    }
                    buffer.put(booleanToByte((boolean) value));
                }
                break;
            case INTEGER:
                for (Object value : values) {
                    if (value == null) {
                        continue;
                    }
                    buffer.putInt((int) value);
                }
                break;
            case LONG:
                for (Object value : values) {
                    if (value == null) {
                        continue;
                    }
                    buffer.putLong((long) value);
                }
                break;
            case FLOAT:
                for (Object value : values) {
                    if (value == null) {
                        continue;
                    }
                    buffer.putFloat((float) value);
                }
                break;
            case DOUBLE:
                for (Object value : values) {
                    if (value == null) {
                        continue;
                    }
                    buffer.putDouble((double) value);
                }
                break;
            case BINARY:
                for (Object value : values) {
                    if (value == null) {
                        continue;
                    }
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

    public static int getRowByteBufferSize(Object[] values, List<DataType> dataTypes) {
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
                case BINARY:
                    size += 4 + ((byte[]) value).length;
                    break;
                default:
                    throw new UnsupportedOperationException(dataType.toString());
            }
        }
        return size;
    }

    public static int getColumnByteBufferSize(Object[] values, DataType dataType) {
        int size = 0;
        switch (dataType) {
            case BOOLEAN:
                for (Object value : values) {
                    if (value != null) {
                        size += 1;
                    }
                }
                break;
            case INTEGER:
            case FLOAT:
                for (Object value : values) {
                    if (value != null) {
                        size += 4;
                    }
                }
                break;
            case LONG:
            case DOUBLE:
                for (Object value : values) {
                    if (value != null) {
                        size += 8;
                    }
                }
                break;
            case BINARY:
                for (Object value : values) {
                    if (value != null) {
                        size += 4 + ((byte[]) value).length;
                    }
                }
                break;
            default:
                throw new UnsupportedOperationException(dataType.toString());
        }
        return size;
    }

    public static Object getValueFromByteBufferByDataType(ByteBuffer buffer, DataType dataType) {
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
            case BINARY:
                int length = buffer.getInt();
                byte[] bytes = new byte[length];
                buffer.get(bytes, 0, length);
                value = bytes;
                break;
            default:
                throw new UnsupportedOperationException(dataType.toString());
        }
        return value;
    }


    public static ByteBuffer getByteBufferFromObjectByDataType(Object value, DataType dataType) {
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
            case BINARY:
                buffer = ByteBuffer.allocate(4 + ((byte[]) value).length);
                buffer.putInt(((byte[]) value).length);
                buffer.put(((byte[]) value));
                break;
            default:
                throw new UnsupportedOperationException(dataType.toString());
        }
        buffer.flip();
        return buffer;
    }
}
