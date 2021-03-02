package cn.edu.tsinghua.iginx.utils;

import java.nio.ByteBuffer;
import java.util.List;

public class ByteUtils {

	public static ByteBuffer getByteBuffer(List<Object> values, DataType dataType) {
		ByteBuffer buffer = ByteBuffer.allocate(values.size());

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
					buffer.putInt(((String) value).length());
					buffer.put(((String) value).getBytes());
				}
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}

		buffer.flip();
		return buffer;
	}

	public static ByteBuffer getByteBuffer(Object value, DataType dataType) {
		ByteBuffer buffer = ByteBuffer.allocate(1);

		switch (dataType) {
			case BOOLEAN:
				buffer.put(ByteUtils.booleanToByte((boolean) value));
				break;
			case INT32:
				buffer.putInt((int) value);
				break;
			case INT64:
				buffer.putLong((long) value);
				break;
			case FLOAT:
				buffer.putFloat((float) value);
				break;
			case DOUBLE:
				buffer.putDouble((double) value);
				break;
			case TEXT:
				buffer.putInt(((String) value).length());
				buffer.put(((String) value).getBytes());
				break;
			default:
				throw new UnsupportedOperationException(dataType.toString());
		}

		buffer.flip();
		return buffer;
	}

	public static byte booleanToByte(boolean x) {
		if (x) {
			return 1;
		} else {
			return 0;
		}
	}
}
