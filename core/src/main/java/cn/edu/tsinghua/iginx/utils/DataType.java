package cn.edu.tsinghua.iginx.utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum DataType {
	BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT;

	public static DataType deserialize(short type) {
		return getTsDataType(type);
	}

	private static DataType getTsDataType(short type) {
		if (type >= 6 || type < 0) {
			throw new IllegalArgumentException("Invalid input: " + type);
		}
		switch (type) {
			case 0:
				return BOOLEAN;
			case 1:
				return INT32;
			case 2:
				return INT64;
			case 3:
				return FLOAT;
			case 4:
				return DOUBLE;
			default:
				return TEXT;
		}
	}

	public static byte deserializeToByte(short type) {
		if (type >= 6 || type < 0) {
			throw new IllegalArgumentException("Invalid input: " + type);
		}
		return (byte) type;
	}

	public static DataType byteToEnum(byte type) {
		return getTsDataType(type);
	}

	public static DataType deserializeFrom(ByteBuffer buffer) {
		return deserialize(buffer.getShort());
	}

	public static int getSerializedSize() {
		return Short.BYTES;
	}

	public void serializeTo(ByteBuffer byteBuffer) {
		byteBuffer.putShort(serialize());
	}

	public void serializeTo(DataOutputStream outputStream) throws IOException {
		outputStream.writeShort(serialize());
	}

	public short serialize() {
		return enumToByte();
	}

	public int getDataTypeSize() {
		switch (this) {
			case BOOLEAN:
				return 1;
			case INT32:
			case FLOAT:
				return 4;
			// For text: return the size of reference here
			case TEXT:
			case INT64:
			case DOUBLE:
				return 8;
			default:
				throw new UnsupportedOperationException(this.toString());
		}
	}

	public byte enumToByte() {
		switch (this) {
			case BOOLEAN:
				return 0;
			case INT32:
				return 1;
			case INT64:
				return 2;
			case FLOAT:
				return 3;
			case DOUBLE:
				return 4;
			case TEXT:
				return 5;
			default:
				return -1;
		}
	}
}
