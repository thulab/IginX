package cn.edu.tsinghua.iginx.exceptions;

public class UnsupportedDataTypeException extends RuntimeException {

	private static final long serialVersionUID = 5278528888805786089L;

	public UnsupportedDataTypeException(String dataTypeName) {
		super("Unsupported DataType: " + dataTypeName);
	}
}
