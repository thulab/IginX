package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.UnsupportedDataTypeException;
import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.nio.ByteBuffer;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;

public class SessionAggregateQueryDataSet {

	private List<String> paths;

	private long[] timestamps;

	private Object[] values;

	public SessionAggregateQueryDataSet(AggregateQueryResp resp) {
		this.paths = resp.getPaths();
		if (resp.timestamps != null) {
			this.timestamps = getLongArrayFromByteBuffer(resp.timestamps);
		}
		parseValues(resp.dataTypeList, resp.valuesList);
	}

	private void parseValues(List<DataType> dataTypeList, ByteBuffer valuesList) {
		values = new Object[paths.size()];
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
					break;
				default:
					throw new UnsupportedDataTypeException(dataTypeList.get(i).toString());
			}
		}
	}

	public List<String> getPaths() {
		return paths;
	}

	public void setPaths(List<String> paths) {
		this.paths = paths;
	}

	public long[] getTimestamps() {
		return timestamps;
	}

	public Object[] getValues() {
		return values;
	}

	public void print() {
		System.out.println("start to print dataset:");
		if (timestamps != null) {
			System.out.print("Time\t");
		}
		for (String path : paths) {
			System.out.print(path + "\t");
		}
		System.out.println();

		if (timestamps != null) {
			for (long timestamp : timestamps) {
				System.out.print(timestamp + "\t");
			}
			System.out.println();
		}

		for (Object value : values) {
			System.out.print(value + "\t");
		}
		System.out.println();
		System.out.println("print dataset finished.");
	}
}
