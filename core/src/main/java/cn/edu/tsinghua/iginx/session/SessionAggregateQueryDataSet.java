package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.AggregateQueryResp;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

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
		values = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
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
