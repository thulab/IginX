package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.utils.Bitmap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueByDataType;

public class SessionQueryDataSet {

	private List<String> paths;

	private long[] timestamps;

	private List<List<Object>> values;

	public SessionQueryDataSet(QueryDataResp resp) {
		this.paths = resp.getPaths();
		this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
		parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
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
					tempValues.add(getValueByDataType(valuesBuffer, dataTypeList.get(j)));
				} else{
					tempValues.add(null);
				}
			}
			values.add(tempValues);
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

	public List<List<Object>> getValues() {
		return values;
	}

	public void print() {
		System.out.print("\t");
		for (String path : paths) {
			System.out.print(path + "\t");
		}

		for (int i = 0; i < timestamps.length; i++) {
			System.out.print(timestamps[i] + "\t");
			for (int j = 0; j < paths.size(); j++) {
				System.out.print(values.get(i).get(j) + "\t");
			}
		}
	}
}
