package cn.edu.tsinghua.iginx.influxdb.query.entity;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBQueryExecuteDataSet implements QueryExecuteDataSet  {

	private List<FluxTable> tableList;

	public InfluxDBQueryExecuteDataSet(List<FluxTable> tableList) {
		this.tableList = tableList;
	}

	@Override
	public List<String> getColumnNames() throws ExecutionException {
		List<String> columns = new ArrayList<>();
		for (FluxTable table : tableList) {
			StringBuilder path = new StringBuilder();
			String measurement;
			String field;
			Map<String, String> tags = new HashMap<>();
			// TODO
		}
		return null;
	}

	@Override
	public List<DataType> getColumnTypes() throws ExecutionException {
		return null;
	}

	@Override
	public boolean hasNext() throws ExecutionException {
		return false;
	}

	@Override
	public RowRecord next() throws ExecutionException {
		return null;
	}

	@Override
	public void close() throws ExecutionException {

	}
}
