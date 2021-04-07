package cn.edu.tsinghua.iginx.influxdb.query.entity;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;
import com.influxdb.query.FluxTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.influxdb.tools.DataTypeTransformer.fromInfluxDB;
import static cn.edu.tsinghua.iginx.thrift.DataType.BINARY;
import static cn.edu.tsinghua.iginx.thrift.DataType.LONG;

public class InfluxDBQueryExecuteDataSet implements QueryExecuteDataSet  {

	private String bucketName;

	private FluxTable table;

	private int index;

	public InfluxDBQueryExecuteDataSet(String bucketName, FluxTable table) {
		this.bucketName = bucketName;
		this.table = table;
		this.index = 0;
	}

	@Override
	public List<String> getColumnNames() throws ExecutionException {
		List<String> columnNames = new ArrayList<>();
		columnNames.add("Time");
		columnNames.add(bucketName +
				"." +
				table.getRecords().get(0).getMeasurement() +
				"." +
				table.getRecords().get(0).getValueByKey("tag") +
				"." +
				table.getRecords().get(0).getField());
		return columnNames;
	}

	@Override
	public List<DataType> getColumnTypes() throws ExecutionException {
		List<DataType> columnTypes = new ArrayList<>();
		columnTypes.add(LONG);
		columnTypes.add(fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()));
		return columnTypes;
	}

	@Override
	public boolean hasNext() throws ExecutionException {
		return index < table.getRecords().size();
	}

	@Override
	public RowRecord next() throws ExecutionException {
		RowRecord rowRecord = new RowRecord(table.getRecords().get(index).getTime().toEpochMilli());
		Object value = table.getRecords().get(index).getValue();
		rowRecord.setFields(Collections.singletonList(fromInfluxDB(table.getColumns().stream().filter(x -> x.getLabel().equals("_value")).collect(Collectors.toList()).get(0).getDataType()) == BINARY ? ((String) value).getBytes() : value));
		index++;
		return rowRecord;
	}

	@Override
	public void close() throws ExecutionException {
		// TODO do nothing
	}
}
