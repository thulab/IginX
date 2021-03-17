package cn.edu.tsinghua.iginx.iotdb.query.entity;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.iotdb.tools.DataTypeTransformer;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class IoTDBQueryExecuteDataSet implements QueryExecuteDataSet {

	private final SessionDataSet dataSet;

	private final Session session;

	public IoTDBQueryExecuteDataSet(SessionDataSet dataSet, Session session) {
		this.dataSet = dataSet;
		this.session = session;
	}

	@Override
	public List<String> getColumnNames() throws ExecutionException {
		return new ArrayList<>(dataSet.getColumnNames());
	}

	@Override
	public List<DataType> getColumnTypes() throws ExecutionException {
		return dataSet.getColumnTypes().stream().map(DataTypeTransformer::fromIoTDB).collect(Collectors.toList());
	}

	@Override
	public boolean hasNext() throws ExecutionException {
		try {
			return dataSet.hasNext();
		} catch (StatementExecutionException | IoTDBConnectionException e) {
			throw new ExecutionException(e.getMessage());
		}
	}

	@Override
	public RowRecord next() throws ExecutionException {
		try {
			RowRecord rowRecord = new RowRecord(dataSet.next().getTimestamp());
			List<Object> fields = new ArrayList<>();
			for (Field field : dataSet.next().getFields()) {
				fields.add(field.getObjectValue(field.getDataType()));
			}
			rowRecord.setFields(fields);
			return rowRecord;
		} catch (StatementExecutionException | IoTDBConnectionException e) {
			throw new ExecutionException(e.getMessage());
		}
	}

	@Override
	public void close() throws ExecutionException {
		try {
			dataSet.closeOperationHandle();
			session.close();
		} catch (StatementExecutionException | IoTDBConnectionException e) {
			throw new ExecutionException(e.getMessage());
		}
	}
}
