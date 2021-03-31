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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.file.metadata.enums.TSDataType.TEXT;

public class IoTDBQueryExecuteDataSet implements QueryExecuteDataSet {

	private final SessionDataSet dataSet;

	private final Session session;

	private final AtomicInteger activeDataSetCount;

	public IoTDBQueryExecuteDataSet(SessionDataSet dataSet, Session session, AtomicInteger activeDataSetCount) {
		this.dataSet = dataSet;
		this.session = session;
		this.activeDataSetCount = activeDataSetCount;
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
			org.apache.iotdb.tsfile.read.common.RowRecord iotdbRowRecord = dataSet.next();
			RowRecord rowRecord = new RowRecord(iotdbRowRecord.getTimestamp());
			List<Object> fields = new ArrayList<>();
			for (Field field : iotdbRowRecord.getFields()) {
				if (field.getDataType() == TEXT) {
					fields.add(field.getBinaryV().getValues());
				} else {
					fields.add(field.getObjectValue(field.getDataType()));
				}
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
			if (activeDataSetCount.decrementAndGet() == 0) {
				session.close();
			}
		} catch (StatementExecutionException | IoTDBConnectionException e) {
			throw new ExecutionException(e.getMessage());
		}
	}
}
