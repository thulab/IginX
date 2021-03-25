package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

public class InfluxDBSessionExample {

	private static Session session;

	private static final String ORGANIZATION_NAME = "my-second-org";
	private static final String BUCKET_NAME = "my-second-bucket";

	// measurement tag(key-value) field
	private static final String TS1 = "census.location-klamath.scientist-anderson.ants";
	private static final String TS2 = "census.location-klamath.scientist-anderson.bees";
	private static final String TS3 = "census.location-portland.scientist-mullen.ants";
	private static final String TS4 = "census.location-portland.scientist-mullen.bees";

	public static void main(String[] args) throws SessionException, ExecutionException, TTransportException {
		session = new Session("127.0.0.1", 6324, "root", "root");
		session.openSession();

//		createDatabase();

		insertRecords();
		queryData();
//		aggregateQuery();
//		deleteDataInColumns();
//		queryData();

//		dropDatabase();

		session.closeSession();
	}

	private static void createDatabase() throws SessionException, ExecutionException {
		session.createDatabase(ORGANIZATION_NAME + "$" + BUCKET_NAME);
	}

	private static void dropDatabase() throws SessionException, ExecutionException {
		session.dropDatabase(ORGANIZATION_NAME + "$" + BUCKET_NAME);
	}

	private static void insertRecords() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);
		paths.add(TS4);

		long[] timestamps = new long[100];
		for (long i = 0; i < 100; i++) {
			timestamps[(int) i] = i;
		}

		Object[] valuesList = new Object[4];
		for (long i = 0; i < 4; i++) {
			Object[] values = new Object[100];
			for (long j = 0; j < 100; j++) {
				values[(int) j] = i + j;
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			dataTypeList.add(DataType.LONG);
		}

		session.insertRecords(paths, timestamps, valuesList, dataTypeList, null);
	}

	private static void queryData() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);
		paths.add(TS4);

		long startTime = 0L;
		long endTime = 55L;

		SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
		dataSet.print();
	}

	private static void deleteDataInColumns() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);

		long startTime = 25L;
		long endTime = 30L;

		session.deleteDataInColumns(paths, startTime, endTime);
	}

	private static void aggregateQuery() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(TS1);
		paths.add(TS2);
		paths.add(TS3);
		paths.add(TS4);

		long startTime = 5L;
		long endTime = 55L;

		SessionAggregateQueryDataSet dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MAX);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.MIN);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.FIRST);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.LAST);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.COUNT);
		dataSet.print();

		dataSet = session.aggregateQuery(paths, startTime, endTime, AggregateType.SUM);
		dataSet.print();
	}
}
