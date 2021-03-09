package cn.edu.tsinghua.session;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IoTDBSessionExample {

	private static Session session;

	private static final String DATABASE_NAME = "root.sg1";
	private static final String COLUMN_D1_S1 = "root.sg1.d1.s1";
	private static final String COLUMN_D1_S2 = "root.sg1.d1.s2";
	private static final String COLUMN_D2_S1 = "root.sg1.d2.s1";
	private static final String COLUMN_D3_S1 = "root.sg1.d3.s1";

	public static void main(String[] args) throws SessionException, ExecutionException {
		ConfigDescriptor.getInstance().getConfig().setLevel(3);

		session = new Session("127.0.0.1", 6324, "root", "root");
		session.openSession();

		session.createDatabase(DATABASE_NAME);

		addColumns();
		insertRecords();
		queryData();
		deleteColumns();

		session.dropDatabase(DATABASE_NAME);

		session.closeSession();
	}

	private static void addColumns() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		Map<String, String> attributesForOnePath = new HashMap<>();
		// INT64
		attributesForOnePath.put("DataType", "2");
		// RLE
		attributesForOnePath.put("Encoding", "2");
		// SNAPPY
		attributesForOnePath.put("Compression", "1");

		List<Map<String, String>> attributes = new ArrayList<>();
		attributes.add(attributesForOnePath);
		attributes.add(attributesForOnePath);
		attributes.add(attributesForOnePath);

		session.addColumns(paths, attributes);
	}

	private static void insertRecords() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		long[] timestamps = new long[100];
		Object[] valuesList = new Object[100];
		for (long i = 0; i < 100; i++) {
			timestamps[(int) i] = i;
			Object[] values = new Object[4];
			for (long j = 0; j < 4; j++) {
				values[(int) j] = i + j;
			}
			valuesList[(int) i] = values;
		}

		List<DataType> dataTypeList = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			dataTypeList.add(DataType.LONG);
		}

		List<Map<String, String>> attributes = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			Map<String, String> attributesForOnePath = new HashMap<>();
			attributesForOnePath.put("DataType", "2");
			attributes.add(attributesForOnePath);
		}

		session.insertRecords(paths, timestamps, valuesList, dataTypeList, attributes);
	}

	private static void queryData() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		long startTime = 5L;
		long endTime = 55L;

		QueryDataSet dataSet = session.queryData(paths, startTime, endTime);
		// TODO
	}

	private static void deleteDataInColumns() throws SessionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		long startTime = 25L;
		long endTime = 30L;

		session.deleteDataInColumns(paths, startTime, endTime);
	}

	private static void deleteColumns() throws SessionException, ExecutionException {
		List<String> paths = new ArrayList<>();
		paths.add(COLUMN_D1_S1);
		paths.add(COLUMN_D1_S2);
		paths.add(COLUMN_D2_S1);
		paths.add(COLUMN_D3_S1);

		session.deleteColumns(paths);
	}
}
