package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.Iginx;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class IoTDBSessionExampleITest {

    private static Session session;

    private static final String DATABASE_NAME = "sg1";
    private static final String COLUMN_D1_S1 = "sg1.d1.s1";
    private static final String COLUMN_D1_S2 = "sg1.d1.s2";
    private static final String COLUMN_D2_S1 = "sg1.d2.s1";
    private static final String COLUMN_D3_S1 = "sg1.d3.s1";
    private List<String> paths = new ArrayList<>();

    private static final long timePeriod = 1000L;

    private static final long startTime = 0L;
    private static final long endTime = startTime + timePeriod - 1;


    private static class StartIginX implements Runnable {

        @Override
        public void run()  {
            Iginx iginx = new Iginx();
            String[] args = new String[0];
            Constants.CONFIG_FILE = "../conf/config.properties";
            try {
                iginx.main(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Before
    public void setUp(){
        //Start IginX
        try {
            Thread th = new Thread(new StartIginX());
            th.setDaemon(true);
            th.start();
            Thread.sleep(20000);
            session = new Session("127.0.0.1", 6324, "root", "root");
            session.openSession();
            session.createDatabase(DATABASE_NAME);
            addColumns();
            insertRecords();
            paths.add(COLUMN_D1_S1);
            paths.add(COLUMN_D1_S2);
            paths.add(COLUMN_D2_S1);
            paths.add(COLUMN_D3_S1);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws ExecutionException, SessionException {
        session.dropDatabase(DATABASE_NAME);
        session.closeSession();
    }

    @Test
    public void queryDataTest() throws SessionException {
        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, timePeriod);
        assertEquals(dataSet.getValues().size(), timePeriod);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                switch (resPaths.get(j)){
                    case "root.sg1.d1.s1":
                        assertEquals(result.get(j), (long)i);
                        break;
                    case "root.sg1.d1.s2":
                        assertEquals(result.get(j), (long)(i + 1));
                        break;
                    case "root.sg1.d2.s1":
                        assertEquals(result.get(j), (long)(i + 2));
                        break;
                    case "root.sg1.d3.s1":
                        assertEquals(result.get(j), (long)(i + 3));
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    //TODO make the insert time DISORDER

    //TODO make order and disorder example in the test. Try to construct a function with its value disordered
    @Test
    public void aggrMaxTest() throws SessionException {

        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.MAX);

        int len = maxDataSet.getTimestamps().length;
        List<String> resPaths = maxDataSet.getPaths();
        Object[] result = maxDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(maxDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(maxDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "root.sg1.d1.s1":
                    assertEquals(result[i], endTime);
                    break;
                case "root.sg1.d1.s2":
                    assertEquals(result[i],  endTime + 1);
                    break;
                case "root.sg1.d2.s1":
                    assertEquals(result[i], endTime + 2);
                    break;
                case "root.sg1.d3.s1":
                    assertEquals(result[i], endTime + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void aggrMinTest() throws SessionException {

        SessionAggregateQueryDataSet minDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.MIN);

        int len = minDataSet.getTimestamps().length;
        List<String> resPaths = minDataSet.getPaths();
        Object[] result = minDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(minDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(minDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(result[i], startTime);
                    break;
                case "sg1.d1.s2":
                    assertEquals(result[i],  startTime + 1);
                    break;
                case "sg1.d2.s1":
                    assertEquals(result[i], startTime + 2);
                    break;
                case "sg1.d3.s1":
                    assertEquals(result[i], startTime + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void aggrFirstTest() throws SessionException {

        SessionAggregateQueryDataSet firstDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.FIRST);

        int len = firstDataSet.getTimestamps().length;
        List<String> resPaths = firstDataSet.getPaths();
        Object[] result = firstDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(firstDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(firstDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "root.sg1.d1.s1":
                    assertEquals(result[i], startTime);
                    break;
                case "root.sg1.d1.s2":
                    assertEquals(result[i],  startTime + 1);
                    break;
                case "root.sg1.d2.s1":
                    assertEquals(result[i], startTime + 2);
                    break;
                case "root.sg1.d3.s1":
                    assertEquals(result[i], startTime + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void aggrLastTest() throws SessionException {

        SessionAggregateQueryDataSet lastDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.LAST);

        int len = lastDataSet.getTimestamps().length;
        List<String> resPaths = lastDataSet.getPaths();
        Object[] result = lastDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(lastDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(lastDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "root.sg1.d1.s1":
                    assertEquals(result[i], endTime);
                    break;
                case "root.sg1.d1.s2":
                    assertEquals(result[i],  endTime + 1);
                    break;
                case "root.sg1.d2.s1":
                    assertEquals(result[i], endTime + 2);
                    break;
                case "root.sg1.d3.s1":
                    assertEquals(result[i], endTime + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void aggrCountTest() throws SessionException {

        SessionAggregateQueryDataSet countDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.COUNT);
        assertNull(countDataSet.getTimestamps());
        List<String> resPaths = countDataSet.getPaths();
        Object[] result = countDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(countDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(result[i], timePeriod);
        }
    }

    @Test
    public void aggrSumTest() throws SessionException {

        SessionAggregateQueryDataSet sumDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.SUM);
        sumDataSet.print();
        assertNull(sumDataSet.getTimestamps());
        List<String> resPaths = sumDataSet.getPaths();
        Object[] result = sumDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(sumDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            double sum = (startTime + endTime) * timePeriod / 2.0;
            switch (resPaths.get(i)){
                case "root.sg1.d1.s1":
                    assertEquals(result[i], sum);
                    break;
                case "root.sg1.d1.s2":
                    assertEquals(result[i], sum + timePeriod);
                    break;
                case "root.sg1.d2.s1":
                    assertEquals(result[i], sum + timePeriod * 2);
                    break;
                case "root.sg1.d3.s1":
                    assertEquals(result[i], sum + timePeriod * 3);
                    break;
                default:
                    System.out.println();
                    fail();
                    break;
            }
        }
    }

    @Test
    public void aggrAvgTest() throws SessionException {

        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, startTime, endTime + 1, AggregateType.AVG);

        assertNull(avgDataSet.getTimestamps());
        List<String> resPaths = avgDataSet.getPaths();
        Object[] result = avgDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(avgDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            switch (resPaths.get(i)){
                case "root.sg1.d1.s1":
                    assertEquals(result[i], (startTime + endTime) / 2.0);
                    break;
                case "root.sg1.d1.s2":
                    assertEquals(result[i],  (startTime + endTime) / 2.0 + 1);
                    break;
                case "root.sg1.d2.s1":
                    assertEquals(result[i], (startTime + endTime) / 2.0 + 2);
                    break;
                case "root.sg1.d3.s1":
                    assertEquals(result[i], (startTime + endTime) / 2.0 + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    // TODO combine the delete into different aggregate functions

    @Test
    public void deletePathTest() throws SessionException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D2_S1);
        delPaths.add(COLUMN_D3_S1);

        long delStartTime = 25L;
        long delEndTime = 30L;

        session.deleteDataInColumns(delPaths, delStartTime, delEndTime);

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);

        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(resPaths.size(), 4);
        assertEquals(dataSet.getTimestamps().length, timePeriod);
        assertEquals(dataSet.getValues().size(), timePeriod);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i);
            List<Object> result = dataSet.getValues().get(i);
            if (delStartTime <= i & i <= delEndTime) {
                for (int j = 0; j < 4; j++) {
                    if ("root.sg1.d1.s2".equals(resPaths.get(j))) {
                        assertEquals(result.get(j), (long) (i + 1));
                    } else {
                        assertNull(result.get(j));
                    }
                }
            } else {
                for (int j = 0; j < 4; j++) {
                    switch (resPaths.get(j)) {
                        case "root.sg1.d1.s1":
                            assertEquals(result.get(j), (long) i);
                            break;
                        case "root.sg1.d1.s2":
                            assertEquals(result.get(j), (long) (i + 1));
                            break;
                        case "root.sg1.d2.s1":
                            assertEquals(result.get(j), (long) (i + 2));
                            break;
                        case "root.sg1.d3.s1":
                            assertEquals(result.get(j), (long) (i + 3));
                            break;
                        default:
                            fail();
                            break;
                    }
                }
            }
        }
    }

    @Test
    public void deleteAllColumnTest() throws SessionException, ExecutionException {
        session.deleteColumns(paths);
        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        assertEquals(dataSet.getPaths().size(), 0);
        assertEquals(dataSet.getTimestamps().length, 0);
        assertEquals(dataSet.getValues().size(), 0);
    }

    @Test
    public void deletePartColumnTest() throws SessionException, ExecutionException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D2_S1);
        delPaths.add(COLUMN_D3_S1);
        session.deleteColumns(delPaths);
        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime);
        int len = dataSet.getTimestamps().length;
        assertEquals(dataSet.getPaths().size(), 1);
        assertEquals(dataSet.getPaths().get(0), "root.sg1.d1.s2");
        assertEquals(len, timePeriod);
        assertEquals(dataSet.getValues().size(), timePeriod);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i);
            List<Object> result = dataSet.getValues().get(i);
            assertEquals(result.get(0), (long)(i + 1));
        }
    }

    private static void addColumns() throws SessionException, ExecutionException {
        List<String> addPaths = new ArrayList<>();
        addPaths.add(COLUMN_D1_S1);
        addPaths.add(COLUMN_D1_S2);
        addPaths.add(COLUMN_D2_S1);
        addPaths.add(COLUMN_D3_S1);

        Map<String, String> attributesForOnePath = new HashMap<>();
        // INT64
        attributesForOnePath.put("DataType", "2");
        // RLE
        attributesForOnePath.put("Encoding", "2");
        // SNAPPY
        attributesForOnePath.put("Compression", "1");

        List<Map<String, String>> attributes = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            attributes.add(attributesForOnePath);
        }

        session.addColumns(addPaths, attributes);
    }

    private static void insertRecords() throws SessionException, ExecutionException {
        List<String> insertPaths = new ArrayList<>();
        insertPaths.add(COLUMN_D1_S1);
        insertPaths.add(COLUMN_D1_S2);
        insertPaths.add(COLUMN_D2_S1);
        insertPaths.add(COLUMN_D3_S1);

        long[] timestamps = new long[(int) timePeriod];
        for (long i = 0; i < timePeriod; i++) {
            timestamps[(int) i] = i + startTime;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[(int) timePeriod];
            for (long j = 0; j < timePeriod; j++) {
                values[(int) j] = i + j + startTime;
            }
            valuesList[(int) i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            dataTypeList.add(DataType.LONG);
        }

        session.insertColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

}
