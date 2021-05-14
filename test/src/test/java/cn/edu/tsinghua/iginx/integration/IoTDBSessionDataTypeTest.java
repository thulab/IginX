package cn.edu.tsinghua.iginx.integration;

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
import org.stringtemplate.v4.ST;

import java.util.*;

import static org.junit.Assert.*;

public class IoTDBSessionDataTypeTest {

    private static Session session;

    private static final String DATABASE_NAME = "sg1";
    private static final String COLUMN_D1_S1 = "sg1.d1.s1";
    private static final String COLUMN_D2_S2 = "sg1.d2.s2";
    private static final String COLUMN_D3_S3 = "sg1.d3.s3";
    private static final String COLUMN_D4_S4 = "sg1.d4.s4";
    private static final String COLUMN_D5_S5= "sg1.d5.s5";
    private static final String COLUMN_D0_S0= "sg1.d0.s0";

    private List<String> paths = new ArrayList<>();

    private static final long TIME_PERIOD = 100000L;

    private static final long START_TIME = 0L;
    private static final long END_TIME = START_TIME + TIME_PERIOD - 1;

    private static final int STRING_LEN = 1000;
    private static final String ranStr = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    private static final double delta = 1e-6;

    @Before
    public void setUp(){
        /*
    一共包括上述的6种数据类型
    BOOLEAN(0),
    INTEGER(1),
    LONG(2),
    FLOAT(3),
    DOUBLE(4),
    BINARY(5);*/

        try {
            paths.add(COLUMN_D0_S0);
            paths.add(COLUMN_D1_S1);
            paths.add(COLUMN_D2_S2);
            paths.add(COLUMN_D3_S3);
            paths.add(COLUMN_D4_S4);
            paths.add(COLUMN_D5_S5);
            session = new Session("127.0.0.1", 6324, "root", "root");
            session.openSession();
            session.createDatabase(DATABASE_NAME);
            addColumns();
            insertRecords();
            //TODO remove this line when the new iotdb release version fix this bug
            Thread.sleep(10000);
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
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(resPaths.size(), 6);
        assertEquals(len, TIME_PERIOD);
        assertEquals(dataSet.getValues().size(), TIME_PERIOD);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 6; j++) {
                switch (resPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(result.get(j), (int)((END_TIME - i) + 1 + START_TIME));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(result.get(j), (i + 2 + START_TIME) * 1000);
                        break;
                    case "sg1.d3.s3":
                        assertEquals((float)result.get(j), (float)(i + 3 + START_TIME + 0.01), (float)delta);
                        break;
                    case "sg1.d4.s4":
                        assertEquals((double)result.get(j), ((END_TIME - i) + 4 + START_TIME + 0.01) * 999, delta);
                        break;
                    case "sg1.d5.s5":
                        assertArrayEquals((byte[])(result.get(j)), getRandomStr(i, STRING_LEN).getBytes());
                        break;
                    case "sg1.d0.s0":
                        assertEquals(result.get(j), i % 2 == 0);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void aggregateTest() throws SessionException {
        //Test aggrgate functions:  max avg
        List<String> aggrPaths = new ArrayList<>();
        aggrPaths.add(COLUMN_D1_S1);
        aggrPaths.add(COLUMN_D3_S3);
        aggrPaths.add(COLUMN_D2_S2);
        aggrPaths.add(COLUMN_D4_S4);

        // Test max function for the delete
        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(aggrPaths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> maxResPaths = maxDataSet.getPaths();
        Object[] maxResult = maxDataSet.getValues();
        assertEquals(maxResPaths.size(), aggrPaths.size());
        assertEquals(maxDataSet.getValues().length, aggrPaths.size());
        for(int i = 0; i < 4; i++) {
            switch (maxResPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(maxResult[i], (int)(END_TIME + 1));
                    break;
                case "sg1.d2.s2":
                    assertEquals(maxResult[i], (END_TIME + 2) * 1000);
                    break;
                case "sg1.d3.s3":
                    assertEquals((float)maxResult[i], (float)(END_TIME + 3 + 0.01), delta);
                    break;
                case "sg1.d4.s4":
                    assertEquals((double)maxResult[i], (END_TIME + 4 + 0.01) * 999, delta);
                    break;
                default:
                    fail();
                    break;
            }
        }

        // Test avg function for the delete
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(aggrPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(avgResPaths.size(), aggrPaths.size());
        assertEquals(avgDataSet.getValues().length, aggrPaths.size());
        for(int i = 0; i < 4; i++) {
            switch (avgResPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals((double)avgResult[i], (START_TIME + END_TIME) / 2.0 + 1, delta);
                    break;
                case "sg1.d2.s2":
                    assertEquals(avgResult[i], (START_TIME + END_TIME) * 500.0 + 2000);
                    break;
                case "sg1.d3.s3":
                    assertEquals((double)avgResult[i], (START_TIME + END_TIME) / 2.0 + 3 + 0.01, delta * 1000);
                    break;
                case "sg1.d4.s4":
                    assertEquals((double)avgResult[i], (START_TIME + END_TIME) * 999 / 2.0 + 4.01 * 999, delta * 1000);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    @Test
    public void deletePartDataTest() throws SessionException, InterruptedException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D3_S3);
        delPaths.add(COLUMN_D5_S5);

        // ensure after delete there are still points in the timeseries
        long delStartTime = START_TIME + TIME_PERIOD / 5;
        long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
        long delTimePeriod = delEndTime - delStartTime + 1;



        session.deleteDataInColumns(delPaths, delStartTime, delEndTime);

        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);

        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(dataSet.getTimestamps().length, TIME_PERIOD);
        assertEquals(dataSet.getValues().size(), TIME_PERIOD);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i + START_TIME);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 6; j++) {
                switch(resPaths.get(j)) {
                    case "sg1.d0.s0":
                        assertEquals(result.get(j), timestamp % 2 == 0);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(result.get(j), (timestamp + 2) * 1000);
                        break;
                    case "sg1.d4.s4":
                        assertEquals((double) result.get(j), (4 + (END_TIME - timestamp) + 0.01) * 999, delta);
                        break;
                    case "sg1.d1.s1":
                        if (delStartTime <= timestamp && timestamp <= delEndTime) {
                            assertNull(result.get(j));
                        } else {
                            assertEquals(result.get(j), (int) ((END_TIME - i) + 1 + START_TIME));
                        }
                        break;
                    case "sg1.d3.s3":
                        if (delStartTime <= timestamp && timestamp <= delEndTime) {
                            assertNull(result.get(j));
                        } else {
                            assertEquals((float) result.get(j), (float) (i + 3 + START_TIME + 0.01), (float) delta);
                        }
                        break;
                    case "sg1.d5.s5":
                        if (delStartTime <= timestamp && timestamp <= delEndTime) {
                            assertNull(result.get(j));
                        } else {
                            assertArrayEquals((byte[])(result.get(j)), getRandomStr(i, STRING_LEN).getBytes());
                        }
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }


        List<String> aggrPaths = new ArrayList<>();
        aggrPaths.add(COLUMN_D1_S1);
        aggrPaths.add(COLUMN_D2_S2);
        aggrPaths.add(COLUMN_D3_S3);
        aggrPaths.add(COLUMN_D4_S4);

        // Test aggregate function for the delete
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(aggrPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(avgResPaths.size(), aggrPaths.size());
        assertEquals(avgDataSet.getValues().length, aggrPaths.size());
        for(int i = 0; i < 4; i++) {
            switch (avgResPaths.get(i)){
                case "sg1.d2.s2":
                    assertEquals(avgResult[i], (START_TIME + END_TIME) * 500.0 + 2000);
                    break;
                case "sg1.d4.s4":
                    assertEquals((double)avgResult[i], (START_TIME + END_TIME) * 999 / 2.0 + 4.01 * 999, delta * 1000);
                    break;
                case "sg1.d1.s1":
                    assertEquals((double)avgResult[i], ((START_TIME + END_TIME) * TIME_PERIOD / 2.0 -
                            (END_TIME - delStartTime + END_TIME - delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod) + 1.0, delta * 1000);
                    break;
                case "sg1.d3.s3":
                    assertEquals((double)avgResult[i], ((START_TIME + END_TIME) * TIME_PERIOD / 2.0 -
                            (delStartTime + delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod) + 3.01, delta * 1000);
                    break;
                default:
                    fail();
                    break;
            }
        }
    }


    @Test
    public void deleteAllDataInColumnTest() throws SessionException, InterruptedException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D3_S3);
        delPaths.add(COLUMN_D5_S5);

        session.deleteDataInColumns(delPaths, START_TIME, END_TIME);
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(dataSet.getTimestamps().length, TIME_PERIOD);
        assertEquals(dataSet.getValues().size(), TIME_PERIOD);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i + START_TIME);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 6; j++) {
                switch(resPaths.get(j)) {
                    case "sg1.d0.s0":
                        assertEquals(result.get(j), timestamp % 2 == 0);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(result.get(j),(timestamp + 2) * 1000);
                        break;
                    case "sg1.d4.s4":
                        assertEquals((double)result.get(j),(4 + (END_TIME - timestamp) + 0.01) * 999, delta);
                        break;
                    case "sg1.d1.s1":
                    case "sg1.d3.s3":
                    case "sg1.d5.s5":
                        assertNull(result.get(j));
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }

        List<String> aggrPaths = new ArrayList<>();
        aggrPaths.add(COLUMN_D1_S1);
        aggrPaths.add(COLUMN_D2_S2);
        aggrPaths.add(COLUMN_D3_S3);
        aggrPaths.add(COLUMN_D4_S4);

        // Test aggregate function for the delete
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(aggrPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(avgResPaths.size(), aggrPaths.size());
        assertEquals(avgDataSet.getValues().length, aggrPaths.size());
        for(int i = 0; i < 4; i++) {
            switch (avgResPaths.get(i)){
                case "sg1.d2.s2":
                    assertEquals(avgResult[i], (START_TIME + END_TIME) * 500.0 + 2000);
                    break;
                case "sg1.d4.s4":
                    assertEquals((double)avgResult[i], (START_TIME + END_TIME) * 999 / 2.0 + 4.01 * 999, delta * 1000);
                    break;
                case "sg1.d1.s1":
                case "sg1.d3.s3":
                    assertEquals(new String((byte[]) avgResult[i]), "null");
                    break;
                default:
                    fail();
                    break;
            }
        }
    }

    private static void addColumns() throws SessionException, ExecutionException {
        List<String> addPaths = new ArrayList<>();
        addPaths.add(COLUMN_D0_S0);
        addPaths.add(COLUMN_D1_S1);
        addPaths.add(COLUMN_D2_S2);
        addPaths.add(COLUMN_D3_S3);
        addPaths.add(COLUMN_D4_S4);
        addPaths.add(COLUMN_D5_S5);

        List<Map<String, String>> attributes = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            Map<String, String> attributesForOnePath = new HashMap<>();
            attributesForOnePath.put("DataType", String.valueOf(i));
            if(i != 5) {
                attributesForOnePath.put("Encoding", "2");
            } else {
                attributesForOnePath.put("Encoding", "0");
            }
            attributesForOnePath.put("Compression", "1");
            attributes.add(attributesForOnePath);
        }
        session.addColumns(addPaths, attributes);
    }

    private static void insertRecords() throws SessionException, ExecutionException {
        List<String> insertPaths = new ArrayList<>();
        insertPaths.add(COLUMN_D0_S0);
        insertPaths.add(COLUMN_D1_S1);
        insertPaths.add(COLUMN_D2_S2);
        insertPaths.add(COLUMN_D3_S3);
        insertPaths.add(COLUMN_D4_S4);
        insertPaths.add(COLUMN_D5_S5);

        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME;
        }

        Object[] valuesList = new Object[6];
        for (int i = 0; i < 6; i++) {
            Object[] values = new Object[(int) TIME_PERIOD];
            for (int j = 0; j < TIME_PERIOD; j++) {
                switch (i){
                    case 1:
                        //integer
                        values[j] = (int)(i + (TIME_PERIOD - j - 1) + START_TIME);
                        break;
                    case 2:
                        //long
                        values[j] = (i + j + START_TIME) * 1000;
                        break;
                    case 3:
                        values[j] = (float)(i + j + START_TIME + 0.01);
                        //float
                        break;
                    case 4:
                        //double
                        values[j] = (i + (TIME_PERIOD - j - 1) + START_TIME + 0.01) * 999;
                        break;
                    case 5:
                        //binary
                        values[j] = getRandomStr(j, STRING_LEN).getBytes();
                        break;
                    default:
                        //boolean
                        values[j] = (j % 2 == 0);
                        break;
                }
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < 6; i++){
            dataTypeList.add(DataType.findByValue(i));
        }
        session.insertColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    private static String getRandomStr(int seed, int length) {
        Random random = new Random(seed);
        StringBuilder sb = new StringBuilder();
        for(int k = 0; k < length; k++){
            int number = random.nextInt(ranStr.length());
            sb.append(ranStr.charAt(number));
        }
        return sb.toString();
    }

}
