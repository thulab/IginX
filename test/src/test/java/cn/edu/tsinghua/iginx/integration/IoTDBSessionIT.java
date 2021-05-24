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

public class IoTDBSessionIT {

    private static Session session;

    private static final String DATABASE_NAME = "sg1";
    private static final String COLUMN_D1_S1 = "sg1.d1.s1";
    private static final String COLUMN_D2_S2 = "sg1.d2.s2";
    private static final String COLUMN_D3_S3 = "sg1.d3.s3";
    private static final String COLUMN_D4_S4 = "sg1.d4.s4";
    private List<String> paths = new ArrayList<>();

    private static final long TIME_PERIOD = 100000L;
    private static final long START_TIME = 1000L;
    private static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    private static final long PRECISION = 123L;
    private static final double delta = 1e-7;

    @Before
    public void setUp(){
        try {
            paths.add(COLUMN_D1_S1);
            paths.add(COLUMN_D2_S2);
            paths.add(COLUMN_D3_S3);
            paths.add(COLUMN_D4_S4);
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
    public void queryDataTest() throws SessionException, ExecutionException {
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, TIME_PERIOD);
        assertEquals(dataSet.getValues().size(), TIME_PERIOD);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i + START_TIME);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                switch (resPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(result.get(j), timestamp);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(result.get(j), timestamp + 1);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(result.get(j), timestamp + 2);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(result.get(j), timestamp + 3);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void valueFilterQueryTest() throws SessionException, ExecutionException {
        long s1 = 9900;
        long s2 = 90000;
        long s3 = 40050;
        long s4 = 40001;

        paths.add(COLUMN_D1_S1);
        paths.add(COLUMN_D2_S2);
        paths.add(COLUMN_D3_S3);
        paths.add(COLUMN_D4_S4);

        String booleanExpression = COLUMN_D1_S1 + ">" +  s1 + " and " + COLUMN_D2_S2 + "<" + s2 + " and(" +
                COLUMN_D3_S3 + " > " + s3 + " or " + COLUMN_D4_S4 + " < " + s4 +")";
        SessionQueryDataSet dataSet = session.valueFilterQuery(paths, START_TIME, END_TIME, booleanExpression);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, TIME_PERIOD - (s1 - START_TIME + 1) - (END_TIME - s2 + 2) - (s3 - s4 + 2));
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                switch (resPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(result.get(j), timestamp);
                        assertTrue(timestamp > s1);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(result.get(j), timestamp + 1);
                        assertTrue(timestamp + 1 < s2);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(result.get(j), timestamp + 2);
                        assertTrue(timestamp + 2 > s3 || timestamp + 3 < s4);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(result.get(j), timestamp + 3);
                        assertTrue(timestamp + 2 > s3 || timestamp + 3 < s4);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }

    }

    @Test
    public void aggrMaxTest() throws SessionException, ExecutionException {

        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX);

        int len = maxDataSet.getTimestamps().length;
        List<String> resPaths = maxDataSet.getPaths();
        Object[] result = maxDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(maxDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(maxDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(result[i], END_TIME);
                    break;
                case "sg1.d2.s2":
                    assertEquals(result[i],  END_TIME + 1);
                    break;
                case "sg1.d3.s3":
                    assertEquals(result[i], END_TIME + 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(result[i], END_TIME + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                switch (dsResPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(dsResult.get(j), maxNum);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(dsResult.get(j), maxNum + 1);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(dsResult.get(j), maxNum + 2);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(dsResult.get(j), maxNum + 3);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void aggrMinTest() throws SessionException, ExecutionException {

        SessionAggregateQueryDataSet minDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN);

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
                    assertEquals(result[i], START_TIME);
                    break;
                case "sg1.d2.s2":
                    assertEquals(result[i],  START_TIME + 1);
                    break;
                case "sg1.d3.s3":
                    assertEquals(result[i], START_TIME + 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(result[i], START_TIME + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long minNum = START_TIME + i * PRECISION;
                switch (dsResPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(dsResult.get(j), minNum);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(dsResult.get(j), minNum + 1);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(dsResult.get(j), minNum + 2);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(dsResult.get(j), minNum + 3);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void aggrFirstTest() throws SessionException, ExecutionException {

        SessionAggregateQueryDataSet firstDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST);

        int len = firstDataSet.getTimestamps().length;
        List<String> resPaths = firstDataSet.getPaths();
        Object[] result = firstDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(firstDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(firstDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(result[i], START_TIME);
                    break;
                case "sg1.d2.s2":
                    assertEquals(result[i],  START_TIME + 1);
                    break;
                case "sg1.d3.s3":
                    assertEquals(result[i], START_TIME + 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(result[i], START_TIME + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long firstNum = START_TIME + i * PRECISION;
                switch (dsResPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(dsResult.get(j), firstNum);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(dsResult.get(j), firstNum + 1);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(dsResult.get(j), firstNum + 2);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(dsResult.get(j), firstNum + 3);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void aggrLastTest() throws SessionException, ExecutionException {

        SessionAggregateQueryDataSet lastDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST);

        int len = lastDataSet.getTimestamps().length;
        List<String> resPaths = lastDataSet.getPaths();
        Object[] result = lastDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(len, 4);
        assertEquals(lastDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(lastDataSet.getTimestamps()[i], -1);
            switch (resPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(result[i], END_TIME);
                    break;
                case "sg1.d2.s2":
                    assertEquals(result[i],  END_TIME + 1);
                    break;
                case "sg1.d3.s3":
                    assertEquals(result[i], END_TIME + 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(result[i], END_TIME + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long lastNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                switch (dsResPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(dsResult.get(j), lastNum);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(dsResult.get(j), lastNum + 1);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(dsResult.get(j), lastNum + 2);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(dsResult.get(j), lastNum + 3);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void aggrCountTest() throws SessionException, ExecutionException {

        SessionAggregateQueryDataSet countDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT);
        assertNull(countDataSet.getTimestamps());
        List<String> resPaths = countDataSet.getPaths();
        Object[] result = countDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(countDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            assertEquals(result[i], TIME_PERIOD);
        }
        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long countNum = ((START_TIME + (i + 1) * PRECISION - 1) <= END_TIME) ? PRECISION : END_TIME - dsTimestamp + 1;
                assertEquals(dsResult.get(j), countNum);
            }
        }
    }

    @Test
    public void aggrSumTest() throws SessionException, ExecutionException {
        SessionAggregateQueryDataSet sumDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM);
        assertNull(sumDataSet.getTimestamps());
        List<String> resPaths = sumDataSet.getPaths();
        Object[] result = sumDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(sumDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            double sum = (START_TIME + END_TIME) * TIME_PERIOD / 2.0;
            switch (resPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(result[i], sum);
                    break;
                case "sg1.d2.s2":
                    assertEquals(result[i], sum + TIME_PERIOD);
                    break;
                case "sg1.d3.s3":
                    assertEquals(result[i], sum + TIME_PERIOD * 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(result[i], sum + TIME_PERIOD * 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double sum = (dsTimestamp + maxNum) * (maxNum - dsTimestamp + 1) / 2.0;
                switch (dsResPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals(dsResult.get(j), sum);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(dsResult.get(j), sum + (maxNum - dsTimestamp + 1));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(dsResult.get(j), sum + 2 * (maxNum - dsTimestamp + 1));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(dsResult.get(j), sum + 3 * (maxNum - dsTimestamp + 1));
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void aggrAvgTest() throws SessionException, ExecutionException {

        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);

        assertNull(avgDataSet.getTimestamps());
        List<String> resPaths = avgDataSet.getPaths();
        Object[] result = avgDataSet.getValues();
        assertEquals(resPaths.size(), 4);
        assertEquals(avgDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            double avg = (START_TIME + END_TIME) / 2.0;
            switch (resPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals((double)result[i], avg, delta);
                    break;
                case "sg1.d2.s2":
                    assertEquals((double)result[i],  avg + 1, delta);
                    break;
                case "sg1.d3.s3":
                    assertEquals((double)result[i], avg + 2, delta);
                    break;
                case "sg1.d4.s4":
                    assertEquals((double)result[i], avg + 3, delta);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(dsResPaths.size(), 4);
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                switch (dsResPaths.get(j)){
                    case "sg1.d1.s1":
                        assertEquals((double)dsResult.get(j), avg, delta);
                        break;
                    case "sg1.d2.s2":
                        assertEquals((double)dsResult.get(j), avg + 1, delta);
                        break;
                    case "sg1.d3.s3":
                        assertEquals((double)dsResult.get(j), avg + 2, delta);
                        break;
                    case "sg1.d4.s4":
                        assertEquals((double)dsResult.get(j), avg + 3, delta);
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void deletePartialDataInColumnTest() throws SessionException, ExecutionException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D3_S3);
        delPaths.add(COLUMN_D4_S4);

        // ensure after delete there are still points in the timeseries
        long delStartTime = START_TIME + TIME_PERIOD / 5;
        long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
        long delTimePeriod = delEndTime - delStartTime + 1;

        session.deleteDataInColumns(delPaths, delStartTime, delEndTime);

        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);

        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(resPaths.size(), 4);
        assertEquals(dataSet.getTimestamps().length, TIME_PERIOD);
        assertEquals(dataSet.getValues().size(), TIME_PERIOD);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i + START_TIME);
            List<Object> result = dataSet.getValues().get(i);
            if (delStartTime <= timestamp && timestamp <= delEndTime) {
                for (int j = 0; j < 4; j++) {
                    if ("sg1.d2.s2".equals(resPaths.get(j))) {
                        assertEquals(result.get(j), timestamp + 1);
                    } else {
                        assertNull(result.get(j));
                    }
                }
            } else {
                for (int j = 0; j < 4; j++) {
                    switch (resPaths.get(j)) {
                        case "sg1.d1.s1":
                            assertEquals(result.get(j), timestamp);
                            break;
                        case "sg1.d2.s2":
                            assertEquals(result.get(j), timestamp + 1);
                            break;
                        case "sg1.d3.s3":
                            assertEquals(result.get(j), timestamp + 2);
                            break;
                        case "sg1.d4.s4":
                            assertEquals(result.get(j), timestamp + 3);
                            break;
                        default:
                            fail();
                            break;
                    }
                }
            }
        }

        // Test avg for the delete
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(avgResPaths.size(), 4);
        assertEquals(avgDataSet.getValues().length, 4);

        for(int i = 0; i < 4; i++) {
            double avg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                    - (delStartTime + delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);
            switch (avgResPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(avgResult[i], avg);
                    break;
                case "sg1.d2.s2":
                    assertEquals(avgResult[i],(START_TIME + END_TIME) / 2.0 + 1);
                    break;
                case "sg1.d3.s3":
                    assertEquals(avgResult[i],avg + 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(avgResult[i],avg + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        // Test max for the delete
        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> maxResPaths = maxDataSet.getPaths();
        Object[] maxResult = maxDataSet.getValues();
        assertEquals(maxResPaths.size(), 4);
        assertEquals(maxDataSet.getValues().length, 4);

        for(int i = 0; i < 4; i++) {
            long max = (delEndTime >= END_TIME) ? delStartTime - 1 : END_TIME;
            switch (maxResPaths.get(i)){
                case "sg1.d1.s1":
                    assertEquals(maxResult[i], max);
                    break;
                case "sg1.d2.s2":
                    assertEquals(maxResult[i], END_TIME + 1);
                    break;
                case "sg1.d3.s3":
                    assertEquals(maxResult[i], max + 2);
                    break;
                case "sg1.d4.s4":
                    assertEquals(maxResult[i], max + 3);
                    break;
                default:
                    fail();
                    break;
            }
        }

        // Test downSample avg of the delete
        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsStartTime = dsDataSet.getTimestamps()[i];
            assertEquals(dsStartTime, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < dsResPaths.size(); j++) {
                long dsEndTime = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsStartTime + dsEndTime) / 2.0;
                switch (dsResPaths.get(j)){
                    case "sg1.d2.s2":
                        assertEquals((double)dsResult.get(j), avg + 1, delta);
                        break;
                    case "sg1.d1.s1":
                        if(dsStartTime > delEndTime || dsEndTime < delStartTime){
                            assertEquals((double)dsResult.get(j), avg, delta);
                        } else if(dsStartTime >= delStartTime && dsEndTime <= delEndTime){
                            assertNull(dsResult.get(j));
                        } else if (dsStartTime < delStartTime){
                            assertEquals((double)dsResult.get(j), (dsStartTime + delStartTime - 1) / 2.0, delta);
                        } else {
                            assertEquals((double)dsResult.get(j), (dsEndTime + delEndTime + 1) / 2.0, delta);
                        }
                        break;
                    case "sg1.d3.s3":
                        if(dsStartTime > delEndTime || dsEndTime < delStartTime){
                            assertEquals((double)dsResult.get(j), avg + 2, delta);
                        } else if(dsStartTime >= delStartTime && dsEndTime <= delEndTime){
                            assertNull(dsResult.get(j));
                        } else if (dsStartTime < delStartTime){
                            assertEquals((double)dsResult.get(j), (dsStartTime + delStartTime - 1) / 2.0 + 2, delta);
                        } else {
                            assertEquals((double)dsResult.get(j), (dsEndTime + delEndTime + 1) / 2.0 + 2, delta);
                        }
                        break;
                    case "sg1.d4.s4":
                        if(dsStartTime > delEndTime || dsEndTime < delStartTime){
                            assertEquals((double)dsResult.get(j), avg + 3, delta);
                        } else if(dsStartTime >= delStartTime && dsEndTime <= delEndTime){
                            assertNull(dsResult.get(j));
                        } else if (dsStartTime < delStartTime){
                            assertEquals((double)dsResult.get(j), (dsStartTime + delStartTime - 1) / 2.0 + 3, delta);
                        } else {
                            assertEquals((double)dsResult.get(j), (dsEndTime + delEndTime + 1) / 2.0 + 3, delta);
                        }
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }

    }

    @Test
    public void deleteAllDataInColumnTest() throws SessionException, ExecutionException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D3_S3);
        delPaths.add(COLUMN_D4_S4);

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
            for (int j = 0; j < 4; j++) {
                if ("sg1.d2.s2".equals(resPaths.get(j))) {
                    assertEquals(result.get(j), timestamp + 1);
                } else {
                    assertNull(result.get(j));
                }
            }
        }

        // Test value filter for the delete
        int vftime = 1123;
        String booleanExpression = COLUMN_D2_S2 + " > "+vftime;
        SessionQueryDataSet vfDataSet = session.valueFilterQuery(paths, START_TIME, END_TIME, booleanExpression);
        int vflen = vfDataSet.getTimestamps().length;
        List<String> vfResPaths = vfDataSet.getPaths();
        assertEquals(vfDataSet.getTimestamps().length, TIME_PERIOD + START_TIME - vftime - 1);
        for (int i = 0; i < vflen; i++){
            long timestamp = vfDataSet.getTimestamps()[i];
            assertEquals(timestamp, i + vftime);
            List<Object> result = vfDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                if ("sg1.d2.s2".equals(vfResPaths.get(j))) {
                    assertEquals(result.get(j), timestamp + 1);
                } else {
                    assertNull(result.get(j));
                }
            }
        }

        // Test aggregate function for the delete
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(avgResPaths.size(), 4);
        assertEquals(avgDataSet.getValues().length, 4);
        for(int i = 0; i < 4; i++) {
            switch (avgResPaths.get(i)){
                case "sg1.d2.s2":
                    assertEquals(avgResult[i],(START_TIME + END_TIME) / 2.0 + 1);
                    break;
                case "sg1.d1.s1":
                case "sg1.d3.s3":
                case "sg1.d4.s4":
                    assertEquals(new String((byte[]) avgResult[i]), "null");
                    break;
                default:
                    fail();
                    break;
            }
        }
        // Test downsample function for the delete
        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(dsLen, factLen);
        assertEquals(dsDataSet.getValues().size(), factLen);
        for (int i = 0; i < dsLen; i++){
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(dsTimestamp, START_TIME + i * PRECISION);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < dsResPaths.size(); j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                switch (dsResPaths.get(j)){
                    case "sg1.d2.s2":
                        assertEquals((double)dsResult.get(j), avg + 1, delta);
                        break;
                    case "sg1.d1.s1":
                    case "sg1.d3.s3":
                    case "sg1.d4.s4":
                        assertNull(dsResult.get(j));
                        break;
                    default:
                        fail();
                        break;
                }
            }
        }
    }

    @Test
    public void deleteAllColumnsTest() throws SessionException, ExecutionException {
        session.deleteColumns(paths);
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        assertEquals(dataSet.getPaths().size(), 0);
        assertEquals(dataSet.getTimestamps().length, 0);
        assertEquals(dataSet.getValues().size(), 0);
    }

    @Test
    public void deletePartialColumnsTest() throws SessionException, ExecutionException {
        List<String> delPaths = new ArrayList<>();
        delPaths.add(COLUMN_D1_S1);
        delPaths.add(COLUMN_D3_S3);
        delPaths.add(COLUMN_D4_S4);
        session.deleteColumns(delPaths);
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        assertEquals(dataSet.getPaths().size(), 1);
        assertEquals(dataSet.getPaths().get(0), "sg1.d2.s2");
        assertEquals(len, TIME_PERIOD);
        assertEquals(dataSet.getValues().size(), TIME_PERIOD);
        for (int i = 0; i < len; i++){
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(timestamp, i + START_TIME);
            List<Object> result = dataSet.getValues().get(i);
            assertEquals(result.get(0), timestamp + 1);
        }
    }

    private static void addColumns() throws SessionException, ExecutionException {
        List<String> addPaths = new ArrayList<>();
        addPaths.add(COLUMN_D1_S1);
        addPaths.add(COLUMN_D2_S2);
        addPaths.add(COLUMN_D3_S3);
        addPaths.add(COLUMN_D4_S4);

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
        insertPaths.add(COLUMN_D2_S2);
        insertPaths.add(COLUMN_D3_S3);
        insertPaths.add(COLUMN_D4_S4);

        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME;
        }

        Object[] valuesList = new Object[4];
        for (long i = 0; i < 4; i++) {
            Object[] values = new Object[(int) TIME_PERIOD];
            for (long j = 0; j < TIME_PERIOD; j++) {
                values[(int) j] = i + j + START_TIME;
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
