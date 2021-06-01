package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBSessionIT {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBSessionIT.class);
    private static final String COLUMN_D1_S1 = "sg1.d1.s1";
    private static final String COLUMN_D2_S2 = "sg1.d2.s2";
    private static final String COLUMN_D3_S3 = "sg1.d3.s3";
    private static final String COLUMN_D4_S4 = "sg1.d4.s4";
    private static final long TIME_PERIOD = 100000L;
    private static final long START_TIME = 1000L;
    private static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    private static final long PRECISION = 123L;
    private static final double delta = 1e-7;
    private static Session session;
    private List<String> paths = new ArrayList<>();

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

    @Before
    public void setUp() {
        try {
            paths.add(COLUMN_D1_S1);
            paths.add(COLUMN_D2_S2);
            paths.add(COLUMN_D3_S3);
            paths.add(COLUMN_D4_S4);
            session = new Session("127.0.0.1", 6888, "root", "root");
            session.openSession();
            insertRecords();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    @After
    public void tearDown() throws SessionException {
        // delete metadata from ZooKeeper
        ZooKeeper zk = null;
        try {
            zk = new ZooKeeper("127.0.0.1:2181", 5000, null);
            ZKUtil.deleteRecursive(zk, "/iginx");
            ZKUtil.deleteRecursive(zk, "/storage");
            ZKUtil.deleteRecursive(zk, "/unit");
            ZKUtil.deleteRecursive(zk, "/lock");
            ZKUtil.deleteRecursive(zk, "/fragment");
        } catch (IOException | InterruptedException | KeeperException e) {
            logger.error(e.getMessage());
        }

        // delete data from IoTDB
        org.apache.iotdb.session.Session iotdbSession = null;
        try {
            iotdbSession = new org.apache.iotdb.session.Session("127.0.0.1", 6667, "root", "root");
            iotdbSession.open(false);
            iotdbSession.executeNonQueryStatement("DELETE TIMESERIES root.*");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }

        // close session
        try {
            iotdbSession.close();
            if (zk != null) {
                zk.close();
            }
            session.closeSession();
        } catch (InterruptedException | IoTDBConnectionException e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void queryDataTest() throws SessionException, ExecutionException {
        SessionQueryDataSet dataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(4, resPaths.size());
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                switch (resPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(timestamp, result.get(j));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(timestamp + 1, result.get(j));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(timestamp + 2, result.get(j));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(timestamp + 3, result.get(j));
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

        String booleanExpression = COLUMN_D1_S1 + ">" + s1 + " and " + COLUMN_D2_S2 + "<" + s2 + " and(" +
                COLUMN_D3_S3 + " > " + s3 + " or " + COLUMN_D4_S4 + " < " + s4 + ")";
        SessionQueryDataSet dataSet = session.valueFilterQuery(paths, START_TIME, END_TIME, booleanExpression);
        int len = dataSet.getTimestamps().length;
        List<String> resPaths = dataSet.getPaths();
        assertEquals(4, resPaths.size());
        assertEquals(TIME_PERIOD - (s1 - START_TIME + 1) - (END_TIME - s2 + 2) - (s3 - s4 + 2), len);
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                switch (resPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(timestamp, result.get(j));
                        assertTrue(timestamp > s1);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(timestamp + 1, result.get(j));
                        assertTrue(timestamp + 1 < s2);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(timestamp + 2, result.get(j));
                        assertTrue(timestamp + 2 > s3 || timestamp + 3 < s4);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(timestamp + 3, result.get(j));
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
        assertEquals(4, resPaths.size());
        assertEquals(4, len);
        assertEquals(4, maxDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(-1, maxDataSet.getTimestamps()[i]);
            switch (resPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(END_TIME, result[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals(END_TIME + 1, result[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(END_TIME + 2, result[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(END_TIME + 3, result[i]);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                switch (dsResPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(maxNum, dsResult.get(j));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(maxNum + 1, dsResult.get(j));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(maxNum + 2, dsResult.get(j));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(maxNum + 3, dsResult.get(j));
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
        assertEquals(4, resPaths.size());
        assertEquals(4, len);
        assertEquals(4, minDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(-1, minDataSet.getTimestamps()[i]);
            switch (resPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(START_TIME, result[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals(START_TIME + 1, result[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(START_TIME + 2, result[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(START_TIME + 3, result[i]);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long minNum = START_TIME + i * PRECISION;
                switch (dsResPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(minNum, dsResult.get(j));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(minNum + 1, dsResult.get(j));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(minNum + 2, dsResult.get(j));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(minNum + 3, dsResult.get(j));
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
        assertEquals(4, resPaths.size());
        assertEquals(4, len);
        assertEquals(4, firstDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(-1, firstDataSet.getTimestamps()[i]);
            switch (resPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(START_TIME, result[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals(START_TIME + 1, result[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(START_TIME + 2, result[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(START_TIME + 3, result[i]);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long firstNum = START_TIME + i * PRECISION;
                switch (dsResPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(firstNum, dsResult.get(j));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(firstNum + 1, dsResult.get(j));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(firstNum + 2, dsResult.get(j));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(firstNum + 3, dsResult.get(j));
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
        assertEquals(4, resPaths.size());
        assertEquals(4, len);
        assertEquals(4, lastDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(-1, lastDataSet.getTimestamps()[i]);
            switch (resPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(END_TIME, result[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals(END_TIME + 1, result[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(END_TIME + 2, result[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(END_TIME + 3, result[i]);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long lastNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                switch (dsResPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(lastNum, dsResult.get(j));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(lastNum + 1, dsResult.get(j));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(lastNum + 2, dsResult.get(j));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(lastNum + 3, dsResult.get(j));
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
        assertEquals(4, resPaths.size());
        assertEquals(4, countDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(TIME_PERIOD, result[i]);
        }
        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long countNum = ((START_TIME + (i + 1) * PRECISION - 1) <= END_TIME) ? PRECISION : END_TIME - dsTimestamp + 1;
                assertEquals(countNum, dsResult.get(j));
            }
        }
    }

    @Test
    public void aggrSumTest() throws SessionException, ExecutionException {
        SessionAggregateQueryDataSet sumDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM);
        assertNull(sumDataSet.getTimestamps());
        List<String> resPaths = sumDataSet.getPaths();
        Object[] result = sumDataSet.getValues();
        assertEquals(4, resPaths.size());
        assertEquals(4, sumDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            double sum = (START_TIME + END_TIME) * TIME_PERIOD / 2.0;
            switch (resPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(sum, result[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals(sum + TIME_PERIOD, result[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(sum + TIME_PERIOD * 2, result[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(sum + TIME_PERIOD * 3, result[i]);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double sum = (dsTimestamp + maxNum) * (maxNum - dsTimestamp + 1) / 2.0;
                switch (dsResPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(sum, dsResult.get(j));
                        break;
                    case "sg1.d2.s2":
                        assertEquals(sum + (maxNum - dsTimestamp + 1), dsResult.get(j));
                        break;
                    case "sg1.d3.s3":
                        assertEquals(sum + 2 * (maxNum - dsTimestamp + 1), dsResult.get(j));
                        break;
                    case "sg1.d4.s4":
                        assertEquals(sum + 3 * (maxNum - dsTimestamp + 1), dsResult.get(j));
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
        assertEquals(4, resPaths.size());
        assertEquals(4, avgDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            double avg = (START_TIME + END_TIME) / 2.0;
            switch (resPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(avg, (double) result[i], delta);
                    break;
                case "sg1.d2.s2":
                    assertEquals(avg + 1, (double) result[i], delta);
                    break;
                case "sg1.d3.s3":
                    assertEquals(avg + 2, (double) result[i], delta);
                    break;
                case "sg1.d4.s4":
                    assertEquals(avg + 3, (double) result[i], delta);
                    break;
                default:
                    fail();
                    break;
            }
        }

        SessionQueryDataSet dsDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsLen = dsDataSet.getTimestamps().length;
        List<String> dsResPaths = dsDataSet.getPaths();
        assertEquals(4, dsResPaths.size());
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                switch (dsResPaths.get(j)) {
                    case "sg1.d1.s1":
                        assertEquals(avg, (double) dsResult.get(j), delta);
                        break;
                    case "sg1.d2.s2":
                        assertEquals(avg + 1, (double) dsResult.get(j), delta);
                        break;
                    case "sg1.d3.s3":
                        assertEquals(avg + 2, (double) dsResult.get(j), delta);
                        break;
                    case "sg1.d4.s4":
                        assertEquals(avg + 3, (double) dsResult.get(j), delta);
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
        assertEquals(4, resPaths.size());
        assertEquals(TIME_PERIOD, dataSet.getTimestamps().length);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            if (delStartTime <= timestamp && timestamp <= delEndTime) {
                for (int j = 0; j < 4; j++) {
                    if ("sg1.d2.s2".equals(resPaths.get(j))) {
                        assertEquals(timestamp + 1, result.get(j));
                    } else {
                        assertNull(result.get(j));
                    }
                }
            } else {
                for (int j = 0; j < 4; j++) {
                    switch (resPaths.get(j)) {
                        case "sg1.d1.s1":
                            assertEquals(timestamp, result.get(j));
                            break;
                        case "sg1.d2.s2":
                            assertEquals(timestamp + 1, result.get(j));
                            break;
                        case "sg1.d3.s3":
                            assertEquals(timestamp + 2, result.get(j));
                            break;
                        case "sg1.d4.s4":
                            assertEquals(timestamp + 3, result.get(j));
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
        assertEquals(4, avgResPaths.size());
        assertEquals(4, avgDataSet.getValues().length);

        for (int i = 0; i < 4; i++) {
            double avg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                    - (delStartTime + delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);
            switch (avgResPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(avg, avgResult[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals((START_TIME + END_TIME) / 2.0 + 1, avgResult[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(avg + 2, avgResult[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(avg + 3, avgResult[i]);
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
        assertEquals(4, maxResPaths.size());
        assertEquals(4, maxDataSet.getValues().length);

        for (int i = 0; i < 4; i++) {
            long max = (delEndTime >= END_TIME) ? delStartTime - 1 : END_TIME;
            switch (maxResPaths.get(i)) {
                case "sg1.d1.s1":
                    assertEquals(max, maxResult[i]);
                    break;
                case "sg1.d2.s2":
                    assertEquals(END_TIME + 1, maxResult[i]);
                    break;
                case "sg1.d3.s3":
                    assertEquals(max + 2, maxResult[i]);
                    break;
                case "sg1.d4.s4":
                    assertEquals(max + 3, maxResult[i]);
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
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsStartTime = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsStartTime);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < dsResPaths.size(); j++) {
                long dsEndTime = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsStartTime + dsEndTime) / 2.0;
                switch (dsResPaths.get(j)) {
                    case "sg1.d2.s2":
                        assertEquals(avg + 1, (double) dsResult.get(j), delta);
                        break;
                    case "sg1.d1.s1":
                        if (dsStartTime > delEndTime || dsEndTime < delStartTime) {
                            assertEquals(avg, (double) dsResult.get(j), delta);
                        } else if (dsStartTime >= delStartTime && dsEndTime <= delEndTime) {
                            assertNull(dsResult.get(j));
                        } else if (dsStartTime < delStartTime) {
                            assertEquals((dsStartTime + delStartTime - 1) / 2.0, (double) dsResult.get(j), delta);
                        } else {
                            assertEquals((dsEndTime + delEndTime + 1) / 2.0, (double) dsResult.get(j), delta);
                        }
                        break;
                    case "sg1.d3.s3":
                        if (dsStartTime > delEndTime || dsEndTime < delStartTime) {
                            assertEquals(avg + 2, (double) dsResult.get(j), delta);
                        } else if (dsStartTime >= delStartTime && dsEndTime <= delEndTime) {
                            assertNull(dsResult.get(j));
                        } else if (dsStartTime < delStartTime) {
                            assertEquals((dsStartTime + delStartTime - 1) / 2.0 + 2, (double) dsResult.get(j), delta);
                        } else {
                            assertEquals((dsEndTime + delEndTime + 1) / 2.0 + 2, (double) dsResult.get(j), delta);
                        }
                        break;
                    case "sg1.d4.s4":
                        if (dsStartTime > delEndTime || dsEndTime < delStartTime) {
                            assertEquals(avg + 3, (double) dsResult.get(j), delta);
                        } else if (dsStartTime >= delStartTime && dsEndTime <= delEndTime) {
                            assertNull(dsResult.get(j));
                        } else if (dsStartTime < delStartTime) {
                            assertEquals((dsStartTime + delStartTime - 1) / 2.0 + 3, (double) dsResult.get(j), delta);
                        } else {
                            assertEquals((dsEndTime + delEndTime + 1) / 2.0 + 3, (double) dsResult.get(j), delta);
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
        assertEquals(TIME_PERIOD, dataSet.getTimestamps().length);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                if ("sg1.d2.s2".equals(resPaths.get(j))) {
                    assertEquals(timestamp + 1, result.get(j));
                } else {
                    assertNull(result.get(j));
                }
            }
        }

        // Test value filter for the delete
        int vftime = 1123;
        String booleanExpression = COLUMN_D2_S2 + " > " + vftime;
        SessionQueryDataSet vfDataSet = session.valueFilterQuery(paths, START_TIME, END_TIME, booleanExpression);
        int vflen = vfDataSet.getTimestamps().length;
        List<String> vfResPaths = vfDataSet.getPaths();
        assertEquals(TIME_PERIOD + START_TIME - vftime - 1, vfDataSet.getTimestamps().length);
        for (int i = 0; i < vflen; i++) {
            long timestamp = vfDataSet.getTimestamps()[i];
            assertEquals(i + vftime, timestamp);
            List<Object> result = vfDataSet.getValues().get(i);
            for (int j = 0; j < 4; j++) {
                if ("sg1.d2.s2".equals(vfResPaths.get(j))) {
                    assertEquals(timestamp + 1, result.get(j));
                } else {
                    assertNull(result.get(j));
                }
            }
        }

        // Test aggregate function for the delete
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(4, avgResPaths.size());
        assertEquals(4, avgDataSet.getValues().length);
        for (int i = 0; i < 4; i++) {
            switch (avgResPaths.get(i)) {
                case "sg1.d2.s2":
                    assertEquals((START_TIME + END_TIME) / 2.0 + 1, avgResult[i]);
                    break;
                case "sg1.d1.s1":
                case "sg1.d3.s3":
                case "sg1.d4.s4":
                    assertEquals("null", new String((byte[]) avgResult[i]));
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
        assertEquals(factLen, dsLen);
        assertEquals(factLen, dsDataSet.getValues().size());
        for (int i = 0; i < dsLen; i++) {
            long dsTimestamp = dsDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDataSet.getValues().get(i);
            for (int j = 0; j < dsResPaths.size(); j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                switch (dsResPaths.get(j)) {
                    case "sg1.d2.s2":
                        assertEquals(avg + 1, (double) dsResult.get(j), delta);
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
        assertEquals(0, dataSet.getPaths().size());
        assertEquals(0, dataSet.getTimestamps().length);
        assertEquals(0, dataSet.getValues().size());
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
        assertEquals(1, dataSet.getPaths().size());
        assertEquals("sg1.d2.s2", dataSet.getPaths().get(0));
        assertEquals(TIME_PERIOD, len);
        assertEquals(TIME_PERIOD, dataSet.getValues().size());
        for (int i = 0; i < len; i++) {
            long timestamp = dataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = dataSet.getValues().get(i);
            assertEquals(timestamp + 1, result.get(0));
        }
    }
}
