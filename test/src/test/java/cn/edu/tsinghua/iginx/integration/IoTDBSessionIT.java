package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionAggregateQueryDataSet;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.StorageEngineType;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class IoTDBSessionIT {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBSessionIT.class);
    /*
    private static final String COLUMN_D1_S1 = "sg1.d1.s1";
    private static final String COLUMN_D2_S2 = "sg1.d2.s2";
    private static final String COLUMN_D3_S3 = "sg1.d3.s3";
    private static final String COLUMN_D4_S4 = "sg1.d4.s4";*/
    private static final long TIME_PERIOD = 100000L;
    private static final long START_TIME = 1000L;
    private static final long END_TIME = START_TIME + TIME_PERIOD - 1;
    private static final long PRECISION = 123L;
    private static final double delta = 1e-7;
    private static Session session;
    private int currPath = 0;

    private List<String> getPaths(int startPosition, int len) {
        List<String> paths = new ArrayList<>();
        for(int i = startPosition; i < startPosition + len; i++){
            paths.add("sg1.d" + i + ".s" + i);
        }
        return paths;
    }

    private int getPathNum(String path) {
        String pattern = "^sg1\\.d(\\d+)\\.s(\\d+)$";
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(path);
        if (m.find()) {
            int d = Integer.parseInt(m.group(1));
            int s = Integer.parseInt(m.group(2));
            if (d == s) {
                return d;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    private void insertNumRecords(List<String> insertPaths) throws SessionException, ExecutionException {
        int pathLen = insertPaths.size();
        long[] timestamps = new long[(int) TIME_PERIOD];
        for (long i = 0; i < TIME_PERIOD; i++) {
            timestamps[(int) i] = i + START_TIME;
        }

        Object[] valuesList = new Object[pathLen];
        for (int i = 0; i < pathLen; i++) {
            int pathNum = getPathNum(insertPaths.get(i));
            Object[] values = new Object[(int) TIME_PERIOD];
            for (long j = 0; j < TIME_PERIOD; j++) {
                values[(int) j] = pathNum + j + START_TIME;
            }
            valuesList[i] = values;
        }

        List<DataType> dataTypeList = new ArrayList<>();
        for (int i = 0; i < pathLen; i++) {
            dataTypeList.add(DataType.LONG);
        }
        session.insertColumnRecords(insertPaths, timestamps, valuesList, dataTypeList, null);
    }

    @Before
    public void setUp() {
        try {
            session = new Session("127.0.0.1", 6888, "root", "root");
            session.openSession();
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
            ZKUtil.deleteRecursive(zk, "/unit");
            ZKUtil.deleteRecursive(zk, "/lock");
            ZKUtil.deleteRecursive(zk, "/fragment");
            ZKUtil.deleteRecursive(zk, "/schema");
        } catch (IOException | InterruptedException | KeeperException e) {
            logger.error(e.getMessage());
        }

        /*
        // delete data from IoTDB
        org.apache.iotdb.session.Session iotdbSession = null;
        try {
            iotdbSession = new org.apache.iotdb.session.Session("127.0.0.1", 6667, "root", "root");
            iotdbSession.open(false);
            iotdbSession.executeNonQueryStatement("DELETE TIMESERIES root.*");
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            logger.error(e.getMessage());
        }*/

        // close session
        try {
            //iotdbSession.close();
            if (zk != null) {
                zk.close();
            }
            session.closeSession();
        } catch (InterruptedException /*| IoTDBConnectionException*/ e) {
            logger.error(e.getMessage());
        }
    }

    @Test
    public void ioTDBSessionTest() throws ExecutionException, SessionException {
        int simpleLen = 2;
        List<String> paths = getPaths(currPath, simpleLen);
        //Simple Test(Including query,valueFilter,aggr:max/min/first/last/count/sum/avg)
        insertNumRecords(paths);
        //query
        SessionQueryDataSet simpleQueryDataSet = session.queryData(paths, START_TIME, END_TIME + 1);
        int simpleResLen = simpleQueryDataSet.getTimestamps().length;
        List<String> queryResPaths = simpleQueryDataSet.getPaths();
        assertEquals(simpleLen, queryResPaths.size());
        assertEquals(TIME_PERIOD, simpleResLen);
        assertEquals(TIME_PERIOD, simpleQueryDataSet.getValues().size());
        for (int i = 0; i < simpleResLen; i++) {
            long timestamp = simpleQueryDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> queryResult = simpleQueryDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                int pathNum = getPathNum(queryResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(timestamp + pathNum, queryResult.get(j));
            }
        }
        // aggrMax
        SessionAggregateQueryDataSet maxDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX);
        List<String> maxResPaths = maxDataSet.getPaths();
        Object[] maxResult = maxDataSet.getValues();
        assertEquals(simpleLen, maxResPaths.size());
        assertEquals(simpleLen, maxDataSet.getTimestamps().length);
        assertEquals(simpleLen, maxDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            assertEquals(-1, maxDataSet.getTimestamps()[i]);
            int pathNum = getPathNum(maxResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(END_TIME + pathNum, maxResult[i]);
        }
        // aggrMin
        SessionAggregateQueryDataSet minDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN);
        List<String> minResPaths = minDataSet.getPaths();
        Object[] minResult = minDataSet.getValues();
        assertEquals(simpleLen, minResPaths.size());
        assertEquals(simpleLen, minDataSet.getTimestamps().length);
        assertEquals(simpleLen, minDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            assertEquals(-1, minDataSet.getTimestamps()[i]);
            int pathNum = getPathNum(minResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(START_TIME + pathNum, minResult[i]);
        }
        //aggrFirst
        SessionAggregateQueryDataSet firstDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST);
        List<String> firstResPaths = firstDataSet.getPaths();
        Object[] firstResult = firstDataSet.getValues();
        assertEquals(simpleLen, firstResPaths.size());
        assertEquals(simpleLen, firstDataSet.getTimestamps().length);
        assertEquals(simpleLen, firstDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            int pathNum = getPathNum(firstResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(START_TIME + pathNum, firstResult[i]);
        }
        //aggrLast
        SessionAggregateQueryDataSet lastDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST);
        List<String> lastResPaths = lastDataSet.getPaths();
        Object[] lastResult = lastDataSet.getValues();
        assertEquals(simpleLen, lastResPaths.size());
        assertEquals(simpleLen, lastDataSet.getTimestamps().length);
        assertEquals(simpleLen, lastDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            int pathNum = getPathNum(lastResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(END_TIME + pathNum, lastResult[i]);
        }
        //aggrCount
        SessionAggregateQueryDataSet countDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT);
        assertNull(countDataSet.getTimestamps());
        List<String> countResPaths = countDataSet.getPaths();
        Object[] countResult = countDataSet.getValues();
        assertEquals(simpleLen, countResPaths.size());
        assertEquals(simpleLen, countDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            assertEquals(TIME_PERIOD, countResult[i]);
        }
        //aggrSum
        SessionAggregateQueryDataSet sumDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM);
        assertNull(sumDataSet.getTimestamps());
        List<String> sumResPaths = sumDataSet.getPaths();
        Object[] sumResult = sumDataSet.getValues();
        assertEquals(simpleLen, sumResPaths.size());
        assertEquals(simpleLen, sumDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            double sum = (START_TIME + END_TIME) * TIME_PERIOD / 2.0;
            int pathNum = getPathNum(sumResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(sum + pathNum * TIME_PERIOD, (double)sumResult[i], delta);
        }
        //aggrAvg
        SessionAggregateQueryDataSet avgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        assertNull(avgDataSet.getTimestamps());
        List<String> avgResPaths = avgDataSet.getPaths();
        Object[] avgResult = avgDataSet.getValues();
        assertEquals(simpleLen, avgResPaths.size());
        assertEquals(simpleLen, avgDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            double avg = (START_TIME + END_TIME) / 2.0;
            int pathNum = getPathNum(sumResPaths.get(i));
            assertNotEquals(pathNum, -1);
            assertEquals(avg + pathNum, (double)avgResult[i], delta);
        }

        //DownSample
        long factLen = (TIME_PERIOD / PRECISION) + ((TIME_PERIOD % PRECISION == 0) ? 0 : 1);

        // max
        SessionQueryDataSet dsMaxDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MAX, PRECISION);
        int dsMaxLen = dsMaxDataSet.getTimestamps().length;
        List<String> dsMaxResPaths = dsMaxDataSet.getPaths();
        assertEquals(simpleLen, dsMaxResPaths.size());
        assertEquals(factLen, dsMaxLen);
        assertEquals(factLen, dsMaxDataSet.getValues().size());
        for (int i = 0; i < dsMaxLen; i++) {
            long dsTimestamp = dsMaxDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsMaxDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                int pathNum = getPathNum(dsMaxResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(maxNum + pathNum, dsResult.get(j));
            }
        }
        //min
        SessionQueryDataSet dsMinDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.MIN, PRECISION);
        int dsMinLen = dsMinDataSet.getTimestamps().length;
        List<String> dsMinResPaths = dsMinDataSet.getPaths();
        assertEquals(simpleLen, dsMinResPaths.size());
        assertEquals(factLen, dsMinLen);
        assertEquals(factLen, dsMinDataSet.getValues().size());
        for (int i = 0; i < dsMinLen; i++) {
            long dsTimestamp = dsMinDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsMinDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long minNum = START_TIME + i * PRECISION;
                int pathNum = getPathNum(dsMinResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(minNum + pathNum, dsResult.get(j));
            }
        }
        //first
        SessionQueryDataSet dsFirstDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.FIRST, PRECISION);
        int dsFirstLen = dsFirstDataSet.getTimestamps().length;
        List<String> dsFirstResPaths = dsFirstDataSet.getPaths();
        assertEquals(simpleLen, dsFirstResPaths.size());
        assertEquals(factLen, dsFirstLen);
        assertEquals(factLen, dsFirstDataSet.getValues().size());
        for (int i = 0; i < dsFirstLen; i++) {
            long dsTimestamp = dsFirstDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsFirstDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long firstNum = START_TIME + i * PRECISION;
                int pathNum = getPathNum(dsFirstResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(firstNum + pathNum, dsResult.get(j));
            }
        }
        //last
        SessionQueryDataSet dsLastDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.LAST, PRECISION);
        int dsLastLen = dsLastDataSet.getTimestamps().length;
        List<String> dsLastResPaths = dsLastDataSet.getPaths();
        assertEquals(simpleLen, dsLastResPaths.size());
        assertEquals(factLen, dsLastLen);
        assertEquals(factLen, dsLastDataSet.getValues().size());
        for (int i = 0; i < dsLastLen; i++) {
            long dsTimestamp = dsLastDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsLastDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long lastNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                int pathNum = getPathNum(dsLastResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(lastNum + pathNum, dsResult.get(j));
            }
        }
        //Count
        SessionQueryDataSet dsCountDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT, PRECISION);
        int dsCountLen = dsCountDataSet.getTimestamps().length;
        List<String> dsCountResPaths = dsCountDataSet.getPaths();
        assertEquals(simpleLen, dsCountResPaths.size());
        assertEquals(factLen, dsCountLen);
        assertEquals(factLen, dsCountDataSet.getValues().size());
        for (int i = 0; i < dsCountLen; i++) {
            long dsTimestamp = dsCountDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsCountDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long countNum = ((START_TIME + (i + 1) * PRECISION - 1) <= END_TIME) ? PRECISION : END_TIME - dsTimestamp + 1;
                assertEquals(countNum, dsResult.get(j));
            }
        }
        //Sum
        SessionQueryDataSet dsSumDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.SUM, PRECISION);
        int dsSumLen = dsSumDataSet.getTimestamps().length;
        List<String> dsSumResPaths = dsSumDataSet.getPaths();
        assertEquals(simpleLen, dsSumResPaths.size());
        assertEquals(factLen, dsSumLen);
        assertEquals(factLen, dsSumDataSet.getValues().size());
        for (int i = 0; i < dsSumLen; i++) {
            long dsTimestamp = dsSumDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsSumDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double sum = (dsTimestamp + maxNum) * (maxNum - dsTimestamp + 1) / 2.0;
                int pathNum = getPathNum(dsSumResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(sum + pathNum * (maxNum - dsTimestamp + 1), (double)dsResult.get(j), delta);
            }
        }
        //avg
        SessionQueryDataSet dsAvgDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsAvgLen = dsAvgDataSet.getTimestamps().length;
        List<String> dsAvgResPaths = dsAvgDataSet.getPaths();
        assertEquals(simpleLen, dsAvgResPaths.size());
        assertEquals(factLen, dsAvgLen);
        assertEquals(factLen, dsAvgDataSet.getValues().size());
        for (int i = 0; i < dsAvgLen; i++) {
            long dsTimestamp = dsAvgDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsAvgDataSet.getValues().get(i);
            for (int j = 0; j < simpleLen; j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                int pathNum = getPathNum(dsAvgResPaths.get(j));
                assertNotEquals(pathNum, -1);
                assertEquals(avg + pathNum, (double) dsResult.get(j), delta);
            }
        }
        //deletePartialDataInColumnTest
        int removeLen = 1;
        List<String> delPartPaths = getPaths(currPath, removeLen);
        // ensure after delete there are still points in the timeseries
        long delStartTime = START_TIME + TIME_PERIOD / 5;
        long delEndTime = START_TIME + TIME_PERIOD / 10 * 9;
        long delTimePeriod = delEndTime - delStartTime + 1;

        //delete data
        session.deleteDataInColumns(delPartPaths, delStartTime, delEndTime);

        SessionQueryDataSet delPartDataSet = session.queryData(paths, START_TIME, END_TIME + 1);

        int delPartLen = delPartDataSet.getTimestamps().length;
        List<String> delPartResPaths = delPartDataSet.getPaths();
        assertEquals(simpleLen, delPartResPaths.size());
        assertEquals(TIME_PERIOD, delPartDataSet.getTimestamps().length);
        assertEquals(TIME_PERIOD, delPartDataSet.getValues().size());
        for (int i = 0; i < delPartLen; i++) {
            long timestamp = delPartDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = delPartDataSet.getValues().get(i);
            if (delStartTime <= timestamp && timestamp <= delEndTime) {
                for (int j = 0; j < simpleLen; j++) {
                    int pathNum = getPathNum(delPartResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    if(pathNum >= removeLen) {
                        assertEquals(timestamp + pathNum, result.get(j));
                    } else {
                        assertNull(result.get(j));
                    }
                }
            } else {
                for (int j = 0; j < simpleLen; j++) {
                    int pathNum = getPathNum(delPartResPaths.get(j));
                    assertNotEquals(pathNum, -1);
                    assertEquals(timestamp + pathNum, result.get(j));
                }
            }
        }

        // Test avg for the delete
        SessionAggregateQueryDataSet delPartAvgDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> delPartAvgResPaths = delPartAvgDataSet.getPaths();
        Object[] delPartAvgResult = delPartAvgDataSet.getValues();
        assertEquals(simpleLen, delPartAvgResPaths.size());
        assertEquals(simpleLen, delPartAvgDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            double deleteAvg = ((START_TIME + END_TIME) * TIME_PERIOD / 2.0
                    - (delStartTime + delEndTime) * delTimePeriod / 2.0) / (TIME_PERIOD - delTimePeriod);
            double originAvg = (START_TIME + END_TIME) / 2.0;
            int pathNum = getPathNum(delPartAvgResPaths.get(i));
            assertNotEquals(pathNum, -1);
            if (pathNum < currPath + removeLen){ // Here is the removed rows
                assertEquals(deleteAvg + pathNum, (double)delPartAvgResult[i], delta);
            } else {
                assertEquals(originAvg + pathNum, (double)delPartAvgResult[i], delta);
            }
        }

        // Test count for the delete
        SessionAggregateQueryDataSet delPartCountDataSet = session.aggregateQuery(paths, START_TIME, END_TIME + 1, AggregateType.COUNT);
        List<String> delPartCountResPaths = delPartCountDataSet.getPaths();
        Object[] delPartCountResult = delPartCountDataSet.getValues();
        assertEquals(simpleLen, delPartCountResPaths.size());
        assertEquals(simpleLen, delPartCountDataSet.getValues().length);
        for (int i = 0; i < simpleLen; i++) {
            int pathNum = getPathNum(delPartAvgResPaths.get(i));
            assertNotEquals(pathNum, -1);
            if (pathNum < currPath + removeLen){ // Here is the removed rows
                assertEquals(TIME_PERIOD - delTimePeriod, delPartCountResult[i]);
            } else {
                assertEquals(TIME_PERIOD, delPartCountResult[i]);
            }
        }

        // Test downSample avg of the delete
        SessionQueryDataSet delDsAvgDataSet = session.downsampleQuery(paths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int delDsLen = delDsAvgDataSet.getTimestamps().length;
        List<String> delDsResPaths = delDsAvgDataSet.getPaths();
        assertEquals(factLen, delDsLen);
        assertEquals(factLen, delDsAvgDataSet.getValues().size());
        for (int i = 0; i < delDsLen; i++) {
            long dsStartTime = delDsAvgDataSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsStartTime);
            List<Object> dsResult = delDsAvgDataSet.getValues().get(i);
            for (int j = 0; j < delDsResPaths.size(); j++) {
                long dsEndTime = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double delDsAvg = (dsStartTime + dsEndTime) / 2.0;
                int pathNum = getPathNum(delDsResPaths.get(j));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + removeLen){ // Here is the removed rows
                    if (dsStartTime > delEndTime || dsEndTime < delStartTime) {
                        assertEquals(delDsAvg + pathNum, (double) dsResult.get(j), delta);
                    } else if (dsStartTime >= delStartTime && dsEndTime <= delEndTime) {
                        assertNull(dsResult.get(j));
                    } else if (dsStartTime < delStartTime) {
                        assertEquals((dsStartTime + delStartTime - 1) / 2.0 + pathNum, (double) dsResult.get(j), delta);
                    } else {
                        assertEquals((dsEndTime + delEndTime + 1) / 2.0 + pathNum, (double) dsResult.get(j), delta);
                    }
                } else {
                    assertEquals(delDsAvg + pathNum, (double) dsResult.get(j), delta);
                }
            }
        }
        // The first insert and delete end;
        currPath += simpleLen;

        //deleteAllDataInColumnTest, make new insert and delete here
        int dataInColumnLen = 3;
        List<String> delDataInColumnPaths = getPaths(currPath, dataInColumnLen);
        insertNumRecords(delDataInColumnPaths);
        //TODO add test to test if the insert is right(Is it necessary?)
        int deleteDataInColumnLen = 2;
        List<String> delAllDataInColumnPaths = getPaths(currPath, deleteDataInColumnLen);
        session.deleteDataInColumns(delAllDataInColumnPaths, START_TIME, END_TIME);
        SessionQueryDataSet delDataInColumnDataSet = session.queryData(delDataInColumnPaths, START_TIME, END_TIME + 1);
        int delDataInColumnLen = delDataInColumnDataSet.getTimestamps().length;
        List<String> delDataInColumnResPaths = delDataInColumnDataSet.getPaths();
        assertEquals(TIME_PERIOD, delDataInColumnLen);
        assertEquals(TIME_PERIOD, delDataInColumnDataSet.getValues().size());
        for (int i = 0; i < delDataInColumnLen; i++) {
            long timestamp = delDataInColumnDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = delDataInColumnDataSet.getValues().get(i);
            for (int j = 0; j < dataInColumnLen; j++) {
                int pathNum = getPathNum(delDataInColumnResPaths.get(j));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + deleteDataInColumnLen){ // Here is the removed rows
                    assertNull(result.get(j));
                } else {
                    assertEquals(timestamp + pathNum, result.get(j));
                }
            }
        }

        /*
        // Test value filter for the delete TODO change the value filter to the right test
        int vfTime = 1123;
        String booleanExpression = COLUMN_D2_S2 + " > " + vfTime;
        SessionQueryDataSet vfDataSet = session.valueFilterQuery(delDataInColumnPaths, START_TIME, END_TIME, booleanExpression);
        int vfLen = vfDataSet.getTimestamps().length;
        List<String> vfResPaths = vfDataSet.getPaths();
        assertEquals(TIME_PERIOD + START_TIME - vfTime - 1, vfDataSet.getTimestamps().length);
        for (int i = 0; i < vfLen; i++) {
            long timestamp = vfDataSet.getTimestamps()[i];
            assertEquals(i + vfTime, timestamp);
            List<Object> result = vfDataSet.getValues().get(i);
            for (int j = 0; j < dataInColumnLen; j++) {
                int pathNum = getPathNum(delDataInColumnResPaths.get(j));
                assertNotEquals(pathNum, -1);
                if (pathNum < deleteDataInColumnLen){ // Here is the removed rows
                    assertNull(result.get(j));
                } else {
                    assertEquals(timestamp + pathNum, result.get(j));
                }
            }
        }*/

        // Test aggregate function for the delete
        SessionAggregateQueryDataSet delDataAvgSet = session.aggregateQuery(delDataInColumnPaths, START_TIME, END_TIME + 1, AggregateType.AVG);
        List<String> delDataAvgResPaths = delDataAvgSet.getPaths();
        Object[] delDataAvgResult = delDataAvgSet.getValues();
        assertEquals(dataInColumnLen, delDataAvgResPaths.size());
        assertEquals(dataInColumnLen, delDataAvgSet.getValues().length);
        for (int i = 0; i < dataInColumnLen; i++) {
            int pathNum = getPathNum(delDataAvgResPaths.get(i));
            assertNotEquals(pathNum, -1);
            if (pathNum < currPath + deleteDataInColumnLen){ // Here is the removed rows
                assertEquals("null", new String((byte[]) delDataAvgResult[i]));
            } else {
                assertEquals((START_TIME + END_TIME) / 2.0 + pathNum, delDataAvgResult[i]);
            }
        }

        // Test downsample function for the delete
        SessionQueryDataSet dsDelDataInColSet = session.downsampleQuery(delDataInColumnPaths, START_TIME, END_TIME + 1, AggregateType.AVG, PRECISION);
        int dsDelDataLen = dsDelDataInColSet.getTimestamps().length;
        List<String> dsDelDataResPaths = dsDelDataInColSet.getPaths();
        assertEquals(factLen, dsDelDataLen);
        assertEquals(factLen, dsDelDataInColSet.getValues().size());
        for (int i = 0; i < dsDelDataLen; i++) {
            long dsTimestamp = dsDelDataInColSet.getTimestamps()[i];
            assertEquals(START_TIME + i * PRECISION, dsTimestamp);
            List<Object> dsResult = dsDelDataInColSet.getValues().get(i);
            for (int j = 0; j < dsDelDataResPaths.size(); j++) {
                long maxNum = Math.min((START_TIME + (i + 1) * PRECISION - 1), END_TIME);
                double avg = (dsTimestamp + maxNum) / 2.0;
                int pathNum = getPathNum(dsDelDataResPaths.get(j));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + deleteDataInColumnLen){ // Here is the removed rows
                    assertNull(dsResult.get(j));
                } else {
                    assertEquals(avg + pathNum, (double) dsResult.get(j), delta);
                }
            }
        }

        currPath += dataInColumnLen;
        // deleteAllColumnsTest
        int delAllColumnLen = 3;
        List<String> delAllColumnPaths = getPaths(currPath, delAllColumnLen);
        insertNumRecords(delAllColumnPaths);
        session.deleteColumns(delAllColumnPaths);
        SessionQueryDataSet dataSet = session.queryData(delAllColumnPaths, START_TIME, END_TIME + 1);
        assertEquals(0, dataSet.getPaths().size());
        assertEquals(0, dataSet.getTimestamps().length);
        assertEquals(0, dataSet.getValues().size());

        // deletePartColumnsTest
        currPath += delAllColumnLen;
        int delPartColumnLen = 3;
        List<String> partColumnPaths = getPaths(currPath, delPartColumnLen);
        insertNumRecords(partColumnPaths);
        int delPartColumnNum = 2;
        List<String> delPartColumnPaths = getPaths(currPath, delPartColumnNum);
        session.deleteColumns(delPartColumnPaths);
        SessionQueryDataSet delPartColumnDataSet = session.queryData(partColumnPaths, START_TIME, END_TIME + 1);
        int delPartResLen = delPartColumnDataSet.getTimestamps().length;
        List<String> delPartColResPaths = delPartColumnDataSet.getPaths();
        assertEquals(delPartColumnLen - delPartColumnNum, delPartColResPaths.size());
        assertEquals(TIME_PERIOD, delPartResLen);
        assertEquals(TIME_PERIOD, delPartColumnDataSet.getValues().size());
        for (int i = 0; i < delPartResLen; i++) {
            long timestamp = delPartColumnDataSet.getTimestamps()[i];
            assertEquals(i + START_TIME, timestamp);
            List<Object> result = delPartColumnDataSet.getValues().get(i);
            for(int j = 0; j < delPartColumnLen - delPartColumnNum; j++) {
                int pathNum = getPathNum(delPartColResPaths.get(j));
                assertNotEquals(pathNum, -1);
                if (pathNum < currPath + delPartColumnNum){ // Here is the removed rows
                    fail();
                } else {
                    assertEquals(timestamp + pathNum, result.get(j));
                }
            }
        }


        //DataTypeTest

        //multiThreadTest

        //addStorageGroupTest TODO fix the parameters
        //session.addStorageEngine("127.0.0.1", 6668, StorageEngineType.IOTDB, null);
    }


    /*
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
    }*/
}
