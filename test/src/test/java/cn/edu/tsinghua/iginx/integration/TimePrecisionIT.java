package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.session.SessionQueryDataSet;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TimePrecisionIT {
    protected static Session session;
    protected static boolean isForSession = true, isForSessionPool = false;

    private final TimePrecision[] timePrecision = new TimePrecision[]{TimePrecision.S, TimePrecision.MS, TimePrecision.US, TimePrecision.NS};

    private final String[] path = new String[]{"sg.d1.s1", "sg.d1.s2", "sg.d2.s1", "sg.d3.s1"};

    //host info
    protected static String defaultTestHost = "127.0.0.1";
    protected static int defaultTestPort = 6888;
    protected static String defaultTestUser = "root";
    protected static String defaultTestPass = "root";

    protected static final Logger logger = LoggerFactory.getLogger(TimePrecisionIT.class);

    protected String storageEngineType;

    @BeforeClass
    public static void setUp() {
        if(isForSession)
            session = new Session (defaultTestHost, defaultTestPort, defaultTestUser, defaultTestPass);
        try {
            session.openSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @AfterClass
    public static void tearDown() {
        try {
            session.closeSession();
        } catch (SessionException e) {
            logger.error(e.getMessage());
        }
    }

    @Before
    public void insertData() throws ExecutionException, SessionException {

        for (int i=0; i<4; i++) {
            List<String> paths = new ArrayList<>();
            paths.add(this.path[i]);

            long[] timestamps = new long[1];
            timestamps[0] = 100;

            Object[] valuesList = new Object[1];
            Object[] values = new Object[1];
            values[0] = 5211314L;
            valuesList[0] = values;

            List<DataType> dataTypeList = new ArrayList<>();
            dataTypeList.add(DataType.LONG);

            session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null, timePrecision[i]);
        }

    }

    @After
    public void clearData() throws ExecutionException, SessionException {
        String clearData = "CLEAR DATA;";

        SessionExecuteSqlResult res = session.executeSql(clearData);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Clear date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
        }
    }

    private void Compare(String actualOutput, String expectedOutput) {
        assertEquals(expectedOutput, actualOutput);
    }

    @Test
    public void queryTimeS() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        for (int i=0; i<4; i++) {
            paths.add(this.path[i]);
        }

        long startTime = 0L;
        long endTime = 101L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime, null, TimePrecision.S);
        dataSet.print();

        long[] timeList = dataSet.getKeys();
        for (int i=0; i<4; i++) {
            if (timeList[i] == 100 && i == 0 || timeList[i] == 100000 && i == 1 || timeList[i] == 100000000 && i == 2 || timeList[i] == 100000000000L && i == 3) {}
            else
                fail();
        }
    }

    @Test
    public void queryTimeMS() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        for (int i=0; i<4; i++) {
            paths.add(this.path[i]);
        }

        long startTime = 0L;
        long endTime = 101L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime, null, TimePrecision.MS);
        dataSet.print();

        long[] timeList = dataSet.getKeys();
        for (int i=0; i<4; i++) {
            if (timeList.length <= i || timeList[i] == 100 && i == 0 || timeList[i] == 100000 && i == 1 || timeList[i] == 100000000 && i == 2) {}
            else
                fail();
        }
    }

    @Test
    public void queryTimeUS() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        for (int i=0; i<4; i++) {
            paths.add(this.path[i]);
        }

        long startTime = 0L;
        long endTime = 101L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime, null, TimePrecision.US);
        dataSet.print();

        long[] timeList = dataSet.getKeys();
        for (int i=0; i<4; i++) {
            if (timeList.length <= i || timeList[i] == 100 && i == 0 || timeList[i] == 100000 && i == 1) {}
            else
                fail();
        }
    }

    @Test
    public void queryTimeNS() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        for (int i=0; i<4; i++) {
            paths.add(this.path[i]);
        }

        long startTime = 0L;
        long endTime = 101L;

        SessionQueryDataSet dataSet = session.queryData(paths, startTime, endTime, null, TimePrecision.NS);
        dataSet.print();

        long[] timeList = dataSet.getKeys();
        for (int i=0; i<4; i++) {
            if (timeList.length <= i || timeList[i] == 100 && i == 0) {}
            else
                fail();
        }
    }

}