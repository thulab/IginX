package cn.edu.tsinghua.iginx.integration;



import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.*;
import cn.edu.tsinghua.iginx.transform.pojo.Task;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import static org.junit.Assert.*;


public class TransformIT {
    private static final double delta = 0.001d;
    private static final Logger logger = LoggerFactory.getLogger(TransformIT.class);

    private static Session session;

    private static final int columnNum = 5;
    private static final String[] columnList = new String[columnNum];

    private static final String SHOW_REGISTER_TASK_SQL = "SHOW REGISTER PYTHON TASK;";
    private static final String REGISTER_SQL_FORMATTER = "REGISTER TRANSFORM PYTHON TASK %s IN %s AS %s";
    private static final String DROP_SQL_FORMATTER = "DROP PYTHON TASK %s";

   private static final String OUTPUT_DIR_PREFIX = System.getProperty("user.dir") + File.separator+ ".." + File.separator +
        "example" + File.separator + "src" + File.separator + "main" + File.separator + "resources";
    private static final String NEW_OUTPUT_DIR_PREFIX = OUTPUT_DIR_PREFIX + File.separator + "transformer";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 10L;
    private static final int LEN = (int)(END_TIMESTAMP - START_TIMESTAMP + 1);

    private static final Object[] valuesList = new Object[LEN];
    private static final List<DataType> dataTypeList = new ArrayList<>();

    private static final Map<String, String> TASK_MAP = new HashMap<>();
    static {
        TASK_MAP.put("\"RowSumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_row_sum.py\"");
        TASK_MAP.put("\"AddOneTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py\"");
        TASK_MAP.put("\"SumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_sum.py\"");
        TASK_MAP.put("\"AvgTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_avg.py\"");
        TASK_MAP.put("\"CountTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_count.py\"");
        TASK_MAP.put("\"MaxTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_max.py\"");
        TASK_MAP.put("\"MinTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_min.py\"");
        TASK_MAP.put("\"AbsTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_abs.py\"");
        TASK_MAP.put("\"AcosTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_acos.py\"");
        TASK_MAP.put("\"AsinTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_asin.py\"");
        TASK_MAP.put("\"AtanTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_atan.py\"");
        TASK_MAP.put("\"Atan2Transformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_atan2.py\"");
        TASK_MAP.put("\"CeilTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_ceil.py\"");
        TASK_MAP.put("\"CosTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_cos.py\"");
        TASK_MAP.put("\"CumulativeSumTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_cumulative_sum.py\"");
        TASK_MAP.put("\"DerivativeTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_derivative.py\"");
        TASK_MAP.put("\"DifferenceTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_difference.py\"");
        TASK_MAP.put("\"ElapsedTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_elapsed.py\"");
        TASK_MAP.put("\"ExpTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_exp.py\"");
        TASK_MAP.put("\"FirstTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_first.py\"");
        TASK_MAP.put("\"FloorTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_floor.py\"");
        TASK_MAP.put("\"IntegralTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_integral.py\"");
        TASK_MAP.put("\"LastTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_last.py\"");
        TASK_MAP.put("\"LnTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_ln.py\"");
        TASK_MAP.put("\"Log2Transformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_log2.py\"");
        TASK_MAP.put("\"Log10Transformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_log10.py\"");
        TASK_MAP.put("\"MedianTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_median.py\"");
        TASK_MAP.put("\"NonNegativeDifferenceTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_nonnegative_difference.py\"");
        TASK_MAP.put("\"NonNegativeDerivativeTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_nonnegative_derivative.py\"");
        TASK_MAP.put("\"RoundTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_round.py\"");
        TASK_MAP.put("\"SinTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_sin.py\"");
        TASK_MAP.put("\"SqrtTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_sqrt.py\"");
        TASK_MAP.put("\"StddevTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_stddev.py\"");
        TASK_MAP.put("\"TanTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + File.separator + "transformer_tan.py\"");
    }


    @BeforeClass
    public static void before() throws SessionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();
        // 删除可能存在的相关结果输出文件
        try {
            session.deleteColumns(Collections.singletonList("*"));
            prepareData();
            registerTask();
        } catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    @AfterClass
    public static void after() throws ExecutionException, SessionException {
        // 注销任务
        dropTask();

        // 清除数据
        //session.deleteColumns(Collections.singletonList("*"));
        // 关闭 Session
        session.closeSession();
    }


    private static void registerTask() {
        TASK_MAP.forEach((k, v) -> {
            String registerSQL = String.format(REGISTER_SQL_FORMATTER, k, v, k);
            try {
                SessionExecuteSqlResult res = session.executeSql(registerSQL);
                if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
                    logger.error("register in key {} execute fail. Caused by: {}.", k, res.getParseErrorMsg());
                    fail();
                } else {
                    logger.info("register in key {} finished", k);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    private static void dropTask() {
        TASK_MAP.forEach((k, v) -> {
            String registerSQL = String.format(DROP_SQL_FORMATTER, k);
            try {
                session.executeSql(registerSQL);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    public static String generateQuerySql(boolean hasTime, boolean isAll, List<Integer> cols, boolean hasTimeStamp,
                                          List<Integer> timeStamp){
        StringBuilder sb = new StringBuilder("select");
        if(hasTime){
            sb.append(" time,");
        }
        if(isAll) {
            for (int i = 0; i < columnNum; i++) {
                sb.append(" value" + i);
                if (i != columnNum - 1) {
                    sb.append(",");
                }
            }
        } else {
            int len = cols.size();
            for (int i = 0; i < len; i++) {
                sb.append(" value" + i);
                if (i != len - 1) {
                    sb.append(",");
                }
            }
        }
        sb.append(" from transform");
        if(hasTimeStamp){
            sb.append(" where time >= " + timeStamp.get(0) + " and time <= " + timeStamp.get(1));
        }
        sb.append(";");
        return sb.toString();
    }

    @Test
    public void absTest()  {
        // 构造任务
        String key = "AbsTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = aim > 0 ? aim : -aim;
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void acosTest(){
        // 构造任务
        String key = "AcosTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.acos(aim);
                        if (Double.isNaN(res)) {
                            assertTrue(Double.isNaN((double) row[j]));
                        } else {
                            assertEquals((double) row[j], res, delta);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void asinTest(){
        // 构造任务
        String key = "AsinTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.asin(aim);
                        if (Double.isNaN(res)) {
                            assertTrue(Double.isNaN((double) row[j]));
                        } else {
                            assertEquals((double) row[j], res, delta);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void atanTest() {
        // 构造任务
        String key = "AtanTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.atan(aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void atan2Test() {
        String key = "Atan2Transformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = new ArrayList<>();
        for(int i = 0; i < columnNum; i++){
            for(int j = 0; j < columnNum; j++){
                //special sql to get 2 diffferent lines
                if(i != j) {
                    List<Integer> cols = new ArrayList<>();
                    cols.add(i);
                    cols.add(j);
                    TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
                    iginxTask.setSql(generateQuerySql(false, false, null, false, null));
                    taskInfoList.add(iginxTask);

                    TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
                    pyTask.setPyTaskName(key);
                    taskInfoList.add(pyTask);
                    try {
                        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                        waitUntilJobFinish(jobId);
                    } catch (Exception e) {
                        logger.error("Error in transform test {} : {}", key, e);
                        fail();
                    }
                }
            }
        }

        Object[] data = getDataFromFile(aimFileName);

        assertEquals(data.length, columnNum * (columnNum - 1) * LEN);
        int count = 0;
        for(int y = 0; y < columnNum; y++) {
            for(int x = 0; x < columnNum; x++) {
                if(y != x) {
                    for (int i = 0; i < LEN; i++) {
                        Object[] row = (Object[]) data[count * LEN + i];
                        assertEquals(row.length, 2);
                        assertEquals((double)row[0], i, 0);
                        if (isNull(i, x) || isNull(i, y) || Math.abs((double)getInsertData(i, x)) < delta) {
                            assertTrue(Double.isNaN((double) row[1]));
                        } else {
                            double aim = (double) getInsertData(i, y) / (double) getInsertData(i, x);
                            double res = Math.atan(aim);
                            assertEquals((double) row[1], res, delta);
                        }
                    }
                    count++;
                }
            }
        }
    }

    @Test
    public void ceilTest() {
        String key = "CeilTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.ceil(aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void cosTest() {
        String key = "CosTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.cos(aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void countTest() {
        String key = "CountTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        Object[] row = (Object[])data[0];
        assertEquals(row.length, columnNum);
        for(int i = 0; i < columnNum; i++){
            int count = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    count++;
                }
            }
            assertEquals((double)row[i], count, delta);
        }
    }

    @Test
    public void cumulativeSumTest() {
        String key = "CumulativeSumTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            double[] totalSum = new double[columnNum];
            for (int j = 0; j < columnNum + 1; j++) {
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (!isNull(i, j - 1)) {
                        totalSum[j - 1] += (double) getInsertData(i, j - 1);
                    }
                    assertEquals((double) row[j], totalSum[j - 1], delta);
                }
            }
        }
    }

    @Test
    public void derivativeTest() {
        String key = "DerivativeTransformer";
        String aimFileName = getAimFileName(key);
        for(int i = 0; i < columnNum; i++) {
            List<TaskInfo> taskInfoList = generateBatchSingleTransformList(key, i);
            try {
                long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                waitUntilJobFinish(jobId);
            } catch (Exception e) {
                logger.error("Error in transform test {} : {}", key, e);
                fail();
            }
        }
        Object[] data = getDataFromFile(aimFileName);
        int count = 0;
        for(int i = 0; i < columnNum; i++){
            int currTimeStamp = -1;
            double currNum = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    if(currTimeStamp != -1){
                        Object[] row = (Object[])data[count];
                        assertEquals(row.length, 2);
                        assertEquals((double)row[1],
                                ((double)getInsertData(j, i) -currNum) / (j - currTimeStamp), delta);
                        assertEquals((double) row[0], j, delta);
                    }
                    count++;
                    currTimeStamp = j;
                    currNum = (double)getInsertData(j, i);
                }
            }
            if(count <= 1){
                assertEquals(data.length, 1);
                assertTrue(Double.isNaN(((double[]) data[0])[0]));
            } else {
                assertEquals(data.length, count - 1);
            }
        }
    }

    @Test
    public void differenceTest() {
        String key = "DifferenceTransformer";
        String aimFileName = getAimFileName(key);
        for(int i = 0; i < columnNum; i++) {
            List<TaskInfo> taskInfoList = generateBatchSingleTransformList(key, i);
            try {
                long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                waitUntilJobFinish(jobId);
            } catch (Exception e) {
                logger.error("Error in transform test {} : {}", key, e);
                fail();
            }
        }
        Object[] data = getDataFromFile(aimFileName);
        int count = 0;
        for(int i = 0; i < columnNum; i++){
            boolean hasFirst = false;
            double currNum = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    if(hasFirst){
                        Object[] row = (Object[])data[count];
                        assertEquals(row.length, 2);
                        assertEquals((double)row[1],
                                (double)getInsertData(j, i) -currNum, delta);
                        assertEquals((double)row[0], j, delta);
                    }
                    count++;
                    hasFirst = true;
                    currNum = (double)getInsertData(j, i);
                }
            }
            if(count <= 1){
                assertEquals(data.length, 1);
                assertTrue(Double.isNaN(((double[])data[0])[0]));
            } else {
                assertEquals(data.length, count - 1);
            }
        }
    }

    @Test
    public void elapsedTest() {
        String key = "ElapsedTransformer";
        String aimFileName = getAimFileName(key);
        for(int i = 0; i < columnNum; i++) {
            List<TaskInfo> taskInfoList = generateBatchSingleTransformList(key, i);
            try {
                long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                waitUntilJobFinish(jobId);
            } catch (Exception e) {
                logger.error("Error in transform test {} : {}", key, e);
                fail();
            }
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, columnNum);
        int count = 0;
        for(int i = 0; i < columnNum; i++){
            boolean hasFirst = false;
            int currTimeStamp = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    if(hasFirst){
                        Object[] row = (Object[])data[count];
                        assertEquals(row.length, 2);
                        assertEquals((double) row[0], j, delta);
                        assertEquals((double)row[1],
                                j - currTimeStamp, delta);
                    }
                    count++;
                    hasFirst = true;
                    currTimeStamp = j;
                }
            }
            if(count <= 1){
                assertEquals(data.length, 1);
                assertTrue(Double.isNaN(((double[]) data[0])[0]));
            } else {
                assertEquals(data.length, count - 1);
            }
        }
    }

    @Test
    public void expTest() {
        String key = "ExpTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.pow(Math.E, aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void firstTest() {
        String key = "FirstTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        for(int i = 0; i < columnNum; i++){
            Object[] row = (Object[])data[0];
            boolean isOk = false;
            for (int j = 0; j < LEN; j++) {
                if (!isOk && !isNull(j, i)) {
                    assertEquals((double) row[i], (double)getInsertData(j, i), delta);
                    isOk = true;
                    break;
                }
            }
            if(!isOk){
                assertTrue(Double.isNaN((double)row[i]));
            }
        }
    }

    @Test
    public void floorTest() {
        String key = "FloorTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.floor(aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void integralTest() {
        String key = "IntegralTransformer";
        String aimFileName = getAimFileName(key);
        for(int i = 0; i < columnNum; i++) {
            List<TaskInfo> taskInfoList = generateBatchTransformList(key);
            try {
                long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                waitUntilJobFinish(jobId);
            } catch (Exception e) {
                logger.error("Error in transform test {} : {}", key, e);
                fail();
            }
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        Object[] row = (Object[])data[0];
        assertEquals(row.length, columnNum);
        for(int i = 0; i < columnNum; i++){
            int count = 0;
            int currTimeStamp = -1;
            double currNum = 0;
            double sum = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    if(currTimeStamp != -1){
                        sum += (Math.abs((double)getInsertData(j, i)) +
                                Math.abs(currNum)) / 2 * (j - currTimeStamp);
                    }
                    currTimeStamp = j;
                    currNum = (double)getInsertData(j, i);
                    count++;
                }
            }
            if(count <= 1){
                assertTrue(Double.isNaN((double)row[i]));
            } else {
                assertEquals((double)row[i], sum, delta);
            }

        }
    }

    @Test
    public void lastTest() {
        String key = "LastTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        for(int i = 0; i < columnNum; i++){
            Object[] row = (Object[])data[0];
            boolean isOk = false;
            for (int j = (int)(END_TIMESTAMP - START_TIMESTAMP); j >= 0; j--) {
                if (!isOk && !isNull(j, i)) {
                    assertEquals((double) row[i], (double)getInsertData(j, i), delta);
                    isOk = true;
                    break;
                }
            }
            if(!isOk){
                assertTrue(Double.isNaN((double)row[i]));
            }
        }
    }

    @Test
    public void lnTest() {
        String key = "LnTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = aim <= 0 ? Double.NaN : Math.log(aim);
                        if (Double.isNaN(res)) {
                            assertTrue(Double.isNaN((double) row[j]));
                        } else {
                            assertEquals((double) row[j], res, delta);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void log2Test() {
        String key = "Log2Transformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = aim <= 0 ? Double.NaN : Math.log(aim) / Math.log(2);
                        if (Double.isNaN(res)) {
                            assertTrue(Double.isNaN((double) row[j]));
                        } else {
                            assertEquals((double) row[j], res, delta);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void log10Test() {
        String key = "Log10Transformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = aim <= 0 ? Double.NaN : Math.log10(aim);
                        if (Double.isNaN(res)) {
                            assertTrue(Double.isNaN((double) row[j]));
                        } else {
                            assertEquals((double) row[j], res, delta);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void maxTest(){
        String key = "MaxTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        for(int i = 0; i < columnNum; i++){
            Object[] row = (Object[])data[0];
            boolean isOk = false;
            double max = Double.NEGATIVE_INFINITY;
            for (int j = 0; j < LEN; j++) {
                if (!isNull(j, i)) {
                    if ((double)getInsertData(j, i) > max){
                        max = (double)getInsertData(j, i);
                    }
                    isOk = true;
                }
            }
            if(!isOk){
                assertTrue(Double.isNaN((double)row[i]));
            } else {
                assertEquals((double)row[i], max, delta);
            }
        }
    }

    @Test
    public void medianTest() {
        //TODO
        String key = "MedianTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        Object[] row = (Object[])data[0];
        assertEquals(row.length, columnNum);
        for(int i = 0; i < columnNum; i++){
            int count = 0;
            List<Double> d = new ArrayList<>();
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    count++;
                    d.add((double)getInsertData(j, i));
                }
            }
            double[] dlist = new double[d.size()];
            for(int j = 0; j < d.size(); j++){
                dlist[j] = d.get(j);
            }
            Arrays.sort(dlist);
            if(count == 0) {
                assertTrue(Double.isNaN((double)row[i]));
            } else if(count % 2 != 0) {
                assertEquals((double) row[i], dlist[count / 2], delta);
            } else {
                assertEquals((double)row[i], (dlist[count / 2] + dlist[count / 2 - 1]) / 2, delta);
            }
        }

    }

    @Test
    public void minTest(){
        String key = "MinTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        for(int i = 0; i < columnNum; i++){
            Object[] row = (Object[])data[0];
            boolean isOk = false;
            double min = Double.POSITIVE_INFINITY;
            for (int j = 0; j < LEN; j++) {
                if (!isNull(j, i)) {
                    if ((double)getInsertData(j, i) < min){
                        min = (double)getInsertData(j, i);
                    }
                    isOk = true;
                }
            }
            if(!isOk){
                assertTrue(Double.isNaN((double)row[i]));
            } else {
                assertEquals((double)row[i], min, delta);
            }
        }
    }

    @Test
    public void nonnegativeDerivativeTest() {
        String key = "NonNegativeDerivativeTransformer";
        String aimFileName = getAimFileName(key);
        for(int i = 0; i < columnNum; i++) {
            List<TaskInfo> taskInfoList = generateBatchSingleTransformList(key, i);
            try {
                long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                waitUntilJobFinish(jobId);
            } catch (Exception e) {
                logger.error("Error in transform test {} : {}", key, e);
                fail();
            }
        }
        Object[] data = getDataFromFile(aimFileName);
        int count = 0;
        for(int i = 0; i < columnNum; i++){
            int currTimeStamp = -1;
            double currNum = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    if(currTimeStamp != -1){
                        Object[] row = (Object[])data[count];
                        assertEquals(row.length, 2);
                        assertEquals((double)row[1],
                                Math.abs(((double)getInsertData(j, i) -currNum) / (j - currTimeStamp)),
                                delta);
                        assertEquals((double) row[0], j, delta);
                    }
                    count++;
                    currTimeStamp = j;
                    currNum = (double)getInsertData(j, i);
                }
            }
            if(count <= 1){
                assertEquals(data.length, 1);
                assertTrue(Double.isNaN(((double[]) data[0])[0]));
            } else {
                assertEquals(data.length, count - 1);
            }
        }
    }

    @Test
    public void nonnegativeDifferenceTest() {
        String key = "NonNegativeDifferenceTransformer";
        String aimFileName = getAimFileName(key);
        for(int i = 0; i < columnNum; i++) {
            List<TaskInfo> taskInfoList = generateBatchSingleTransformList(key, i);
            try {
                long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
                waitUntilJobFinish(jobId);
            } catch (Exception e) {
                logger.error("Error in transform test {} : {}", key, e);
                fail();
            }
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, columnNum);
        int count = 0;
        for(int i = 0; i < columnNum; i++) {
            boolean hasFirst = false;
            double currNum = 0;
            for (int j = 0; j < LEN; j++) {
                if(!isNull(j, i)) {
                    if(hasFirst){
                        Object[] row = (Object[]) data[count];
                        assertEquals(row.length, 2);
                        assertEquals((double) row[1],
                                Math.abs((double)getInsertData(j, i) - currNum),
                                delta);
                        assertEquals((double) row[0], j, delta);
                    }
                    count++;
                    hasFirst = true;
                    currNum = (double) getInsertData(j, i);
                }
            }
            if (count <= 1) {
                assertEquals(data.length, 1);
                assertTrue(Double.isNaN(((double[]) data[0])[0]));
            } else {
                assertEquals(data.length, count - 1);
            }
        }
    }


    @Test
    public void roundTest() {
        String key = "RoundTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = new BigDecimal(aim).setScale(0, RoundingMode.HALF_EVEN).doubleValue();
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }


    @Test
    public void sinTest() {
        String key = "SinTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);

        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.sin(aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }

    }


    @Test
    public void sqrtTest() {
        String key = "SqrtTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.sqrt(aim);
                        if (Double.isNaN(res)) {
                            assertTrue(Double.isNaN((double) row[j]));
                        } else {
                            assertEquals((double) row[j], res, delta);
                        }
                    }
                }
            }
        }
    }

    @Test
    public void stddevTest() {
        String key = "StddevTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        Object[] row = (Object[])data[0];
        for(int i = 0; i < columnNum; i++){
            int count = 0;
            double totalSum = 0;
            double totalSquare = 0;
            for (int j = 0; j < LEN; j++) {
                if (!isNull(j, i)) {
                    count++;
                    totalSum += (double) getInsertData(j, i);
                }
            }
            double avg = totalSum / (double)count;
            for (int j = 0; j < LEN; j++) {
                if (!isNull(j, i)) {
                    totalSquare += Math.pow((double) getInsertData(j, i) - avg, 2);
                }
            }
            assertEquals((double) row[i] * (double)(row[i]), totalSquare / (count - 1), delta);
        }
    }

    @Test
    public void sumTest() {
        String key = "SumTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateBatchTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
        Object[] data = getDataFromFile(aimFileName);
        assertEquals(data.length, 1);
        Object[] row = (Object[])data[0];
        for(int i = 0; i < columnNum; i++){
            double totalSum = 0;
            for (int j = 0; j < LEN; j++) {
                if (!isNull(j, i)) {
                    totalSum += (double) getInsertData(j, i);
                }
            }
            assertEquals((double) row[i], totalSum, delta);
        }
    }

    @Test
    public void tanTest() {
        String key = "TanTransformer";
        String aimFileName = getAimFileName(key);
        List<TaskInfo> taskInfoList = generateSimpleTransformList(key);
        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, aimFileName);
            waitUntilJobFinish(jobId);
        } catch (Exception e){
            logger.error("Error in transform test {} : {}", key, e);
            fail();
        }
       Object[] data = getDataFromFile(aimFileName);

        assertEquals(data.length, LEN);
        for(int i = 0; i < LEN; i++){
            Object[] row = (Object[])data[i];
            assertEquals(row.length, columnNum + 1);
            for(int j = 0; j < columnNum + 1; j++){
                if(j == 0){
                    assertEquals((double)row[j], i, delta);
                } else {
                    if (isNull(i, j - 1)) {
                        assertTrue(Double.isNaN((double) row[j]));
                    } else {
                        double aim = (double) getInsertData(i, j - 1);
                        double res = Math.tan(aim);
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    private static Object insertValue(long timestamp, int num){
        switch (num){
            case 0:
                return timestamp;
            case 1:
                if(timestamp % 2 != 0){
                    return timestamp / 2;
                } else {
                    return null;
                }
            case 2:
                if(timestamp <= 100){
                    return -1L;
                } else {
                    return timestamp - 200;
                }
            case 3:
                return -(double)(timestamp) / 10;
            case 4:
                if(timestamp % 4 == 0){
                    return -1.5;
                } else if(timestamp % 2 != 0){
                    return null;
                } else {
                    return (double)timestamp + (double)(timestamp) / 20;
                }
            default:
                return null;
        }
    }

    private static String getAimFileName(String key){
        return OUTPUT_DIR_PREFIX + File.separator + "export_file_" +
                key.substring(0, key.length() - 11) + ".txt";
    }

    private static List<TaskInfo> generateSimpleTransformList(String key){
        List<TaskInfo> taskInfoList = new ArrayList<>();
        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSql(generateQuerySql(false, true, null, false,null));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName(key);
        taskInfoList.add(pyTask);
        return taskInfoList;
    }

    private static List<TaskInfo> generateBatchTransformList(String key){
        List<TaskInfo> taskInfoList = new ArrayList<>();
        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSql(generateQuerySql(false, true, null, false,null));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
        pyTask.setPyTaskName(key);
        taskInfoList.add(pyTask);
        return taskInfoList;
    }


    private static List<TaskInfo> generateBatchSingleTransformList(String key, int col){
        List<TaskInfo> taskInfoList = new ArrayList<>();
        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        List<Integer> cols = new ArrayList<>();
        cols.add(col);
        iginxTask.setSql(generateQuerySql(false, false, cols, false,null));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
        pyTask.setPyTaskName(key);
        taskInfoList.add(pyTask);
        return taskInfoList;
    }

    private static void prepareData() throws ExecutionException, SessionException {
        for(int i = 0; i < columnNum; i++){
            columnList[i] = "transform.value" + i;
        }
        List<String> paths = new ArrayList<>();
        for(int i = 0; i < columnNum; i++) {
            paths.add(columnList[i]);
        }

        int size = (int) (LEN);
        long[] timestamps = new long[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = START_TIMESTAMP + i;
            Object[] values = new Object[columnNum];
            for (int j = 0; j < columnNum; j++) {
                values[j] = insertValue(i, j);
            }
            valuesList[(int) i] = values;
        }

        for (int i = 0; i < 3; i++) {
            dataTypeList.add(DataType.LONG);
        }
        for (int i = 3; i < columnNum; i++) {
            dataTypeList.add(DataType.DOUBLE);
        }

        session.insertNonAlignedRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static Object getInsertData(int row, int column){
        Object[] rowArr = (Object[])valuesList[row];
        return ((Number)rowArr[column]).doubleValue();
    }

    private static Object[] getDataFromFile(String fileName){
        List<Object> res = new ArrayList<>();
        File file = new File(fileName);
        try {
            if (file.exists() && file.isFile()) {
                FileReader f = new FileReader(file);
                BufferedReader b = new BufferedReader(f);
                String str;
                while ((str = b.readLine()) != null) {
                    String[] tmp = str.split(",");
                    int resLen = tmp.length;
                    Object[] tmpObj = new Object[resLen];
                    for(int i = 0; i < resLen; i++){
                        String value = tmp[i].trim();
                        if(value.equals("NaN")){
                            tmpObj[i] = Double.NaN;
                        } else {
                            tmpObj[i] = Double.parseDouble(value);
                        }
                    }
                    res.add(tmpObj);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return res.toArray();
    }

    private static void waitUntilJobFinish(long jobId) throws ExecutionException, SessionException, InterruptedException {
        int count = 0;
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            count++;
            if(count >= 20){
                logger.error("Timeout in job {}", jobId);
                fail();
                break;
            }            
            Thread.sleep(200);
            jobState = session.queryTransformJobStatus(jobId);
        }
    }

    private static boolean isNull(int row, int col){
        if((col == 1 && row % 2 == 0) || (col == 4 && row % 2 != 0) || col == 5){
            return true;
        } else {
            return false;
        }
    }
}

