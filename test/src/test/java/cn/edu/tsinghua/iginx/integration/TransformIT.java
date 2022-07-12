package cn.edu.tsinghua.iginx.integration;



import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final String OUTPUT_DIR_PREFIX = "/root/IginX/example/src/main/resources";
    private static final String NEW_OUTPUT_DIR_PREFIX = "/root/IginX/example/src/main/resources/transformer";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 10L;

    private static final Object[] valuesList = new Object[(int)(END_TIMESTAMP - START_TIMESTAMP + 1)];
    private static final List<DataType> dataTypeList = new ArrayList<>();

    private static final Map<String, String> TASK_MAP = new HashMap<>();
    static {
        TASK_MAP.put("\"RowSumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + '/' + "transformer_row_sum.py\"");
        TASK_MAP.put("\"AddOneTransformer\"", "\"" + OUTPUT_DIR_PREFIX + '/' + "transformer_add_one.py\"");
        TASK_MAP.put("\"SumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + '/' + "transformer_sum.py\"");
        TASK_MAP.put("\"AvgTransformer\"", "\"" + OUTPUT_DIR_PREFIX + '/' + "transformer_avg.py\"");
        TASK_MAP.put("\"AbsTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_abs.py\"");
        TASK_MAP.put("\"AcosTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_acos.py\"");
        TASK_MAP.put("\"AsinTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_asin.py\"");
        TASK_MAP.put("\"AtanTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_atan.py\"");
        TASK_MAP.put("\"Atan2Transformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_atan2.py\"");
        TASK_MAP.put("\"BottomTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_bottom.py\"");
        TASK_MAP.put("\"CeilTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_ceil.py\"");
        TASK_MAP.put("\"CosTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_cos.py\"");
        TASK_MAP.put("\"CumulativeSumTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_cumulative_sum.py\"");
        TASK_MAP.put("\"DerivativeTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_derivative.py\"");
        TASK_MAP.put("\"DifferenceTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_difference.py\"");
        TASK_MAP.put("\"DistinctTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_distinct.py\"");
        TASK_MAP.put("\"ElapsedTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_elapsed.py\"");
        TASK_MAP.put("\"ExpTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_exp.py\"");
        TASK_MAP.put("\"FirstTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_first.py\"");
        TASK_MAP.put("\"FloorTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_floor.py\"");
        TASK_MAP.put("\"IntegralTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_integral.py\"");
        TASK_MAP.put("\"LastTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_last.py\"");
        TASK_MAP.put("\"LnTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_ln.py\"");
        TASK_MAP.put("\"LogTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_log.py\"");
        TASK_MAP.put("\"Log2Transformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_log2.py\"");
        TASK_MAP.put("\"Log10Transformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_log10.py\"");
        TASK_MAP.put("\"MedianTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_median.py\"");
        TASK_MAP.put("\"ModeTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_mode.py\"");
        TASK_MAP.put("\"MovingAverageTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_moving_average.py\"");
        TASK_MAP.put("\"NonNegativeDifferenceTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_nonnegative_difference.py\"");
        TASK_MAP.put("\"NonNegativeDerivativeTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_nonnegative_derivative.py\"");
        TASK_MAP.put("\"PercentileTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_percentile.py\"");
        TASK_MAP.put("\"PowTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_pow.py\"");
        TASK_MAP.put("\"RoundTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_round.py\"");
        TASK_MAP.put("\"SampleTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_sample.py\"");
        TASK_MAP.put("\"SinTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_sin.py\"");
        TASK_MAP.put("\"SpreadTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_spread.py\"");
        TASK_MAP.put("\"SqrtTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_sqrt.py\"");
        TASK_MAP.put("\"StddevTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_stddev.py\"");
        TASK_MAP.put("\"TanTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_tan.py\"");
        TASK_MAP.put("\"TopTransformer\"", "\"" + NEW_OUTPUT_DIR_PREFIX + '/' + "transformer_top.py\"");
    }


    @BeforeClass
    public static void before() throws SessionException, ExecutionException {
        //session = new Session("120.48.93.68", 6888, "root", "root");
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        //client = IginXClientFactory.create("120.48.93.68:6888");

        // 删除可能存在的相关结果输出文件
        for(String key: TASK_MAP.keySet()){
            File file = new File(getAimFileName(key));
            try {
                if (file.exists() && file.isFile()) {
                    file.delete();
                }
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        // 准备数据
        session.deleteColumns(Collections.singletonList("*"));
        prepareData();

        // 注册任务
        registerTask();

    }

    @AfterClass
    public static void after() throws ExecutionException, SessionException {
        // 注销任务
        dropTask();

        // 查询已注册的任务
        SessionExecuteSqlResult result = session.executeSql(SHOW_REGISTER_TASK_SQL);
        result.print(false, "ms");

        // 清除数据
        //session.deleteColumns(Collections.singletonList("*"));
        // 关闭 Session
        session.closeSession();
    }


    private static void registerTask() {
        TASK_MAP.forEach((k, v) -> {
            String registerSQL = String.format(REGISTER_SQL_FORMATTER, k, v, k);
            try {
                session.executeSql(registerSQL);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = aim > 0 ? aim : -aim;
                    assertEquals((double) row[j], res, delta);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.acos(aim);
                    if(Double.isNaN(res)){
                        assertTrue(Double.isNaN((double)row[j]));
                    } else {
                        assertEquals((double) row[j], res, delta);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.asin(aim);
                    if(Double.isNaN(res)){
                        assertTrue(Double.isNaN((double)row[j]));
                    } else {
                        assertEquals((double) row[j], res, delta);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.atan(aim);
                    assertEquals((double) row[j], res, delta);
                }
            }
        }
    }

    @Test
    public void bottomTest() {
        //now only can run n = 1
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.ceil(aim);
                    assertEquals((double) row[j], res, delta);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.cos(aim);
                    assertEquals((double) row[j], res, delta);
                }
            }
        }
    }

    @Test
    public void cumulativeSumTest() {

    }

    @Test
    public void derivativeTest() {

    }

    @Test
    public void differenceTest() {

    }

    @Test
    public void distinctTest() {

    }

    @Test
    public void elapsedTest() {

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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.pow(Math.E, aim);
                    assertEquals((double) row[j], res, delta);
                }
            }
        }
    }

    @Test
    public void firstTest() {

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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.floor(aim);
                    assertEquals((double) row[j], res, delta);
                }
            }
        }
    }

    @Test
    public void integralTest() {

    }

    @Test
    public void lastTest() {

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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = aim <= 0 ? Double.NaN : Math.log(aim);
                    if(Double.isNaN(res)){
                        assertTrue(Double.isNaN((double)row[j]));
                    } else {
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void logTest() {

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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = aim <= 0 ? Double.NaN : Math.log(aim) / Math.log(2);
                    if(Double.isNaN(res)){
                        assertTrue(Double.isNaN((double)row[j]));
                    } else {
                        assertEquals((double) row[j], res, delta);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = aim <= 0 ? Double.NaN : Math.log10(aim);
                    if(Double.isNaN(res)){
                        assertTrue(Double.isNaN((double)row[j]));
                    } else {
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void medianTest() {

    }

    @Test
    public void modeTest() {

    }

    @Test
    public void movingAverageTest() {

    }

    @Test
    public void nonnegativeDerivativeTest() {

    }

    @Test
    public void nonnegativeDifferenceTest() {

    }

    @Test
    public void percentileTest() {

    }

    @Test
    public void powTest() {/*
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

        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i <= END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if((j == 2 && i % 2 == 0) || (j == 4 && i % 2 != 0)){
                    assertNull(row[j]);
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.pow(aim, n);
                    assertEquals((double)row[j], res, delta);
                }
            }
        }*/
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = new BigDecimal(aim).setScale(0, RoundingMode.HALF_EVEN).doubleValue();
                    assertEquals((double) row[j], res, delta);
                }
            }
        }
    }

    @Test
    public void sampleTest() {

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

        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.sin(aim);
                    assertEquals((double) row[j], res, delta);
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
        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.sqrt(aim);
                    if(Double.isNaN(res)){
                        assertTrue(Double.isNaN((double)row[j]));
                    } else {
                        assertEquals((double) row[j], res, delta);
                    }
                }
            }
        }
    }

    @Test
    public void stddevTest() {

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

        assertEquals(data.length, END_TIMESTAMP - START_TIMESTAMP + 1);
        for(int i = 0; i < END_TIMESTAMP - START_TIMESTAMP + 1; i++){
            Object[] row = (Object[])data[i];
            for(int j = 0; j < columnNum; j++){
                if(isNull(i, j)){
                    assertTrue(Double.isNaN((double)row[j]));
                } else {
                    double aim = (double)getInsertData(i, j);
                    double res = Math.tan(aim);
                    assertEquals((double) row[j], res, delta);
                }
            }
        }
    }

    @Test
    public void topTest() {

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
        return OUTPUT_DIR_PREFIX +  '/' + "export_file_" +
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

    private static List<TaskInfo> generateTransformListWithTime(String key, int startTime, int endTime){
        List<TaskInfo> taskInfoList = new ArrayList<>();
        List<Integer> timeList = new ArrayList<>();
        timeList.add(startTime);
        timeList.add(endTime);
        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSql(generateQuerySql(false, true, null, true,timeList));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
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

        int size = (int) (END_TIMESTAMP - START_TIMESTAMP + 1);
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
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(200);
            jobState = session.queryTransformJobStatus(jobId);
        }
    }

    private static boolean isNull(int row, int col){
        if((col == 1 && row % 2 == 0) || (col == 4 && row % 2 != 0)){
            return true;
        } else {
            return false;
        }
    }
}

