package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientFactory;
import cn.edu.tsinghua.iginx.session_v2.TransformClient;
import cn.edu.tsinghua.iginx.session_v2.domain.Task;
import cn.edu.tsinghua.iginx.session_v2.domain.Transform;
import cn.edu.tsinghua.iginx.thrift.*;

import java.io.File;
import java.util.*;

public class TransformExample {

    private static Session session;
    private static IginXClient client;

    private static final String S1 = "transform.value1";
    private static final String S2 = "transform.value2";
    private static final String S3 = "transform.value3";
    private static final String S4 = "transform.value4";

    private static final String QUERY_SQL = "select value1, value2, value3, value4 from transform;";
    private static final String SHOW_TIME_SERIES_SQL = "show time series;";
    private static final String SHOW_REGISTER_TASK_SQL = "SHOW REGISTER PYTHON TASK;";
    private static final String REGISTER_SQL_FORMATTER = "REGISTER TRANSFORM PYTHON TASK %s IN %s AS %s";
    private static final String DROP_SQL_FORMATTER = "DROP PYTHON TASK %s";

    private static final String OUTPUT_DIR_PREFIX = System.getProperty("user.dir") + File.separator +
        "example" + File.separator + "src" + File.separator + "main" + File.separator + "resources";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 1000L;

    private static final long TIMEOUT = 10000L;

    private static final Map<String, String> TASK_MAP = new HashMap<>();
    static {
        TASK_MAP.put("\"RowSumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_row_sum.py\"");
        TASK_MAP.put("\"AddOneTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py\"");
        TASK_MAP.put("\"SumTransformer\"", "\"" + OUTPUT_DIR_PREFIX + File.separator + "transformer_sum.py\"");
    }

    public static void main(String[] args) throws SessionException, ExecutionException, InterruptedException {
        before();

        // session
        runWithSession();
        // session v2
        runWithSessionV2();

        after();
    }

    private static void before() throws SessionException, ExecutionException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        client = IginXClientFactory.create();

        // 准备数据
        session.deleteColumns(Collections.singletonList("*"));
        prepareData();

        // 查询序列
        SessionExecuteSqlResult result = session.executeSql("show time series");
        result.print(false, "ms");

        // 注册任务
        registerTask();

        // 查询已注册的任务
        result = session.executeSql(SHOW_REGISTER_TASK_SQL);
        result.print(false, "ms");
    }

    private static void after() throws ExecutionException, SessionException {
        // 注销任务
        dropTask();

        // 查询已注册的任务
        SessionExecuteSqlResult result = session.executeSql(SHOW_REGISTER_TASK_SQL);
        result.print(false, "ms");

        // 清除数据
        session.deleteColumns(Collections.singletonList("*"));
        // 关闭 Session
        session.closeSession();
    }

    private static void runWithSession() throws SessionException, ExecutionException, InterruptedException {
        // 直接输出到文件
        commitWithoutPyTask();

        // 导出到日志
        commitStdJob();

        // 导出到file
        commitFileJob();

        // 综合任务
        commitCombineJob();

        // 混合任务
        commitMixedJob();

        // 导出到IginX
        commitIginXJob();

        // SQL提交
        commitBySQL();
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

    private static void commitWithoutPyTask() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(SHOW_TIME_SERIES_SQL));
        taskInfoList.add(iginxTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file_show_ts.txt", Arrays.asList("path", "type"));
        System.out.println("job id is " + jobId);

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }

    private static void commitStdJob() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");
        System.out.println("job id is " + jobId);

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }

    private static void commitFileJob() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file.txt", Arrays.asList("col1", "col2"));
        System.out.println("job id is " + jobId);

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }

    private static void commitCombineJob() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("AddOneTransformer");
        taskInfoList.add(pyTask);

        pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file_combine.txt", Arrays.asList("col1", "col2"));
        System.out.println("job id is " + jobId);

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }

    private static void commitMixedJob() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("AddOneTransformer");
        taskInfoList.add(pyTask);

        pyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
        pyTask.setPyTaskName("SumTransformer");
        taskInfoList.add(pyTask);

        pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file_sum.txt", Arrays.asList("col1", "col2"));
        System.out.println("job id is " + jobId);

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }

    private static void commitIginXJob() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(QUERY_SQL));
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setPyTaskName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.IginX, "", Arrays.asList("col1", "col2"));
        System.out.println("job id is " + jobId);

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());

        // 查询序列
        SessionExecuteSqlResult result = session.executeSql("show time series");
        result.print(false, "ms");
    }

    private static void commitBySQL() throws ExecutionException, SessionException, InterruptedException {
        String yamlPath = "\"" + OUTPUT_DIR_PREFIX + File.separator + "TransformJobExample.yaml\"";
        SessionExecuteSqlResult result = session.executeSql("commit transform job " + yamlPath);

        long jobId = result.getJobId();
        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }

    private static void prepareData() throws ExecutionException, SessionException {
        List<String> paths = new ArrayList<>();
        paths.add(S1);
        paths.add(S2);
        paths.add(S3);
        paths.add(S4);

        int size = (int) (END_TIMESTAMP - START_TIMESTAMP);
        long[] timestamps = new long[size];
        Object[] valuesList = new Object[size];
        for (long i = 0; i < size; i++) {
            timestamps[(int) i] = START_TIMESTAMP + i;
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

        System.out.println("insertRowRecords...");
        session.insertRowRecords(paths, timestamps, valuesList, dataTypeList, null);
    }

    private static void runWithSessionV2() throws InterruptedException {

        TransformClient transformClient = client.getTransformClient();
        long jobId = transformClient.commitTransformJob(
            Transform
                .builder()
                .addTask(
                    Task.builder()
                        .dataFlowType(DataFlowType.Stream)
                        .timeout(TIMEOUT)
                        .sql(QUERY_SQL)
                        .build())
                .addTask(
                    Task.builder()
                        .dataFlowType(DataFlowType.Stream)
                        .timeout(TIMEOUT)
                        .pyTaskName("RowSumTransformer")
                        .build())
                .exportToFile(OUTPUT_DIR_PREFIX + File.separator + "export_file_v2.txt", Arrays.asList("col1", "col2"))
                .build()
        );

        // 轮询查看任务情况
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = transformClient.queryTransformJobStatus(jobId);
        }
        System.out.println("job state is " + jobState.toString());
    }
}
