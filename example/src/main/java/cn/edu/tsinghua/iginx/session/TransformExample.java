package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.thrift.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransformExample {

    private static Session session;

    private static final String S1 = "transform.value1";
    private static final String S2 = "transform.value2";
    private static final String S3 = "transform.value3";
    private static final String S4 = "transform.value4";

    private static final String SQL = "select value1, value2, value3, value4 from transform;";

    private static final String OUTPUT_DIR_PREFIX = System.getProperty("user.dir") + File.separator +
        "example" + File.separator + "src" + File.separator + "main" + File.separator + "resources";

    private static final long START_TIMESTAMP = 0L;
    private static final long END_TIMESTAMP = 1000L;

    public static void main(String[] args) throws SessionException, ExecutionException, InterruptedException {
        session = new Session("127.0.0.1", 6888, "root", "root");
        // 打开 Session
        session.openSession();

        // 准备数据
        session.deleteColumns(Collections.singletonList("*"));
        prepareData();

        // 查询序列
        SessionExecuteSqlResult result = session.executeSql("show time series");
        result.print(false, "ms");

        // 导出到日志
        commitStdJob();

        // 导出到file
        commitFileJob();

        // 复合任务
        commitCombineJob();

        // 混合任务
        commitMixedJob();

        // 导出到IginX
        commitIginXJob();

        // SQL提交
        commitBySQL();

        // 清除数据
        session.deleteColumns(Collections.singletonList("*"));
        // 关闭 Session
        session.closeSession();
    }

    private static void commitStdJob() throws ExecutionException, SessionException, InterruptedException {
        // 构造任务
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSql(SQL);
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_row_sum");
        pyTask.setClassName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.None, "");

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
        iginxTask.setSql(SQL);
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_row_sum");
        pyTask.setClassName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file.txt");

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
        iginxTask.setSql(SQL);
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_add_one");
        pyTask.setClassName("AddOneTransformer");
        taskInfoList.add(pyTask);

        pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_row_sum");
        pyTask.setClassName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file_combine.txt");

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
        iginxTask.setSql(SQL);
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_add_one");
        pyTask.setClassName("AddOneTransformer");
        taskInfoList.add(pyTask);

        pyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
        pyTask.setFileName("transformer_sum");
        pyTask.setClassName("SumTransformer");
        taskInfoList.add(pyTask);

        pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_row_sum");
        pyTask.setClassName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.File, OUTPUT_DIR_PREFIX + File.separator + "export_file_sum.txt");

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
        iginxTask.setSql(SQL);
        taskInfoList.add(iginxTask);

        TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
        pyTask.setFileName("transformer_row_sum");
        pyTask.setClassName("RowSumTransformer");
        taskInfoList.add(pyTask);

        // 提交任务
        long jobId = session.commitTransformJob(taskInfoList, ExportType.IginX, "");

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
}
