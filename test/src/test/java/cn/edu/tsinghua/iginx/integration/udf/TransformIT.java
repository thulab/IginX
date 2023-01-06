/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.integration.udf;

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.exceptions.SessionException;
import cn.edu.tsinghua.iginx.session.Session;
import cn.edu.tsinghua.iginx.session.SessionExecuteSqlResult;
import cn.edu.tsinghua.iginx.thrift.DataFlowType;
import cn.edu.tsinghua.iginx.thrift.ExportType;
import cn.edu.tsinghua.iginx.thrift.JobState;
import cn.edu.tsinghua.iginx.thrift.RegisterTaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskInfo;
import cn.edu.tsinghua.iginx.thrift.TaskType;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TransformIT {

    private static final Logger logger = LoggerFactory.getLogger(TransformIT.class);

    private static Session session;

    private static final String OUTPUT_DIR_PREFIX = System.getProperty("user.dir") + File.separator
            + "src" + File.separator + "test" + File.separator + "resources" + File.separator + "transform";

    private static final long START_TIMESTAMP = 0L;

    private static final long END_TIMESTAMP = 15000L;

    private static final String SHOW_REGISTER_TASK_SQL = "SHOW REGISTER PYTHON TASK;";

    private static final String DROP_SQL_FORMATTER = "DROP PYTHON TASK \"%s\"";

    private static final String REGISTER_SQL_FORMATTER = "REGISTER TRANSFORM PYTHON TASK \"%s\" IN \"%s\" AS \"%s\"";

    private static final String COMMIT_SQL_FORMATTER = "COMMIT TRANSFORM JOB \"%s\"";

    private static final String SHOW_TIME_SERIES_SQL = "SHOW TIME SERIES;";

    private static final String QUERY_SQL_1 = "SELECT s2 FROM us.d1 WHERE key >= 14800;";

    private static final String QUERY_SQL_2 = "SELECT s1, s2 FROM us.d1 WHERE key < 200;";

    private static final Map<String, String> TASK_MAP = new HashMap<>();
    static {
        TASK_MAP.put("RowSumTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_row_sum.py");
        TASK_MAP.put("AddOneTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_add_one.py");
        TASK_MAP.put("SumTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_sum.py");
        TASK_MAP.put("SleepTransformer", OUTPUT_DIR_PREFIX + File.separator + "transformer_sleep.py");
    }

    @BeforeClass
    public static void setUp() {
        session = new Session("127.0.0.1", 6888, "root", "root");
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
        String insertStrPrefix = "INSERT INTO us.d1 (key, s1, s2, s3, s4) values ";
        StringBuilder builder = new StringBuilder(insertStrPrefix);
        int size = (int) (END_TIMESTAMP - START_TIMESTAMP);

        for (int i = 0; i < size; i++) {
            builder.append(", ");
            builder.append("(");
            builder.append(START_TIMESTAMP + i).append(", ");
            builder.append(i).append(", ");
            builder.append(i + 1).append(", ");
            builder.append("\"").append(new String(RandomStringUtils.randomAlphanumeric(10).getBytes())).append("\", ");
            builder.append((i + 0.1));
            builder.append(")");
        }
        builder.append(";");

        String insertStatement = builder.toString();
        SessionExecuteSqlResult res = session.executeSql(insertStatement);
        if (res.getParseErrorMsg() != null && !res.getParseErrorMsg().equals("")) {
            logger.error("Insert date execute fail. Caused by: {}.", res.getParseErrorMsg());
            fail();
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

    private void dropTask(String task) throws SessionException, ExecutionException {
        SessionExecuteSqlResult result = session.executeSql(SHOW_REGISTER_TASK_SQL);
        for (RegisterTaskInfo info : result.getRegisterTaskInfos()) {
            if (info.getClassName().equals(task)) {
                session.executeSql(String.format(DROP_SQL_FORMATTER, task));
            }
        }
    }

    private void registerTask(String task) throws SessionException, ExecutionException, InterruptedException {
        dropTask(task);
        session.executeSql(String.format(
                REGISTER_SQL_FORMATTER, task, TASK_MAP.get(task), task
        ));
    }

    private void verifyJobState(long jobId) throws SessionException, ExecutionException, InterruptedException {
        logger.info("job is {}", jobId);
        JobState jobState = JobState.JOB_CREATED;
        while (!jobState.equals(JobState.JOB_CLOSED) && !jobState.equals(JobState.JOB_FAILED) && !jobState.equals(JobState.JOB_FINISHED)) {
            Thread.sleep(500);
            jobState = session.queryTransformJobStatus(jobId);
        }
        logger.info("job {} state is {}", jobId, jobState.toString());
        assertEquals(JobState.JOB_FINISHED, jobState);

        List<Long> finishedJobIds = session.showEligibleJob(JobState.JOB_FINISHED);
        assertTrue(finishedJobIds.contains(jobId));
    }

    @Test
    public void commitSingleSqlStatementTest() {
        logger.info("commitSingleSqlStatementTest");
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        iginxTask.setSqlList(Collections.singletonList(SHOW_TIME_SERIES_SQL));
        taskInfoList.add(iginxTask);

        try {
            long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");

            verifyJobState(jobId);
        } catch (SessionException | ExecutionException | InterruptedException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitSingleSqlStatementByYamlTest() {
        logger.info("commitSingleSqlStatementByYamlTest");
        try {
            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformSingleSqlStatement.yaml";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));

            long jobId = result.getJobId();
            verifyJobState(jobId);
        } catch (SessionException | ExecutionException | InterruptedException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitMultipleSqlStatementsTest() {
        logger.info("commitMultipleSqlStatementsTest");
        List<TaskInfo> taskInfoList = new ArrayList<>();

        TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
        List<String> sqlList = new ArrayList<>();
        String insertStrPrefix = "INSERT INTO us.d1 (key, s2) values ";
        StringBuilder builder = new StringBuilder(insertStrPrefix);
        for (int i = 0; i < 100; i++) {
            builder.append("(");
            builder.append(END_TIMESTAMP + i).append(", ");
            builder.append(END_TIMESTAMP + i + 1);
            builder.append(")");
        }
        builder.append(";");
        sqlList.add(builder.toString());
        sqlList.add(QUERY_SQL_1);
        iginxTask.setSqlList(sqlList);
        taskInfoList.add(iginxTask);

        try {
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_sql_statements.txt";
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

            verifyJobState(jobId);
            verifyMultipleSqlStatements(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitMultipleSqlStatementsByYamlTest() {
        logger.info("commitMultipleSqlStatementsByYamlTest");
        try {
            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMultipleSqlStatements.yaml";
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_sql_statements_by_yaml.txt";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
            long jobId = result.getJobId();

            verifyJobState(jobId);
            verifyMultipleSqlStatements(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    private void verifyMultipleSqlStatements(String outputFileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
        String line = reader.readLine();
        String[] parts = line.split(",");

        assertEquals(GlobalConstant.KEY_NAME, parts[0]);
        assertEquals("us.d1.s2", parts[1]);

        int index = 0;
        while ((line = reader.readLine()) != null) {
            parts = line.split(",");
            assertEquals(14800 + index, Long.parseLong(parts[0]));
            assertEquals(14800 + index + 1, Long.parseLong(parts[1]));
            index++;
        }
        reader.close();

        assertEquals(300, index);
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
    }

    @Test
    public void commitSinglePythonJobTest() {
        logger.info("commitSinglePythonJobTest");
        try {
            String task = "RowSumTransformer";
            registerTask(task);

            List<TaskInfo> taskInfoList = new ArrayList<>();

            TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
            iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

            TaskInfo pyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
            pyTask.setPyTaskName("RowSumTransformer");

            taskInfoList.add(iginxTask);
            taskInfoList.add(pyTask);

            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_single_python_job.txt";
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

            verifyJobState(jobId);
            verifySinglePythonJob(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitSinglePythonJobByYamlTest() {
        logger.info("commitSinglePythonJobByYamlTest");
        try {
            String task = "RowSumTransformer";
            registerTask(task);

            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformSinglePythonJob.yaml";
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_single_python_job_by_yaml.txt";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
            long jobId = result.getJobId();

            verifyJobState(jobId);
            verifySinglePythonJob(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    private void verifySinglePythonJob(String outputFileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
        String line = reader.readLine();
        String[] parts = line.split(",");

        assertEquals(GlobalConstant.KEY_NAME, parts[0]);
        assertEquals("sum", parts[1]);

        int index = 0;
        while ((line = reader.readLine()) != null) {
            parts = line.split(",");
            assertEquals(index, Long.parseLong(parts[0]));
            assertEquals(index + index + 1, Long.parseLong(parts[1]));
            index++;
        }
        reader.close();

        assertEquals(200, index);
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
    }

    @Test
    public void commitMultiplePythonJobsTest() {
        logger.info("commitMultiplePythonJobsTest");
        try {
            String[] taskList = {"RowSumTransformer", "AddOneTransformer"};
            for (String task : taskList) {
                registerTask(task);
            }

            List<TaskInfo> taskInfoList = new ArrayList<>();

            TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
            iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

            TaskInfo addOnePyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
            addOnePyTask.setPyTaskName("AddOneTransformer");

            TaskInfo rowSumPyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
            rowSumPyTask.setPyTaskName("RowSumTransformer");

            taskInfoList.add(iginxTask);
            taskInfoList.add(addOnePyTask);
            taskInfoList.add(rowSumPyTask);

            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_python_jobs.txt";
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

            verifyJobState(jobId);
            verifyMultiplePythonJobs(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitMultiplePythonJobsByYamlTest() {
        logger.info("commitMultiplePythonJobsByYamlTest");
        try {
            String[] taskList = {"RowSumTransformer", "AddOneTransformer"};
            for (String task : taskList) {
                registerTask(task);
            }

            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMultiplePythonJobs.yaml";
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_python_jobs_by_yaml.txt";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
            long jobId = result.getJobId();

            verifyJobState(jobId);
            verifyMultiplePythonJobs(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitMultiplePythonJobsByYamlWithExportToIginxTest() {
        logger.info("commitMultiplePythonJobsByYamlWithExportToIginxTest");
        try {
            String[] taskList = {"RowSumTransformer", "AddOneTransformer"};
            for (String task : taskList) {
                registerTask(task);
            }

            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMultiplePythonJobsWithExportToIginx.yaml";
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_multiple_python_jobs_by_yaml_with_export_to_iginx.txt";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
            long jobId = result.getJobId();

            verifyJobState(jobId);

            SessionExecuteSqlResult queryResult = session.executeSql("SELECT * FROM transform;");
            int timeIndex = queryResult.getPaths().indexOf("transform.key");
            int sumIndex = queryResult.getPaths().indexOf("transform.sum");
            assertNotEquals(-1, timeIndex);
            assertNotEquals(-1, sumIndex);

            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFileName));
            writer.write("key,sum\n");
            for (List<Object> row : queryResult.getValues()) {
                writer.write(row.get(timeIndex) + "," + row.get(sumIndex) + "\n");
            }
            writer.close();

            verifyMultiplePythonJobs(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    private void verifyMultiplePythonJobs(String outputFileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
        String line = reader.readLine();
        String[] parts = line.split(",");

        assertEquals(GlobalConstant.KEY_NAME, parts[0]);
        assertEquals("sum", parts[1]);

        int index = 0;
        while ((line = reader.readLine()) != null) {
            parts = line.split(",");
            assertEquals(index + 1, Long.parseLong(parts[0]));
            assertEquals(index + 1 + index + 1 + 1, Long.parseLong(parts[1]));
            index++;
        }
        reader.close();

        assertEquals(200, index);
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
    }

    @Test
    public void commitMixedPythonJobsTest() {
        logger.info("commitMixedPythonJobsTest");
        try {
            String[] taskList = {"RowSumTransformer", "AddOneTransformer", "SumTransformer"};
            for (String task : taskList) {
                registerTask(task);
            }

            List<TaskInfo> taskInfoList = new ArrayList<>();

            TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
            iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

            TaskInfo addOnePyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
            addOnePyTask.setPyTaskName("AddOneTransformer");

            TaskInfo sumPyTask = new TaskInfo(TaskType.Python, DataFlowType.Batch);
            sumPyTask.setPyTaskName("SumTransformer");

            TaskInfo rowSumPyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
            rowSumPyTask.setPyTaskName("RowSumTransformer");

            taskInfoList.add(iginxTask);
            taskInfoList.add(addOnePyTask);
            taskInfoList.add(sumPyTask);
            taskInfoList.add(rowSumPyTask);

            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_mixed_python_jobs.txt";
            long jobId = session.commitTransformJob(taskInfoList, ExportType.File, outputFileName);

            verifyJobState(jobId);
            verifyMixedPythonJobs(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitMixedPythonJobsByYamlTest() {
        logger.info("commitMixedPythonJobsByYamlTest");
        try {
            String[] taskList = {"RowSumTransformer", "AddOneTransformer", "SumTransformer"};
            for (String task : taskList) {
                registerTask(task);
            }

            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMixedPythonJobs.yaml";
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_mixed_python_jobs_by_yaml.txt";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
            long jobId = result.getJobId();

            verifyJobState(jobId);
            verifyMixedPythonJobs(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    @Test
    public void commitMixedPythonJobsByYamlWithRegisterTest() {
        logger.info("commitMixedPythonJobsByYamlWithRegisterTest");
        try {
            String[] taskList = {"RowSumTransformer", "AddOneTransformer", "SumTransformer"};
            for (String task : taskList) {
                dropTask(task);
            }

            String yamlFileName = OUTPUT_DIR_PREFIX + File.separator + "TransformMixedPythonJobsWithRegister.yaml";
            String outputFileName = OUTPUT_DIR_PREFIX + File.separator + "export_file_mixed_python_jobs_with_register_by_yaml.txt";
            SessionExecuteSqlResult result = session.executeSql(String.format(COMMIT_SQL_FORMATTER, yamlFileName));
            long jobId = result.getJobId();

            verifyJobState(jobId);
            verifyMixedPythonJobs(outputFileName);
        } catch (SessionException | ExecutionException | InterruptedException | IOException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }

    private void verifyMixedPythonJobs(String outputFileName) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
        String line = reader.readLine();
        String[] parts = line.split(",");

        assertEquals(GlobalConstant.KEY_NAME, parts[0]);
        assertEquals("sum", parts[1]);

        line = reader.readLine();
        parts = line.split(",");
        reader.close();

        assertEquals(20100, Long.parseLong(parts[0]));
        assertEquals(40400, Long.parseLong(parts[1]));
        assertTrue(Files.deleteIfExists(Paths.get(outputFileName)));
    }

    @Test
    public void cancelJobTest() {
        logger.info("cancelJobTest");
        try {
            String task = "SleepTransformer";
            registerTask(task);

            List<TaskInfo> taskInfoList = new ArrayList<>();

            TaskInfo iginxTask = new TaskInfo(TaskType.IginX, DataFlowType.Stream);
            iginxTask.setSqlList(Collections.singletonList(QUERY_SQL_2));

            TaskInfo sleepPyTask = new TaskInfo(TaskType.Python, DataFlowType.Stream);
            sleepPyTask.setPyTaskName("SleepTransformer");

            taskInfoList.add(iginxTask);
            taskInfoList.add(sleepPyTask);

            long jobId = session.commitTransformJob(taskInfoList, ExportType.Log, "");
            logger.info("job is {}", jobId);
            JobState jobState = session.queryTransformJobStatus(jobId);
            logger.info("job {} state is {}", jobId, jobState.toString());

            session.cancelTransformJob(jobId);

            List<Long> finishedJobIds = session.showEligibleJob(JobState.JOB_CLOSED);

            assertTrue(finishedJobIds.contains(jobId));
        } catch (SessionException | ExecutionException | InterruptedException e) {
            logger.error("Transform:  execute fail. Caused by:", e);
            fail();
        }
    }
}
