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
package cn.edu.tsinghua.iginx.transform;

import cn.edu.tsinghua.iginx.utils.JobFromYAML;
import cn.edu.tsinghua.iginx.utils.TaskFromYAML;
import cn.edu.tsinghua.iginx.utils.YAMLReader;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class YamlReadTest {

    final String filePath = "../example/src/main/resources/TransformJobExample.yaml";

    private final static Logger logger = LoggerFactory.getLogger(YAMLReader.class);

    private final static String[] taskTypeArr = {"iginx", "python", "python", "python"};

    private final static String[] dataFlowTypeArr = {"null", "stream", "batch", "stream"};

    private final static long[] timeoutArr = {10000000, 10000000, 10000000, 10000000};

    private final static String[] pyTaskNameArr = {"null", "AddOneTransformer", "SumTransformer", "RowSumTransformer"};

    private final static String[] sqlListArr = {"select value1, value2, value3, value4 from transform;", "null", "null", "null"};

    @Test
    public void test() throws FileNotFoundException {
        try {
            YAMLReader yamlReader = new YAMLReader(filePath);
            JobFromYAML jobFromYAML = yamlReader.getJobFromYAML();
            List<TaskFromYAML> taskList = jobFromYAML.getTaskList();

            assertEquals("file", jobFromYAML.getExportType());
            assertEquals("/Users/cauchy-ny/Downloads/export_file_sum_sql.txt", jobFromYAML.getExportFile());

            for (int i = 0; i < taskList.size(); i++) {
                assertEquals(taskTypeArr[i], taskList.get(i).getTaskType());
                assertEquals(dataFlowTypeArr[i], taskList.get(i).getDataFlowType() == null ? "null" : taskList.get(i).getDataFlowType());
                assertEquals(timeoutArr[i], taskList.get(i).getTimeout());
                assertEquals(pyTaskNameArr[i], taskList.get(i).getPyTaskName() == null ? "null" : taskList.get(i).getPyTaskName());
                assertEquals(sqlListArr[i], taskList.get(i).getSqlList() == null ? "null" : String.join(" ", taskList.get(i).getSqlList()));
            }
        } catch (FileNotFoundException e) {
            logger.error("Fail to close the file, path={}", filePath);
        }
    }
}
