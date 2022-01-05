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
package cn.edu.tsinghua.iginx.influxdb;

import cn.edu.tsinghua.iginx.engine.physical.exception.NonExecutablePhysicalTaskException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.operator.Insert;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;
import cn.edu.tsinghua.iginx.engine.shared.operator.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.domain.WritePrecision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static com.influxdb.client.domain.WritePrecision.MS;

public class InfluxDBStorage implements IStorage {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBPlanExecutor.class);

    private static final String STORAGE_ENGINE = "influxdb";

    private static final WritePrecision WRITE_PRECISION = MS;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static final String QUERY_DATA = "from(bucket:\"%s\") |> range(start: %s, stop: %s)";

    private static final String DELETE_DATA = "_measurement=\"%s\" AND _field=\"%s\" AND t=\"%s\"";

    private final StorageEngineMeta meta;

    private final InfluxDBClient client;

    private final String organization;

    public InfluxDBStorage(StorageEngineMeta meta) throws StorageInitializationException {
        this.meta = meta;
        if (!meta.getStorageEngine().equals(STORAGE_ENGINE)) {
            throw new StorageInitializationException("unexpected database: " + meta.getStorageEngine());
        }
        if (!testConnection()) {
            throw new StorageInitializationException("cannot connect to " + meta.toString());
        }
        Map<String, String> extraParams = meta.getExtraParams();
        String url = extraParams.getOrDefault("url", "http://localhost:8086/");
        client = InfluxDBClientFactory.create(url, extraParams.get("token").toCharArray());
        organization = extraParams.get("organization");
    }

    private boolean testConnection() {
        Map<String, String> extraParams = meta.getExtraParams();
        String url = extraParams.get("url");
        try {
            InfluxDBClient client = InfluxDBClientFactory.create(url, extraParams.get("token").toCharArray());
            client.close();
        } catch (Exception e) {
            logger.error("test connection error: {}", e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public TaskExecuteResult execute(StoragePhysicalTask task) {
        List<Operator> operators = task.getOperators();
        if (operators.size() != 1) {
            return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
        }
        FragmentMeta fragment = task.getTargetFragment();
        Operator op = operators.get(0);
        String storageUnit = task.getStorageUnit();

        if (op.getType() == OperatorType.Project) { // 目前只实现 project 操作符
            Project project = (Project) op;
            return executeProjectTask(fragment.getTimeInterval(), fragment.getTsInterval(), storageUnit, project);
        } else if (op.getType() == OperatorType.Insert) {
            Insert insert = (Insert) op;
            return executeInsertTask(storageUnit, insert);
        } else if (op.getType() == OperatorType.Delete) {
            Delete delete = (Delete) op;
            return executeDeleteTask(storageUnit, delete);
        }
        return new TaskExecuteResult(new NonExecutablePhysicalTaskException("unsupported physical task"));
    }

    private TaskExecuteResult executeProjectTask(TimeInterval timeInterval, TimeSeriesInterval tsInterval, String storageUnit, Project project) {
        Organization organization = client.getOrganizationsApi()
                .findOrganizations().stream()
                .filter(o -> o.getName().equals(this.organization))
                .findFirst()
                .orElseThrow(IllegalStateException::new);

        if (client.getBucketsApi().findBucketByName(storageUnit) == null) {
            logger.error("storage engine {} doesn't exist", storageUnit);
            return new TaskExecuteResult(new PhysicalTaskExecuteFailureException("storage unit " + storageUnit + " doesn't exist"));
        }

        return null;
    }

    private TaskExecuteResult executeInsertTask(String storageUnit, Insert insert) {
        return null;
    }

    private TaskExecuteResult executeDeleteTask(String storageUnit, Delete delete) {
        return null;
    }

}
