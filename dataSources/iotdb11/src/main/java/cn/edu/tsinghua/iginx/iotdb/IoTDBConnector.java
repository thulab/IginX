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
package cn.edu.tsinghua.iginx.iotdb;

import cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.Connector;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IoTDBConnector implements Connector {

    private static final Logger logger = LoggerFactory.getLogger(IoTDBConnector.class);

    private static final ExecutorService service = Executors.newSingleThreadExecutor();

    private Session session;

    public IoTDBConnector(String ip, int port, String username, String password) {
        this.session = new Session(ip, port, username, password);
    }

    @Override
    public boolean echo(long timeout, TimeUnit unit) {
        Future<Boolean> future = service.submit(() -> {
            try {
                session.open();
                SessionDataSet dataSet = session.executeQueryStatement("show version");
                dataSet.closeOperationHandle();
                session.close();
            } catch (IoTDBConnectionException e) {
                logger.error("connect to iotdb error: " + e.getMessage());
                return false;
            } catch (StatementExecutionException e) {
                logger.error("execute statement error: " + e.getMessage());
            }
            return true;
        });
        try {
            return future.get(timeout, unit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            future.cancel(true);
            logger.error("connection timeout: ", e);
        }
        return false;
    }

    @Override
    public void reset() {
        this.session = null;
    }

}
