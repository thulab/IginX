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
package cn.edu.tsinghua.iginx.session_v2.internal;


import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.ClusterClient;
import cn.edu.tsinghua.iginx.session_v2.DeleteClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientOptions;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.UserClient;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.domain.Ready;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IginXClientImpl implements IginXClient {

    private static final Logger logger = LoggerFactory.getLogger(IginXClientImpl.class);

    private IService.Iface client;

    private final TTransport transport;

    private final Lock lock; // IService.Iface 的 lock，保证多个线程在使用 client 时候不会互相影响

    private long sessionId;

    private boolean isClosed;

    public IginXClientImpl(IginXClientOptions options) {
        Arguments.checkNotNull(options, "IginXClientOptions");

        this.lock = new ReentrantLock();
        transport = new TSocket(options.getHost(), options.getPort());

        try {
            transport.open();
            client = new IService.Client(new TBinaryProtocol(transport));

            OpenSessionReq req = new OpenSessionReq();
            req.setUsername(options.getUsername());
            req.setPassword(options.getPassword());

            sessionId = client.openSession(req).getSessionId();
        } catch (TException e) {
            e.printStackTrace();
        }

    }

    @Override
    public WriteClient getWriteClient() {
        return new WriteClientImpl(this);
    }

    @Override
    public QueryClient getQueryClient() {
        return null;
    }

    @Override
    public DeleteClient getDeleteClient() {
        return null;
    }

    @Override
    public UserClient getUserClient() {
        return null;
    }

    @Override
    public ClusterClient getClusterClient() {
        return null;
    }

    @Override
    public Ready ready() {
        return null;
    }

    @Override
    public void close() {
        if (isClosed) {
            return;
        }
        CloseSessionReq req = new CloseSessionReq(sessionId);
        try {
            client.closeSession(req);
        } catch (TException e) {
            logger.error("close session failure: ", e);
        } finally {
            if (transport != null) {
                transport.close();
            }
            isClosed = true;
        }
    }

    public IService.Iface getClient() {
        return client;
    }

    public Lock getLock() {
        return lock;
    }

    public long getSessionId() {
        return sessionId;
    }
}
