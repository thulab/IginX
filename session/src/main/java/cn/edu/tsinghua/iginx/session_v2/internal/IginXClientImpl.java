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
import cn.edu.tsinghua.iginx.session_v2.AsyncWriteClient;
import cn.edu.tsinghua.iginx.session_v2.ClusterClient;
import cn.edu.tsinghua.iginx.session_v2.DeleteClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClient;
import cn.edu.tsinghua.iginx.session_v2.IginXClientOptions;
import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.SQLClient;
import cn.edu.tsinghua.iginx.session_v2.UsersClient;
import cn.edu.tsinghua.iginx.session_v2.WriteClient;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.thrift.CloseSessionReq;
import cn.edu.tsinghua.iginx.thrift.IService;
import cn.edu.tsinghua.iginx.thrift.OpenSessionReq;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class IginXClientImpl implements IginXClient {

    private static final Logger logger = LoggerFactory.getLogger(IginXClientImpl.class);

    private final IService.Iface client;

    private final TTransport transport;

    private final Lock lock; // IService.Iface 的 lock，保证多个线程在使用 client 时候不会互相影响

    private final long sessionId;

    private boolean isClosed;

    private final Collection<AutoCloseable> autoCloseables = new CopyOnWriteArrayList<>();

    public IginXClientImpl(IginXClientOptions options) {
        Arguments.checkNotNull(options, "IginXClientOptions");

        lock = new ReentrantLock();
        transport = new TSocket(options.getHost(), options.getPort());

        try {
            transport.open();
            client = new IService.Client(new TBinaryProtocol(transport));
        } catch (TTransportException e) {
            throw new IginXException("Open socket error: ", e);
        }

        try {
            OpenSessionReq req = new OpenSessionReq();
            req.setUsername(options.getUsername());
            req.setPassword(options.getPassword());
            sessionId = client.openSession(req).getSessionId();

        } catch (TException e) {
            throw new IginXException("Open session error: ", e);
        }

    }

    @Override
    public synchronized WriteClient getWriteClient() {
        checkIsClosed();
        return new WriteClientImpl(this);
    }

    @Override
    public synchronized AsyncWriteClient getAsyncWriteClient() {
        checkIsClosed();
        return new AsyncWriteClientImpl(this, autoCloseables);
    }

    @Override
    public synchronized SQLClient getSQLClient() {
        checkIsClosed();
        return null;
    }

    @Override
    public synchronized QueryClient getQueryClient() {
        checkIsClosed();
        return new QueryClientImpl(this);
    }

    @Override
    public synchronized DeleteClient getDeleteClient() {
        checkIsClosed();
        return null;
    }

    @Override
    public synchronized UsersClient getUserClient() {
        checkIsClosed();
        return new UsersClientImpl(this);
    }

    @Override
    public synchronized ClusterClient getClusterClient() {
        checkIsClosed();
        return new ClusterClientImpl(this);
    }

    void checkIsClosed() {
        if (isClosed) {
            throw new IginXException("Session has been closed.");
        }
    }

    public synchronized boolean isClosed() {
        return isClosed;
    }

    IService.Iface getClient() {
        return client;
    }

    Lock getLock() {
        return lock;
    }

    long getSessionId() {
        return sessionId;
    }

    @Override
    public synchronized void close() {
        if (isClosed) {
            logger.warn("Client has been closed.");
            return;
        }

        autoCloseables.stream().filter(Objects::nonNull).forEach(resource -> {
            try {
                resource.close();
            } catch (Exception e) {
                logger.warn(String.format("Exception was thrown while closing: %s", resource), e);
            }
        });

        CloseSessionReq req = new CloseSessionReq(sessionId);
        try {
            client.closeSession(req);
        } catch (TException e) {
            throw new IginXException("Close session error: ", e);
        } finally {
            if (transport != null) {
                transport.close();
            }
            isClosed = true;
        }
    }

}
