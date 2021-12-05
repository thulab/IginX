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
package cn.edu.tsinghua.iginx;

import cn.edu.tsinghua.iginx.cluster.IginxWorker;
import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.mqtt.MQTTService;
import cn.edu.tsinghua.iginx.rest.RestServer;
import cn.edu.tsinghua.iginx.thrift.IService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Iginx {

    private static final Logger logger = LoggerFactory.getLogger(Iginx.class);

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    public static void main(String[] args) throws Exception {
        if (config.isEnableRestService()) {
            new Thread(new RestServer()).start();
        }
        if (config.isEnableMQTT()) {
            new Thread(MQTTService.getInstance()).start();
        }
        Iginx iginx = new Iginx();
        iginx.startServer();
    }

    private void startServer() throws TTransportException {
        TProcessor processor = new IService.Processor<IService.Iface>(IginxWorker.getInstance());
        TServerSocket serverTransport = new TServerSocket(ConfigDescriptor.getInstance().getConfig().getPort());
        TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverTransport).processor(processor)
                .minWorkerThreads(20);
        args.protocolFactory(new TBinaryProtocol.Factory());
        TServer server = new TThreadPoolServer(args);
        logger.info("iginx starts success!");
        server.serve();
    }


}
