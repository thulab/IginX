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
package cn.edu.tsinghua.iginx.rest;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

public class RestServer implements Runnable {
	private static final Logger LOGGER = LoggerFactory.getLogger(RestServer.class);
	private static Config config = ConfigDescriptor.getInstance().getConfig();
	private static URI baseURI;

	private static URI getBaseURI() {
		return UriBuilder.fromUri("http://" + config.getRestIp() + "/").port(config.getRestPort()).build();
	}

	private static HttpServer startServer() {
		config = ConfigDescriptor.getInstance().getConfig();
		baseURI = getBaseURI();
		final ResourceConfig rc = new ResourceConfig().packages("cn.edu.tsinghua.iginx.rest");
		return GrizzlyHttpServerFactory.createHttpServer(baseURI, rc);
	}

	public static void start() {
		HttpServer server = null;
		try {
			server = startServer();
		} catch (Exception e) {
			LOGGER.error("启动Rest服务失败，请检查是否启动了IoTDB服务以及相关配置参数是否正确", e);
			System.exit(1);
		}
		LOGGER.info("Iginx REST server has been available at {}.", baseURI);
		try {
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			LOGGER.error("Rest主线程出现异常", e);
			Thread.currentThread().interrupt();
		}
		server.shutdown();
	}

	public static void main(String[] argv) {
		start();
	}

	@Override
	public void run() {
		start();
	}
}
