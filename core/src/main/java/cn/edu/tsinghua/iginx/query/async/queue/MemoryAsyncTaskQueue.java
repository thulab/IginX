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
package cn.edu.tsinghua.iginx.query.async.queue;

import cn.edu.tsinghua.iginx.query.async.task.AsyncTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class MemoryAsyncTaskQueue implements AsyncTaskQueue {

	private static final Logger logger = LoggerFactory.getLogger(MemoryAsyncTaskQueue.class);

	private final LinkedBlockingQueue<AsyncTask> asyncTasks = new LinkedBlockingQueue<>();

	@Override
	public boolean addAsyncTask(AsyncTask asyncTask) {
		return asyncTasks.add(asyncTask);
	}

	@Override
	public AsyncTask getAsyncTask() {
		try {
			return asyncTasks.take();
		} catch (Exception e) {
			logger.error("encounter error when get async task: ", e);
		}
		return null;
	}
}
