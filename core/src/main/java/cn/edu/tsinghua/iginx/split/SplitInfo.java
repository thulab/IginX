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
package cn.edu.tsinghua.iginx.split;

import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;

public class SplitInfo {

	private TimeInterval timeInterval;

	private TimeSeriesInterval timeSeriesInterval;

	private FragmentReplicaMeta replica;

	private boolean hasCreatedDatabase;

	public SplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, FragmentReplicaMeta replica) {
		this.timeInterval = timeInterval;
		this.timeSeriesInterval = timeSeriesInterval;
		this.replica = replica;
	}

	public SplitInfo(TimeInterval timeInterval, TimeSeriesInterval timeSeriesInterval, FragmentReplicaMeta replica, boolean hasCreatedDatabase) {
		this.timeInterval = timeInterval;
		this.timeSeriesInterval = timeSeriesInterval;
		this.replica = replica;
		this.hasCreatedDatabase = hasCreatedDatabase;
	}

	public TimeInterval getTimeInterval() {
		return timeInterval;
	}

	public void setTimeInterval(TimeInterval timeInterval) {
		this.timeInterval = timeInterval;
	}

	public TimeSeriesInterval getTimeSeriesInterval() {
		return timeSeriesInterval;
	}

	public void setTimeSeriesInterval(TimeSeriesInterval timeSeriesInterval) {
		this.timeSeriesInterval = timeSeriesInterval;
	}

	public FragmentReplicaMeta getReplica() {
		return replica;
	}

	public void setReplica(FragmentReplicaMeta replica) {
		this.replica = replica;
	}

	public boolean hasCreatedDatabase() {
		return hasCreatedDatabase;
	}

	public void setHasCreatedDatabase(boolean hasCreatedDatabase) {
		this.hasCreatedDatabase = hasCreatedDatabase;
	}
}
