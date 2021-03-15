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

import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentReplicaMeta;

import java.util.List;

public class SplitInfo {

	private List<Integer> pathsIndexes;

	// TODO startTime endTime 不一定与 replica 的 startTime endTime 相同
	// TODO 而且不一定是连续的（？）

	private FragmentReplicaMeta replica;

	public SplitInfo(List<Integer> pathsIndexes, FragmentReplicaMeta replica) {
		this.pathsIndexes = pathsIndexes;
		this.replica = replica;
	}

	public List<Integer> getPathsIndexes() {
		return pathsIndexes;
	}

	public void setPathsIndexes(List<Integer> pathsIndexes) {
		this.pathsIndexes = pathsIndexes;
	}

	public FragmentReplicaMeta getReplica() {
		return replica;
	}

	public void setReplica(FragmentReplicaMeta replica) {
		this.replica = replica;
	}
}
