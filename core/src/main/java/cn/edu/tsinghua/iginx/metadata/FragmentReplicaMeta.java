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
package cn.edu.tsinghua.iginx.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public final class FragmentReplicaMeta implements Serializable {

    private static final long serialVersionUID = 3746419363724248556L;

    /**
     * 分片对应的时间序列的前缀，与开始时间戳、副本编号一起唯一确定一个分片
     */
    private String key;

    /**
     * 当前分片的开始时间戳
     */
    private long startTime;

    /**
     * 当前分片的副本编号，如果 replicaIndex = 0 则意味着当前分片是主分片
     */
    private int replicaIndex;

    /**
     * 当前分片的结束时间戳，如果当前分片尚未结束，则为 0
     */
    private long endTime;

    /**
     * 当前分片所在的数据库
     */
    private long databaseId;

    public FragmentReplicaMeta(String key, long startTime, int replicaIndex, long endTime, long databaseId) {
        //TODO:
    }

    FragmentReplicaMeta endFragmentReplicaMeta(long endTime) {
        return new FragmentReplicaMeta(this.key, this.startTime, this.replicaIndex, endTime, this.databaseId);
    }

    public long getDatabaseId() {
        //TODO:
        return -1;
    }

    public long getStartTime() {
        //TODO:
        return -1;
    }

    public long getEndTime() {
        //TODO:
        return -1;
    }
}
