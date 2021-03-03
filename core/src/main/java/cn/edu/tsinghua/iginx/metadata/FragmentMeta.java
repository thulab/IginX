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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public final class FragmentMeta implements Serializable {

    private final String key;

    private final long startTime;

    private final long endTime;

    /**
     * 所有的分片的信息，使用不可变列表
     */
    private final Map<Integer, FragmentReplicaMeta> replicaMetas;

    public FragmentMeta(String key, long startTime, long endTime, Map<Integer, FragmentReplicaMeta> replicaMetas) {
        this.key = key;
        this.startTime = startTime;
        this.endTime = endTime;
        this.replicaMetas = replicaMetas;
    }

    public FragmentMeta(String key, long startTime, long endTime, long databaseId) {
        this.key = key;
        this.startTime = startTime;
        this.endTime = endTime;
        this.replicaMetas = new HashMap<>();
        this.replicaMetas.put(0, new FragmentReplicaMeta(key, startTime, 0, endTime, databaseId));
    }

    public String getKey() {
        return key;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Map<Integer, FragmentReplicaMeta> getReplicaMetas() {
        return new HashMap<>(replicaMetas);
    }

    FragmentMeta endFragment(long endTime) {
        Map<Integer, FragmentReplicaMeta> newReplicaMetas = new HashMap<>();
        for (Map.Entry<Integer, FragmentReplicaMeta> replicaMetaEntry: replicaMetas.entrySet()) {
            int replicaId = replicaMetaEntry.getKey();
            FragmentReplicaMeta fragmentReplicaMeta = replicaMetaEntry.getValue().endFragmentReplicaMeta(endTime);
            newReplicaMetas.put(replicaId, fragmentReplicaMeta);
        }
        return new FragmentMeta(this.key, this.startTime, endTime, newReplicaMetas);
    }

    public int getReplicaMetasNum() {
        return replicaMetas.size();
    }

}
