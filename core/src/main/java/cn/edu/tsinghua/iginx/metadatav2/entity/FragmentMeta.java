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
package cn.edu.tsinghua.iginx.metadatav2.entity;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class FragmentMeta {

    private final String beginPrefix;

    private final String endPrefix;

    private final long startTime;

    private final long endTime;

    /**
     * 所有的分片的信息
     */
    private final Map<Integer, FragmentReplicaMeta> replicaMetas;

    public FragmentMeta(String beginPrefix, String endPrefix, long startTime, long endTime, Map<Integer, FragmentReplicaMeta> replicaMetas) {
        this.beginPrefix = beginPrefix;
        this.endPrefix = endPrefix;
        this.startTime = startTime;
        this.endTime = endTime;
        this.replicaMetas = replicaMetas;
    }

    public FragmentMeta(String beginPrefix, String endPrefix, long startTime, long endTime, List<Long> databaseIds) {
        this.beginPrefix = beginPrefix;
        this.endPrefix = endPrefix;
        this.startTime = startTime;
        this.endTime = endTime;
        Map<Integer, FragmentReplicaMeta> replicaMetas = new HashMap<>();
        for (int i = 0; i < databaseIds.size(); i++) {
            replicaMetas.put(i, new FragmentReplicaMeta(i, databaseIds.get(i)));
        }
        this.replicaMetas = Collections.unmodifiableMap(replicaMetas);
    }

    public String getBeginPrefix() {
        return beginPrefix;
    }

    public String getEndPrefix() {
        return endPrefix;
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

    public int getReplicaMetasNum() {
        return replicaMetas.size();
    }

}
