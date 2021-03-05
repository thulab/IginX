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

import cn.edu.tsinghua.iginx.core.db.DBType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public final class DatabaseMeta implements Serializable {

    private static final long serialVersionUID = -8247736081621565348L;

    /**
     * 数据库的 id
     */
    private long id;

    /**
     * 时序数据库所在的 ip
     */
    private String ip;

    /**
     * 时序数据库开放的端口
     */
    private int port;

    /**
     * 时序数据库需要的其他参数信息，例如用户名、密码等
     */
    private Map<String, String> extraParams;

    /**
     * 数据库类型
     */
    private DBType dbType;

    /**
     * 时序数据库存储的数据分片，不进行序列化。
     */
    private transient List<FragmentReplicaMeta> fragmentReplicaMetaList;

    public void addFragmentReplicaMeta(FragmentReplicaMeta fragmentReplicaMeta) {
        this.fragmentReplicaMetaList.add(fragmentReplicaMeta);
    }

    public void removeFragmentReplicaMeta(FragmentReplicaMeta fragmentReplicaMeta) {
        this.fragmentReplicaMetaList.remove(fragmentReplicaMeta);
    }

    public DatabaseMeta basicInfo() {
        return new DatabaseMeta(id, ip, port, extraParams, dbType, new ArrayList<>());
    }

    public int getFragmentReplicaMetaNum() {
        return fragmentReplicaMetaList.size();
    }
}
