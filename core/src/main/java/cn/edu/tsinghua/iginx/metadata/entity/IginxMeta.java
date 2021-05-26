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
package cn.edu.tsinghua.iginx.metadata.entity;

import java.util.Map;

public final class IginxMeta {

    /**
     * iginx 的 id
     */
    private final long id;

    /**
     * iginx 所在 ip
     */
    private final String ip;

    /**
     * iginx 对外暴露的端口
     */
    private final int port;

    /**
     * iginx 其他控制参数
     */
    private final Map<String, String> extraParams;

    public IginxMeta(long id, String ip, int port, Map<String, String> extraParams) {
        this.id = id;
        this.ip = ip;
        this.port = port;
        this.extraParams = extraParams;
    }

    public long getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public Map<String, String> getExtraParams() {
        return extraParams;
    }
}
