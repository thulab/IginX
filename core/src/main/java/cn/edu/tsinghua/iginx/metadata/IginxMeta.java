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

import java.util.ArrayList;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

@Data
@AllArgsConstructor
public final class IginxMeta implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(IginxMeta.class);

    private static final long serialVersionUID = 3984654757395062391L;

    /**
     * iginx 的 id
     */
    private long id;

    /**
     * iginx 所在 ip
     */
    private String ip;

    /**
     * iginx 对外暴露的端口
     */
    private int port;

    /**
     * iginx 其他控制参数
     */
    private Map<String, String> extraParams;

    public IginxMeta(long id, String ip, int port, Object o) {
        //TODO:
    }

    public long getId() {
        //TODO:
        return -1;
    }

    public String getIp() {
        //TODO:
        return null;
    }

    public String getPort() {
        //TODO:
        return null;
    }
}
