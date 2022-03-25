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
package cn.edu.tsinghua.iginx.session_v2.domain;

import java.util.Map;

public final class Storage {

  private final String ip;

  private final int port;

  private final String type;

  private final Map<String, String> extraParams;

  public Storage(String ip, int port, String type, Map<String, String> extraParams) {
    this.ip = ip;
    this.port = port;
    this.type = type;
    this.extraParams = extraParams;
  }

  public String getIp() {
    return ip;
  }

  public int getPort() {
    return port;
  }

  public String getType() {
    return type;
  }

  public Map<String, String> getExtraParams() {
    return extraParams;
  }
}
