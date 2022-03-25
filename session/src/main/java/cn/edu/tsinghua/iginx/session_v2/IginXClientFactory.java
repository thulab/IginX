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
package cn.edu.tsinghua.iginx.session_v2;


import cn.edu.tsinghua.iginx.session_v2.internal.IginXClientImpl;

public final class IginXClientFactory {

  private IginXClientFactory() {

  }

  public static IginXClient create() {
    return create("127.0.0.1", 6888);
  }

  public static IginXClient create(String url) {
    IginXClientOptions options = IginXClientOptions.builder()
        .url(url)
        .build();
    return create(options);
  }

  public static IginXClient create(String host, int port) {
    IginXClientOptions options = IginXClientOptions.builder()
        .host(host)
        .port(port)
        .build();
    return create(options);
  }

  public static IginXClient create(String url, String username, String password) {
    IginXClientOptions options = IginXClientOptions.builder()
        .url(url)
        .username(username)
        .password(password)
        .build();
    return create(options);
  }

  public static IginXClient create(String host, int port, String username, String password) {
    IginXClientOptions options = IginXClientOptions.builder()
        .host(host)
        .port(port)
        .username(username)
        .password(password)
        .build();
    return create(options);
  }

  public static IginXClient create(IginXClientOptions options) {
    Arguments.checkNotNull(options, "IginXClientOptions");
    return new IginXClientImpl(options);
  }


}
