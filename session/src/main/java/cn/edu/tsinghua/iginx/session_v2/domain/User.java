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

import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import java.util.Set;

public final class User {

  private final String username;

  private final String password;

  private final UserType userType;

  private final Set<AuthType> auths;

  public User(String username, UserType userType, Set<AuthType> auths) {
    this(username, null, userType, auths);
  }

  public User(String username, String password, UserType userType, Set<AuthType> auths) {
    this.username = username;
    this.password = password;
    this.userType = userType;
    this.auths = auths;
  }

  public String getUsername() {
    return username;
  }

  public String getPassword() {
    return password;
  }

  public UserType getUserType() {
    return userType;
  }

  public Set<AuthType> getAuths() {
    return auths;
  }
}
