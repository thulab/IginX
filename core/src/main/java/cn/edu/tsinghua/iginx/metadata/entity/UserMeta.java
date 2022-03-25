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

import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import java.util.HashSet;
import java.util.Set;

public class UserMeta {

  private String username;

  private String password;

  private UserType userType;

  private Set<AuthType> auths;

  public UserMeta(String username, String password, UserType userType, Set<AuthType> auths) {
    this.username = username;
    this.password = password;
    this.userType = userType;
    this.auths = auths;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public UserType getUserType() {
    return userType;
  }

  public void setUserType(UserType userType) {
    this.userType = userType;
  }

  public Set<AuthType> getAuths() {
    return auths;
  }

  public void setAuths(Set<AuthType> auths) {
    this.auths = auths;
  }

  public UserMeta copy() {
    return new UserMeta(username, password, userType, new HashSet<>(auths));
  }

}
