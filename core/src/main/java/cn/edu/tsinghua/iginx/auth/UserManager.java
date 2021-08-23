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
package cn.edu.tsinghua.iginx.auth;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

public class UserManager {

    private static final Logger logger = LoggerFactory.getLogger(UserManager.class);

    private static UserManager instance;

    private final IMetaManager metaManager;

    private UserManager(IMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public static UserManager getInstance() {
        if (instance == null) {
            synchronized (UserManager.class) {
                if (instance == null) {
                    instance = new UserManager(DefaultMetaManager.getInstance());
                }
            }
        }
        return instance;
    }

    public boolean hasUser(String username) {
        UserMeta user = metaManager.getUser(username);
        return user != null;
    }

    public boolean checkUser(String username, String password) {
        UserMeta user = metaManager.getUser(username);
        return user != null && user.getPassword().equals(password);
    }

    public boolean addUser(String username, String password, Set<AuthType> auths) {
        UserMeta user = new UserMeta(username, password, UserType.OrdinaryUser, auths);
        return metaManager.addUser(user);
    }

    public boolean updateUser(String username, String password, Set<AuthType> auths) {
        return metaManager.updateUser(username, password, auths);
    }

    public boolean deleteUser(String username) {
        return metaManager.removeUser(username);
    }

    public UserMeta getUser(String username) {
        return metaManager.getUser(username);
    }

    public List<UserMeta> getUsers(List<String> usernames) {
        return metaManager.getUsers(usernames);
    }

    public List<UserMeta> getUsers() {
        return metaManager.getUsers();
    }

}
