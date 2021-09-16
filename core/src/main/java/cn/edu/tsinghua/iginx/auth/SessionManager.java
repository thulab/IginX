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

import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import io.netty.util.internal.ConcurrentSet;

import java.util.Set;

public class SessionManager {

    private static SessionManager instance;
    private final UserManager userManager;
    private final Set<Long> sessionIds = new ConcurrentSet<>();

    private SessionManager(UserManager userManager) {
        this.userManager = userManager;
    }

    public static SessionManager getInstance() {
        if (instance == null) {
            synchronized (UserManager.class) {
                if (instance == null) {
                    instance = new SessionManager(UserManager.getInstance());
                }
            }
        }
        return instance;
    }

    public boolean checkSession(long sessionId, AuthType auth) {
        if (!sessionIds.contains(sessionId)) {
            return false;
        }
        return ((1L << auth.getValue()) & sessionId) != 0;
    }

    public long openSession(String username) {
        UserMeta userMeta = userManager.getUser(username);
        if (userMeta == null) {
            throw new IllegalArgumentException("non-existed user: " + username);
        }
        long sessionId = (username.hashCode() + SnowFlakeUtils.getInstance().nextId()) << 4;
        for (AuthType auth : userMeta.getAuths()) {
            sessionId += (1L << auth.getValue());
        }
        sessionIds.add(sessionId);
        return sessionId;
    }

    public void closeSession(long sessionId) {
        sessionIds.remove(sessionId);
    }

}
