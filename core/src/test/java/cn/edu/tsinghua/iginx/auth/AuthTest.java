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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.apache.curator.shaded.com.google.common.collect.Sets;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.List;
import java.util.Set;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AuthTest {

    private static final String FILE_META_STORAGE = "file";

    private static final String FILE_DATA_DIR = "meta";

    private static final String ZOOKEEPER_META_STORAGE = "zookeeper";

    private static final String ZOOKEEPER_CONNECTION_STRING = "127.0.0.1:2181";

    private static final String ETCD_META_STORAGE = "etcd";

    private static final String ETCD_END_POINTS = "http://localhost:2379";

    private static final String username = "thu";

    private static final String password = "pku";

    private static Config config;

    private static UserManager userManager;

    private static SessionManager sessionManager;

    @BeforeClass
    public static void setUp() {
        System.setProperty(Constants.CONF, "../conf/config.properties");

        config = ConfigDescriptor.getInstance().getConfig();

        config.setMetaStorage(FILE_META_STORAGE);
        config.setFileDataDir(FILE_DATA_DIR);

//        config.setMetaStorage(ZOOKEEPER_META_STORAGE);
//        config.setZookeeperConnectionString(ZOOKEEPER_CONNECTION_STRING);
//
//        config.setMetaStorage(ETCD_META_STORAGE);
//        config.setEtcdEndpoints(ETCD_END_POINTS);

        userManager = UserManager.getInstance();
        sessionManager = SessionManager.getInstance();
    }

    @AfterClass
    public static void tearDown() {
        config = null;
        userManager = null;
        sessionManager = null;

    }

    @Test
    public void test1DefaultUser() {
        Assert.assertTrue(userManager.hasUser(config.getUsername()));

        List<UserMeta> users = userManager.getUsers();
        Assert.assertEquals(1, users.size());

        UserMeta adminUser = users.get(0);
        Assert.assertEquals(config.getUsername(), adminUser.getUsername());
        Assert.assertEquals(config.getPassword(), adminUser.getPassword());
        Assert.assertEquals(UserType.Administrator, adminUser.getUserType());

        Set<AuthType> auths = adminUser.getAuths();
        Assert.assertTrue(auths.contains(AuthType.Admin));
        Assert.assertTrue(auths.contains(AuthType.Cluster));
        Assert.assertTrue(auths.contains(AuthType.Read));
        Assert.assertTrue(auths.contains(AuthType.Write));

    }

    @Test
    public void test2AddUser() {
        userManager.addUser(username, password, Sets.newHashSet(AuthType.Read, AuthType.Write));

        Assert.assertTrue(userManager.hasUser(username));

        UserMeta user = userManager.getUser(username);
        Assert.assertNotNull(user);

        Assert.assertEquals(password, user.getPassword());
        Assert.assertEquals(UserType.OrdinaryUser, user.getUserType());

        Set<AuthType> auths = user.getAuths();
        Assert.assertFalse(auths.contains(AuthType.Admin));
        Assert.assertFalse(auths.contains(AuthType.Cluster));
        Assert.assertTrue(auths.contains(AuthType.Read));
        Assert.assertTrue(auths.contains(AuthType.Write));

    }

    @Test
    public void test3OpenSession() {
        long userSession = sessionManager.openSession(username);
        long adminSession = sessionManager.openSession(config.getUsername());

        Assert.assertFalse(sessionManager.checkSession(userSession, AuthType.Admin));
        Assert.assertFalse(sessionManager.checkSession(userSession, AuthType.Cluster));
        Assert.assertTrue(sessionManager.checkSession(userSession, AuthType.Read));
        Assert.assertTrue(sessionManager.checkSession(userSession, AuthType.Write));

        Assert.assertTrue(sessionManager.checkSession(adminSession, AuthType.Admin));
        Assert.assertTrue(sessionManager.checkSession(adminSession, AuthType.Cluster));
        Assert.assertTrue(sessionManager.checkSession(adminSession, AuthType.Read));
        Assert.assertTrue(sessionManager.checkSession(adminSession, AuthType.Write));

        sessionManager.closeSession(userSession);
        Assert.assertFalse(sessionManager.checkSession(userSession, AuthType.Admin));
        Assert.assertFalse(sessionManager.checkSession(userSession, AuthType.Cluster));
        Assert.assertFalse(sessionManager.checkSession(userSession, AuthType.Read));
        Assert.assertFalse(sessionManager.checkSession(userSession, AuthType.Write));

        sessionManager.closeSession(adminSession);
        Assert.assertFalse(sessionManager.checkSession(adminSession, AuthType.Admin));
        Assert.assertFalse(sessionManager.checkSession(adminSession, AuthType.Cluster));
        Assert.assertFalse(sessionManager.checkSession(adminSession, AuthType.Read));
        Assert.assertFalse(sessionManager.checkSession(adminSession, AuthType.Write));

        long fakeSession = (username.hashCode() + SnowFlakeUtils.getInstance().nextId()) << 4;
        fakeSession = fakeSession | 0xF;
        Assert.assertFalse(sessionManager.checkSession(fakeSession, AuthType.Admin));
        Assert.assertFalse(sessionManager.checkSession(fakeSession, AuthType.Cluster));
        Assert.assertFalse(sessionManager.checkSession(fakeSession, AuthType.Read));
        Assert.assertFalse(sessionManager.checkSession(fakeSession, AuthType.Write));
    }

    @Test
    public void test4CheckUser() {
        Assert.assertTrue(userManager.checkUser(username, password));
        Assert.assertFalse(userManager.checkUser(password, username));

        Assert.assertTrue(userManager.checkUser(config.getUsername(), config.getPassword()));
    }

    @Test
    public void test5UpdateUser() {
        userManager.updateUser(username, username, null);

        UserMeta user = userManager.getUser(username);
        Assert.assertNotNull(user);
        Assert.assertEquals(username, user.getPassword());

        Set<AuthType> auths = user.getAuths();
        Assert.assertFalse(auths.contains(AuthType.Admin));
        Assert.assertFalse(auths.contains(AuthType.Cluster));
        Assert.assertTrue(auths.contains(AuthType.Read));
        Assert.assertTrue(auths.contains(AuthType.Write));

        userManager.updateUser(username, password, Sets.newHashSet(AuthType.Read));

        user = userManager.getUser(username);
        Assert.assertNotNull(user);
        Assert.assertEquals(password, user.getPassword());

        auths = user.getAuths();
        Assert.assertFalse(auths.contains(AuthType.Admin));
        Assert.assertFalse(auths.contains(AuthType.Cluster));
        Assert.assertTrue(auths.contains(AuthType.Read));
        Assert.assertFalse(auths.contains(AuthType.Write));

        userManager.updateUser(username, password, Sets.newHashSet(AuthType.Read, AuthType.Write));

        user = userManager.getUser(username);
        Assert.assertNotNull(user);
        Assert.assertEquals(password, user.getPassword());

        auths = user.getAuths();
        Assert.assertFalse(auths.contains(AuthType.Admin));
        Assert.assertFalse(auths.contains(AuthType.Cluster));
        Assert.assertTrue(auths.contains(AuthType.Read));
        Assert.assertTrue(auths.contains(AuthType.Write));

    }

    @Test
    public void test6RemoveUser() {
        Assert.assertTrue(userManager.deleteUser(username));
        Assert.assertFalse(userManager.deleteUser(config.getUsername()));
    }

}
