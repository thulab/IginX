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
package cn.edu.tsinghua.iginx.integration;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class IMetaManagerTest {

    private static IMetaManager iMetaManager;

    @BeforeClass
    public static void beforeClass() {
        switch (System.getenv("STORAGE")) {
            case "zookeeper":
                ConfigDescriptor.getInstance().getConfig().setMetaStorage("zookeeper");
                ConfigDescriptor.getInstance().getConfig().setZookeeperConnectionString(System.getenv("ZOOKEEPER_CONNECTION_STRING"));
                System.out.println("use zookeeper as meta storage engine");
                break;
            case "etcd":
                ConfigDescriptor.getInstance().getConfig().setMetaStorage("etcd");
                ConfigDescriptor.getInstance().getConfig().setEtcdEndpoints(System.getenv("ETCD_ENDPOINTS"));
                System.out.println("use etcd as meta storage engine");
                break;
        }
        iMetaManager = DefaultMetaManager.getInstance();
    }

    @AfterClass
    public static void afterClass() {
        iMetaManager = null;
    }

    @Before
    public void setUp() {

    }

    @After
    public void tearDown() {

    }

    @Test
    public void schemaMappingTest() {
        System.out.println("Test Schema Mapping");
        System.out.printf("env: STORAGE=%s%n", System.getenv("STORAGE"));
    }

    @Test
    public void iginxTest() {

    }

    @Test
    public void storageEngineTest() {

    }

    @Test
    public void fragmentTest() {

    }

    @Test
    public void storageUnitTest() {

    }

}
