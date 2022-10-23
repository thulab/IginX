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
package cn.edu.tsinghua.iginx.integration.mds;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import org.junit.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class IMetaManagerTest {

    private static IMetaManager iMetaManager;

    @BeforeClass
    public static void beforeClass() {
        if (System.getenv("STORAGE") != null) {
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
        }
        ConfigDescriptor.getInstance().getConfig().setStorageEngineList("");
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
        // add schema1-key1-1
        iMetaManager.addOrUpdateSchemaMappingItem("schema1", "key1", 1);
        // query schema1-key1-1
        assertEquals(1, iMetaManager.getSchemaMappingItem("schema1", "key1"));
        // query non-exists schema1-key2
        assertEquals(-1, iMetaManager.getSchemaMappingItem("schema1", "key2"));
        // query non-exists schema2-key1
        assertEquals(-1, iMetaManager.getSchemaMappingItem("schema2", "key1"));
        // add schema2-key1-2, schema2-key2-4
        Map<String, Integer> schemaMap2 = new HashMap<>();
        schemaMap2.put("key1", 2);
        schemaMap2.put("key2", 4);
        iMetaManager.addOrUpdateSchemaMapping("schema2", schemaMap2);
        // query schema2
        Map<String, Integer> queriedSchemaMap2 = iMetaManager.getSchemaMapping("schema2");
        for (String key : queriedSchemaMap2.keySet()) {
            assertEquals(schemaMap2.get(key), queriedSchemaMap2.get(key));
        }
        // add schema2-key3-6
        schemaMap2.put("key3", 6);
        iMetaManager.addOrUpdateSchemaMappingItem("schema2", "key3", 6);
        // query schema2
        queriedSchemaMap2 = iMetaManager.getSchemaMapping("schema2");
        for (String key : queriedSchemaMap2.keySet()) {
            assertEquals(schemaMap2.get(key), queriedSchemaMap2.get(key));
        }
    }

    @Test
    public void storageEngineTest() {
        List<StorageEngineMeta> storageEngines = iMetaManager.getStorageEngineList();
        // 初始情况下没有存储数据后端
        assertEquals(0, storageEngines.size());
        // 增加一个数据后端
        Map<String, String> extraParams = new HashMap<>();
        extraParams.put("username", "root");
        extraParams.put("password", "root");
        extraParams.put("sessionPoolSize", "20");
        StorageEngineMeta engine1 = new StorageEngineMeta(0, "127.0.0.1", 1001, extraParams, "iotdb", iMetaManager.getIginxId());
        iMetaManager.addStorageEngines(Collections.singletonList(engine1));
        // 查询数据后端
        storageEngines = iMetaManager.getStorageEngineList();
        assertEquals(1, storageEngines.size());
        assertEquals(1001, storageEngines.get(0).getPort());
        assertEquals(iMetaManager.getIginxId(), storageEngines.get(0).getCreatedBy());
    }

    @Test
    public void storageUnitAndFragmentTest() {
        // TODO: 测试优先级
    }

}
