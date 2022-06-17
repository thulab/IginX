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
package cn.edu.tsinghua.iginx.iotdb;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.IStorage;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class IoTDBStorageTest {

    private IStorage storage;

    @Test
    public void testInitialization() throws StorageInitializationException {
        StorageEngineMeta meta = new StorageEngineMeta(1L, "127.0.0.1", 6667, new HashMap<>(), "iotdb11", 1L);
        this.storage = new IoTDBStorage(meta);
    }

    @Test
    public void testProject() throws PhysicalException {
        FragmentMeta fragment = new FragmentMeta(null, null, 0, 1000);
        Source source = new FragmentSource(fragment);
        StoragePhysicalTask task = new StoragePhysicalTask(Collections.singletonList(new Project(source, Arrays.asList("wf01.wt01.status", "wf01.wt02.*"), null)));
        task.setStorageUnit("unit001");
        RowStream rowStream = this.storage.execute(task).getRowStream();
        Header header = rowStream.getHeader();;
        Row row;
        while (rowStream.hasNext()) {
            row = rowStream.next();
            System.out.println(row);
        }
        System.out.println(header);
        rowStream.close();
    }

}
