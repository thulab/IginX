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
package cn.edu.tsinghua.iginx.engine.physical.storage;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.engine.physical.exception.NonExistedStorageException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.StorageInitializationException;
import cn.edu.tsinghua.iginx.engine.physical.storage.queue.StoragePhysicalTaskQueue;
import cn.edu.tsinghua.iginx.engine.physical.task.StoragePhysicalTask;
import cn.edu.tsinghua.iginx.engine.physical.task.TaskExecuteResult;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.query.StorageEngineClassLoader;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

// TODO: 考虑扩容事件的响应
public class StorageManager {

    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);

    private final Map<String, ClassLoader> classLoaders = new ConcurrentHashMap<>();

    private final Map<String, String> drivers = new ConcurrentHashMap<>();

    private final Map<Long, Pair<IStorage, ExecutorService>> storageMap = new ConcurrentHashMap<>();

    public StorageManager(List<StorageEngineMeta> metaList) {
        initClassLoaderAndDrivers();
        String engine = "";
        String driver = "";
        try {
            for (StorageEngineMeta meta: metaList) {
                engine = meta.getStorageEngine();
                driver = drivers.get(engine);
                ClassLoader loader = classLoaders.get(engine);
                IStorage storage = (IStorage) loader.loadClass(driver)
                        .getConstructor(StorageEngineMeta.class).newInstance(meta);
                // 启动一个派发线程池
                ExecutorService dispatcher = new ThreadPoolExecutor(0,
                        ConfigDescriptor.getInstance().getConfig().getPhysicalTaskThreadPoolSizePerStorage(),
                        60L, TimeUnit.SECONDS, new SynchronousQueue<>());
                storageMap.put(meta.getId(), new Pair<>(storage, dispatcher));
            }
        } catch (ClassNotFoundException e) {
            logger.error("load class {} for engine {} failure: {}", driver, engine, e);
            System.exit(-1);
        } catch (Exception e) {
            logger.error("unexpected error when process engine {}: {}", engine, e);
            System.exit(-1);
        }

    }

    private void initClassLoaderAndDrivers() {
        String[] parts = ConfigDescriptor.getInstance().getConfig().getDatabaseClassNames().split(",");
        for (String part: parts) {
            String[] kAndV = part.split("=");
            if (kAndV.length != 2) {
                logger.error("unexpected database class names: {}", part);
                System.exit(-1);
            }
            String storage = kAndV[0];
            String driver = kAndV[1];
            try {
                ClassLoader classLoader = new StorageEngineClassLoader(storage);
                classLoaders.put(storage, classLoader);
                drivers.put(storage, driver);
            } catch (IOException e) {
                logger.error("encounter error when init class loader for {}: {}", storage, e);
                System.exit(-1);
            }
        }
    }

    private static String getDriverClassName(String storageEngine) {
        String[] parts = ConfigDescriptor.getInstance().getConfig().getDatabaseClassNames().split(",");
        for (String part : parts) {
            String[] kAndV = part.split("=");
            if (!kAndV[0].equals(storageEngine)) {
                continue;
            }
            return kAndV[1];
        }
        throw new RuntimeException("cannot find driver for " + storageEngine + ", please check config.properties ");
    }

    public Map<Long, Pair<IStorage, ExecutorService>> getStorageMap() {
        return storageMap;
    }

    public Pair<IStorage, ExecutorService> getStorage(long id) {
        return storageMap.get(id);
    }
}
