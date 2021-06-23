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
package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryMetaStorage implements IMetaStorage {

    private final Lock storageUnitLock = new ReentrantLock();

    private final Lock fragmentUnitLock = new ReentrantLock();

    private SchemaMappingChangeHook schemaMappingChangeHook = null;

    private IginxChangeHook iginxChangeHook = null;

    private StorageChangeHook storageChangeHook = null;

    private StorageUnitChangeHook storageUnitChangeHook = null;

    private FragmentChangeHook fragmentChangeHook = null;

    private final AtomicLong idGenerator = new AtomicLong(0L);

    private static MemoryMetaStorage INSTANCE;

    public static MemoryMetaStorage getInstance() {
        if (INSTANCE == null) {
            synchronized (MemoryMetaStorage.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MemoryMetaStorage();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
        return new HashMap<>();
    }

    @Override
    public void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook) {
        if (hook != null) {
            schemaMappingChangeHook = hook;
        }
    }

    @Override
    public void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping) throws MetaStorageException {
        if (schemaMappingChangeHook != null) {
            schemaMappingChangeHook.onChange(schema, schemaMapping);
        }
    }

    @Override
    public Map<Long, IginxMeta> loadIginx() throws MetaStorageException {
        return new HashMap<>();
    }

    @Override
    public long registerIginx(IginxMeta iginx) throws MetaStorageException {
        return idGenerator.incrementAndGet();
    }

    @Override
    public void registerIginxChangeHook(IginxChangeHook hook) {
        if (hook != null) {
            iginxChangeHook = hook;
        }
    }

    @Override
    public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> storageEngines) throws MetaStorageException {
        Map<Long, StorageEngineMeta> storageEngineMap = new HashMap<>();
        for (StorageEngineMeta storageEngine: storageEngines) {
            storageEngineMap.put(storageEngine.getId(), storageEngine);
        }
        return storageEngineMap;
    }

    @Override
    public long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {
        long id = idGenerator.incrementAndGet();
        storageEngine.setId(id);
        if (storageChangeHook != null) {
            storageChangeHook.onChange(id, storageEngine);
        }
        return id;
    }

    @Override
    public void registerStorageChangeHook(StorageChangeHook hook) {
        if (hook != null) {
            storageChangeHook = hook;
        }
    }

    @Override
    public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
        return new HashMap<>();
    }

    @Override
    public void lockStorageUnit() throws MetaStorageException {
        storageUnitLock.lock();
    }

    @Override
    public String addStorageUnit() throws MetaStorageException {
        return Long.toString(idGenerator.incrementAndGet());
    }

    @Override
    public void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException {
        if (storageUnitChangeHook != null) {
            storageUnitChangeHook.onChange(storageUnitMeta.getId(), storageUnitMeta);
        }
    }

    @Override
    public void releaseStorageUnit() throws MetaStorageException {
        storageUnitLock.unlock();
    }

    @Override
    public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {
        if (storageUnitChangeHook != null) {
            storageUnitChangeHook = hook;
        }
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException {
        return new HashMap<>();
    }

    @Override
    public void lockFragment() throws MetaStorageException {
        fragmentUnitLock.lock();
    }

    @Override
    public void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
        if (fragmentChangeHook != null) {
            fragmentChangeHook.onChange(false, fragmentMeta);
        }
    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
        if (fragmentChangeHook != null) {
            fragmentChangeHook.onChange(true, fragmentMeta);
        }
    }

    @Override
    public void releaseFragment() throws MetaStorageException {
        fragmentUnitLock.unlock();
    }

    @Override
    public void registerFragmentChangeHook(FragmentChangeHook hook) {
        if (hook != null) {
            fragmentChangeHook = hook;
        }
    }
}
