package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FileMetaStorage implements IMetaStorage {

    private static final Logger logger = LoggerFactory.getLogger(FileMetaStorage.class);

    private static FileMetaStorage INSTANCE = null;

    private static final String PATH = ConfigDescriptor.getInstance().getConfig().getFileDataDir();

    private static final String IGINX_META_FILE = "iginx.log";

    private static final String STORAGE_META_FILE = "storage.log";

    private static final String SCHEMA_MAPPING_FILE = "schema.log";

    private static final String FRAGMENT_META_FILE = "fragment.log";

    private static final String STORAGE_UNIT_META_FILE = "storage_unit.log";

    private static final String CREATE = "create";

    private static final String UPDATE = "update";

    private static final String REMOVE = "remove";

    private final Lock storageUnitLock = new ReentrantLock();

    private final Lock fragmentUnitLock = new ReentrantLock();

    private SchemaMappingChangeHook schemaMappingChangeHook = null;

    private IginxChangeHook iginxChangeHook = null;

    private StorageChangeHook storageChangeHook = null;

    private StorageUnitChangeHook storageUnitChangeHook = null;

    private FragmentChangeHook fragmentChangeHook = null;

    private AtomicLong idGenerator = null; // 加载完数据之后赋值

    private Map<String, Map<String, Integer>> schemaMapping;

    public static FileMetaStorage getInstance() {
        if (INSTANCE == null) {
            synchronized (FileMetaStorage.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FileMetaStorage();
                }
            }
        }
        return INSTANCE;
    }

    public FileMetaStorage() {
        // 初始化文件
        try {
            if (Files.notExists(Paths.get(PATH, IGINX_META_FILE))) {
                Files.createFile(Paths.get(PATH, IGINX_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, STORAGE_META_FILE))) {
                Files.createFile(Paths.get(PATH, STORAGE_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, SCHEMA_MAPPING_FILE))) {
                Files.createFile(Paths.get(PATH, SCHEMA_MAPPING_FILE));
            }
            if (Files.notExists(Paths.get(PATH, FRAGMENT_META_FILE))) {
                Files.createFile(Paths.get(PATH, FRAGMENT_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, STORAGE_UNIT_META_FILE))) {
                Files.createFile(Paths.get(PATH, STORAGE_UNIT_META_FILE));
            }
        } catch (IOException e) {
            logger.error("encounter error when create file: ", e);
            System.exit(10);
        }
        preloadSchemaMapping();
    }

    private void preloadStorageUnit() {

    }

    private void preloadFragment() {

    }

    private void preloadStorageEngine() {

    }

    private void preloadIginx() {

    }

    private void preloadSchemaMapping() {
        schemaMapping = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, SCHEMA_MAPPING_FILE).toFile())))) {
            String line, params;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith(CREATE)) {
                    params = line.substring(CREATE.length() + 1);

                } else if (line.startsWith(UPDATE)) {
                    params = line.substring(UPDATE.length() + 1);
                } else if (line.startsWith(REMOVE)) {
                    params = line.substring(REMOVE.length() + 1);

                } else {
                    logger.error("unknown log content: " + line);
                }
            }
        } catch (IOException e) {
            logger.error("encounter error when read schema mapping log file: ", e);
            System.exit(10);
        }
    }

    @Override
    public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
        if (schemaMapping == null) { // schemaMapping 此前已经加载过了
            schemaMapping = new HashMap<>();
        }
        return schemaMapping;
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
        return null;
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
        return null;
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
        return null;
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
