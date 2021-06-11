package cn.edu.tsinghua.iginx.metadata;

import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;

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

public class FileMetaStorage implements IMetaStorage {

    private static FileMetaStorage INSTANCE = null;

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

    }

    @Override
    public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
        return null;
    }

    @Override
    public void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook) {

    }

    @Override
    public void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping) throws MetaStorageException {

    }

    @Override
    public Map<Long, IginxMeta> loadIginx() throws MetaStorageException {
        return null;
    }

    @Override
    public long registerIginx(IginxMeta iginx) throws MetaStorageException {
        return 0;
    }

    @Override
    public void registerIginxChangeHook(IginxChangeHook hook) {

    }

    @Override
    public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> storageEngines) throws MetaStorageException {
        return null;
    }

    @Override
    public long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {
        return 0;
    }

    @Override
    public void registerStorageChangeHook(StorageChangeHook hook) {

    }

    @Override
    public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
        return null;
    }

    @Override
    public void lockStorageUnit() throws MetaStorageException {

    }

    @Override
    public String addStorageUnit() throws MetaStorageException {
        return null;
    }

    @Override
    public void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException {

    }

    @Override
    public void releaseStorageUnit() throws MetaStorageException {

    }

    @Override
    public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {

    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException {
        return null;
    }

    @Override
    public void lockFragment() throws MetaStorageException {

    }

    @Override
    public void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException {

    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException {

    }

    @Override
    public void releaseFragment() throws MetaStorageException {

    }

    @Override
    public void registerFragmentChangeHook(FragmentChangeHook hook) {

    }
}
