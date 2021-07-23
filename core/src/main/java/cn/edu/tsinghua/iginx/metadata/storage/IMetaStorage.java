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
package cn.edu.tsinghua.iginx.metadata.storage;

import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.ActiveFragmentStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.hook.ActiveFragmentStatisticsHook;
import cn.edu.tsinghua.iginx.metadata.hook.FragmentChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.IginxChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.SchemaMappingChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitChangeHook;

import java.util.List;
import java.util.Map;

public interface IMetaStorage {

    Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException;

    void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook);

    void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping) throws MetaStorageException;

    Map<Long, IginxMeta> loadIginx() throws MetaStorageException;

    long registerIginx(IginxMeta iginx) throws MetaStorageException;

    void registerIginxChangeHook(IginxChangeHook hook);

    Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> storageEngines) throws MetaStorageException;

    long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException;

    void registerStorageChangeHook(StorageChangeHook hook);

    Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException;

    void lockStorageUnit() throws MetaStorageException;

    String addStorageUnit() throws MetaStorageException;

    void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException;

    void releaseStorageUnit() throws MetaStorageException;

    void registerStorageUnitChangeHook(StorageUnitChangeHook hook);

    Map<TimeSeriesInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException;

    void lockFragment() throws MetaStorageException;

    void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException;

    void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException;

    void releaseFragment() throws MetaStorageException;

    void registerFragmentChangeHook(FragmentChangeHook hook);

    Map<FragmentMeta, ActiveFragmentStatistics> loadActiveFragmentStatistics() throws MetaStorageException;

    void lockActiveFragmentStatistics() throws MetaStorageException;

    void updateActiveFragmentStatistics(Map<FragmentMeta, ActiveFragmentStatistics> activeFragmentStatistics) throws MetaStorageException;

    void addActiveFragmentStatistics(Map<FragmentMeta, ActiveFragmentStatistics> activeFragmentStatistics) throws MetaStorageException;

    void releaseActiveFragmentStatisticsFragment() throws MetaStorageException;

    void registerActiveFragmentStatisticsHook(ActiveFragmentStatisticsHook hook);

    boolean proposeToReshard() throws MetaStorageException;
}
