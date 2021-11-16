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
import cn.edu.tsinghua.iginx.metadata.entity.FragmentStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesIntervalStatistics;
import cn.edu.tsinghua.iginx.metadata.hook.ActiveFragmentStatisticsChangeHook;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.metadata.hook.ActiveSeparatorStatisticsChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.ActiveStorageEngineStatisticsChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.ActiveTimeSeriesIntervalStatisticsChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.FragmentChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.IginxChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.MinimalActiveIginxStatisticsChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.ReshardCounterChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.ReshardStatusHook;
import cn.edu.tsinghua.iginx.metadata.hook.SchemaMappingChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.UserChangeHook;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;

import java.util.List;
import java.util.Map;
import java.util.Set;

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

    List<UserMeta> loadUser(UserMeta userMeta) throws MetaStorageException;

    void registerUserChangeHook(UserChangeHook hook);

    void addUser(UserMeta userMeta) throws MetaStorageException;

    void updateUser(UserMeta userMeta) throws MetaStorageException;

    void removeUser(String username) throws MetaStorageException;

    Map<FragmentMeta, FragmentStatistics> loadActiveFragmentStatistics() throws MetaStorageException;

    void lockActiveFragmentStatistics() throws MetaStorageException;

    void addActiveFragmentStatistics(long id, Map<FragmentMeta, FragmentStatistics> deltaActiveFragmentStatistics) throws MetaStorageException;

    void addInactiveFragmentStatistics(Map<FragmentMeta, FragmentStatistics> activeFragmentStatistics, long endTime) throws MetaStorageException;

    void releaseActiveFragmentStatistics() throws MetaStorageException;

    void removeActiveFragmentStatistics() throws MetaStorageException;

    void registerActiveFragmentStatisticsChangeHook(ActiveFragmentStatisticsChangeHook hook);

    void lockMinimalActiveIginxStatistics() throws MetaStorageException;

    void addMinimalActiveIginxStatistics(double density) throws MetaStorageException;

    void releaseMinimalActiveIginxStatistics() throws MetaStorageException;

    void registerMinimalActiveIginxStatisticsChangeHook(MinimalActiveIginxStatisticsChangeHook hook) throws MetaStorageException;

    void lockActiveSeparatorStatistics() throws MetaStorageException;

    void addActiveSeparatorStatistics(long id, Set<String> separators) throws MetaStorageException;

    void addMergedActiveSeparatorStatistics(Set<String> separators) throws MetaStorageException;

    void releaseActiveSeparatorStatistics() throws MetaStorageException;

    void registerActiveSeparatorStatisticsChangeHook(ActiveSeparatorStatisticsChangeHook hook);

    void lockActiveStorageEngineStatistics() throws MetaStorageException;

    void addActiveStorageEngineStatistics(long id, Map<Long, StorageEngineStatistics> activeStorageEngineStatistics) throws MetaStorageException;

    void releaseActiveStorageEngineStatistics() throws MetaStorageException;

    void registerActiveStorageEngineStatisticsChangeHook(ActiveStorageEngineStatisticsChangeHook hook);

    void lockActiveTimeSeriesIntervalStatistics() throws MetaStorageException;

    void addActiveTimeSeriesIntervalStatistics(long id, Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> statisticsMap) throws MetaStorageException;

    void releaseActiveTimeSeriesIntervalStatistics() throws MetaStorageException;

    void registerActiveTimeSeriesIntervalStatisticsChangeHook(ActiveTimeSeriesIntervalStatisticsChangeHook hook);

    // 提议进入重分片流程，返回值为 true 代表提议成功，本节点成为 proposer；为 false 代表提议失败，说明已有其他节点提议成功
    boolean proposeToReshard() throws MetaStorageException;

    void lockReshardStatus() throws MetaStorageException;

    void updateReshardStatus(ReshardStatus status) throws MetaStorageException;

    void releaseReshardStatus() throws MetaStorageException;

    void removeReshardStatus() throws MetaStorageException;

    void registerReshardStatusHook(ReshardStatusHook hook);

    void lockReshardCounter() throws MetaStorageException;

    void incrementReshardCounter() throws MetaStorageException;

    void resetReshardCounter() throws MetaStorageException;

    void releaseReshardCounter() throws MetaStorageException;

    void removeReshardCounter() throws MetaStorageException;

    void registerReshardCounterChangeHook(ReshardCounterChangeHook hook);

}
