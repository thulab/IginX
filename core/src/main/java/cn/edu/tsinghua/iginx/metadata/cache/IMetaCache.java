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
package cn.edu.tsinghua.iginx.metadata.cache;

import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.simple.TimeSeriesCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface IMetaCache {

    boolean enableFragmentCacheControl();

    // 分片相关的缓存读写接口
    void initFragment(Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap);

    void addFragment(FragmentMeta fragmentMeta);

    void updateFragment(FragmentMeta fragmentMeta);

    Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    List<FragmentMeta> getDummyFragmentsByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMap();

    Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval, TimeInterval timeInterval);

    List<FragmentMeta> getDummyFragmentsByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval, TimeInterval timeInterval);

    List<FragmentMeta> getFragmentListByTimeSeriesName(String tsName);

    FragmentMeta getLatestFragmentByTimeSeriesName(String tsName);

    List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval);

    boolean hasFragment();

    long getFragmentMinTimestamp();

    // 数据单元相关的缓存读写接口
    boolean hasStorageUnit();

    void initStorageUnit(Map<String, StorageUnitMeta> storageUnits);

    StorageUnitMeta getStorageUnit(String id);

    Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids);

    List<StorageUnitMeta> getStorageUnits();

    void addStorageUnit(StorageUnitMeta storageUnitMeta);

    void updateStorageUnit(StorageUnitMeta storageUnitMeta);

    // iginx 相关的缓存读写接口
    List<IginxMeta> getIginxList();

    void addIginx(IginxMeta iginxMeta);

    void removeIginx(long id);

    // 数据后端相关的缓存读写接口
    void addStorageEngine(StorageEngineMeta storageEngineMeta);

    List<StorageEngineMeta> getStorageEngineList();

    StorageEngineMeta getStorageEngine(long id);

    // schemaMapping 相关的缓存读写接口
    Map<String, Integer> getSchemaMapping(String schema);

    int getSchemaMappingItem(String schema, String key);

    void removeSchemaMapping(String schema);

    void removeSchemaMappingItem(String schema, String key);

    void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping);

    void addOrUpdateSchemaMappingItem(String schema, String key, int value);

    void addOrUpdateUser(UserMeta userMeta);

    void removeUser(String username);

    List<UserMeta> getUser();

    List<UserMeta> getUser(List<String> usernames);

    void timeSeriesIsUpdated(int node, int version);

    void saveTimeSeriesData(InsertStatement statement);

    List<TimeSeriesCalDO> getMaxValueFromTimeSeries();

    double getSumFromTimeSeries();

    Map<Integer, Integer> getTimeseriesVersionMap();

    void addOrUpdateTransformTask(TransformTaskMeta transformTask);

    void dropTransformTask(String className);

    TransformTaskMeta getTransformTask(String className);

    List<TransformTaskMeta> getTransformTasks();
}
