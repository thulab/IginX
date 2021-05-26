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

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;

import java.util.List;
import java.util.Map;

public interface IMetaManager {

    /**
     * 新增存储引擎节点
     */
    boolean addStorageEngine(StorageEngineMeta storageEngineMeta);

    /**
     * 获取所有的存储引擎实例的原信息（不包括每个存储引擎的分片列表）
     */
    List<StorageEngineMeta> getStorageEngineList();

    /**
     * 获取某个存储引擎的所有分片的元信息
     */
    List<FragmentReplicaMeta> getFragmentListByStorageEngineId(long storageEngineId);

    /**
     * 获取所有活跃的 iginx 节点的元信息
     */
    List<IginxMeta> getIginxList();

    /**
     * 获取当前 iginx 节点的 ID
     */
    long getIginxId();

    /**
     * 获取某个时间序列区间的所有分片
     */
    Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    /**
     * 获取某个时间区间的所有最新的分片（这些分片一定也都是未终结的分片）
     */
    Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    /**
     * 获取全部最新的分片
     */
    Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMap();

    /**
     * 获取某个时间序列区间在某个时间区间的所有分片。
     */
    Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval,
                                                                                                  TimeInterval timeInterval);

    /**
     * 获取某个时间序列的所有分片（按照分片时间戳排序）
     */
    List<FragmentMeta> getFragmentListByTimeSeriesName(String tsName);

    /**
     * 获取某个时间序列的最新分片
     */
    FragmentMeta getLatestFragmentByTimeSeriesName(String tsName);


    /**
     * 获取某个时间序列在某个时间区间的所有分片（按照分片时间戳排序）
     */
    List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval);

    /**
     * 创建分片
     */
    boolean createFragments(List<FragmentMeta> fragments);

    /**
     * 是否已经创建过分片
     */
    boolean hasFragment();

    /**
     * 尝试创建初始分片
     */
    boolean tryCreateInitialFragments(List<FragmentMeta> initialFragments);

    /**
     * 为新创建的分片选择存储引擎实例
     *
     * @return 选出的存储引擎实例 Id 列表
     */
    List<Long> chooseStorageEngineIdListForNewFragment();

    /**
     * 为 DatabasePlan 选择存储引擎实例
     *
     * @return 选出的存储引擎实例 Id
     */
    long chooseStorageEngineIdForDatabasePlan();

    Map<TimeSeriesInterval, List<FragmentMeta>> generateFragments(String startPath, long startTime);

    List<FragmentMeta> generateFragments(List<String> prefixList, long startTime);

    void registerStorageEngineChangeHook(StorageEngineChangeHook hook);

    /**
     * 增加或更新 schemaMappings
     *
     * @param schema        待更新的 schema 名
     * @param schemaMapping 待更新的 schema，如果 schema 为空，则表示删除给定的 schema
     */
    void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping);

    /**
     * 增加或更新某个给定 schemaMapping 的数据项
     *
     * @param schema 待更新的 schema 名
     * @param key    待更新的数据项的名
     * @param value  待更新的数据项，如果 value = -1 表示删除该数据项
     */
    void addOrUpdateSchemaMappingItem(String schema, String key, int value);

    /**
     * 获取某个 schemaMapping
     *
     * @param schema 需要获取的 schema
     * @return schema。如果不存在则返回空指针
     */
    Map<String, Integer> getSchemaMapping(String schema);

    /**
     * 获取某个 schemaMapping 中的数据项
     *
     * @param schema 需要获取的 schema
     * @param key    需要获取的数据项的名
     * @return 数据项的值。如果不存在则返回 -1
     */
    int getSchemaMappingItem(String schema, String key);

}
