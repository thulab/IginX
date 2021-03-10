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
package cn.edu.tsinghua.iginx.metadatav2;

import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeSeriesInterval;

import java.util.List;
import java.util.Map;

public interface IMetaManager {

    /**
     * 新增数据库节点
     */
    boolean addStorageEngine(StorageEngineMeta storageEngineMeta);

    /**
     * 获取所有的数据库实例的原信息（不包括每个数据库的分片列表）
     */
    List<StorageEngineMeta> getStorageEngineList();

    /**
     * 获取某个时序数据库的所有分片的元信息
     */
    List<FragmentReplicaMeta> getFragmentListByDatabase(long databaseId);

    /**
     * 获取所有活跃的 iginx 节点的元信息
     */
    List<IginxMeta> getIginxList();

    /**
     * 获取当前 iginx 节点的 ID
     */
    long getIginxId();

    /**
     获取某个时间序列区间的所有分片。
     */
    Map<String, List<FragmentMeta>> getFragmentListByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    /**
     获取某个时间区间的所有最新的分片（这些分片一定也都是未终结的分片）。
     */
    Map<String, FragmentMeta> getLatestFragmentListByTimeSeriesInterval(TimeSeriesInterval tsInterval);

    /**
     获取某个时间序列区间在某个时间区间的所有分片。
     */
    Map<String, List<FragmentMeta>> getFragmentListByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval,
                                                                                       TimeInterval timeInterval);

    /**
     获取某个时间序列的所有分片（按照分片时间戳排序）
     */
    List<FragmentMeta> getFragmentByTimeSeriesName(String tsName);

    /**
     获取某个时间序列的最新分片
     */
    FragmentMeta getLatestFragmentByTimeSeriesName(String tsName);


    /**
     获取某个时间序列在某个时间区间的所有分片（按照分片时间戳排序）
     */
    List<FragmentMeta> getFragmentByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval);

    /**
     创建时间分片
     */
    boolean createFragment(FragmentMeta fragment);

}
