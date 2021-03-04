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

import java.util.List;

public interface IMetaManager {

    /**
     * 获取当前所有的数据库实例的元信息
     * @return 所有数据库实例的元信息（不包括每个数据库的分片列表）
     */
    List<DatabaseMeta> getDatabaseList();

    /**
     * 获取某个时序数据库的所有分片的元信息
     * @param databaseId 时序数据库的 id
     * @return 时序数据库的所有分片
     */
    List<FragmentReplicaMeta> getFragmentListByDatabase(long databaseId);

    /**
     * 获取所有活跃的 iginx 节点的元信息
     * 创建新分片、判断是否需要转发请求时可能用到
     * @return 活跃的 iginx 节点的元信息
     */
    List<IginxMeta> getIginxList();

    /**
     * 获取某个 key 指定时间区间的所有主备分片
     * @param key 分片 key
     * @param startTime 开始时间（包含）
     * @param endTime 结束时间（包含）
     * @return 给定 key 指定时间区间的所有主备分片
     */
    List<FragmentMeta> getFragmentListByKeyAndTimeInterval(String key, long startTime, long endTime);

    /**
     * 获取某个 key 的所有主备分片
     * @param key 分片 key
     * @return 给定 key 所有主备分片
     */
    List<FragmentMeta> getFragmentListByKey(String key);

    /**
     * 获取某个 key 指定时间的主备分片
     * @param key 分片 key
     * @param time 给定时间（包含）
     * @return 给定 key 给定时间主备分片
     */
    FragmentMeta getFragmentListByKeyAndTime(String key, long time);

    /**
     * 针对某个前缀创造或追加一个分片
     * 首次创建分片必须从时刻 0 开始
     * 当追加分片时候，会自动将前一个分片的结束时间设置为当前分片的开始时间
     * @param fragmentMeta 要创建的分片
     */
    boolean createFragment(FragmentMeta fragmentMeta);

    /**
     * 为新创建的分片选择数据库实例
     * @return 选出的数据库实例 Id 列表
     */
    List<Long> chooseDatabaseIdsForNewFragment();

}
