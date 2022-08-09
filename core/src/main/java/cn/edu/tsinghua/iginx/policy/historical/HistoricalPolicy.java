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
package cn.edu.tsinghua.iginx.policy.historical;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import cn.edu.tsinghua.iginx.policy.Utils;
import cn.edu.tsinghua.iginx.policy.naive.Sampler;
import cn.edu.tsinghua.iginx.sql.statement.DataStatement;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.sql.statement.StatementType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

// 若要使用历史数据分片策略，须在config.properties中添加以下配置：
// # 历史数据中常见的路径前缀列表，使用','分隔
// historicalPrefixList=000,001,002,AAA
// # 预期存储单元数量
// expectedStorageUnitNum=50
public class HistoricalPolicy implements IPolicy {

    private static final Logger logger = LoggerFactory.getLogger(HistoricalPolicy.class);

    protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
    private IMetaManager iMetaManager;
    private Sampler sampler;
    private List<String> suffixList = new ArrayList<>();

    @Override
    public void notify(DataStatement statement) {
        List<String> pathList = Utils.getPathListFromStatement(statement);
        if (pathList != null && !pathList.isEmpty()) {
            sampler.updatePrefix(new ArrayList<>(Arrays.asList(pathList.get(0), pathList.get(pathList.size() - 1))));
        }
    }

    @Override
    public void init(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
        this.sampler = Sampler.getInstance();
        StorageEngineChangeHook hook = getStorageEngineChangeHook();
        if (hook != null) {
            iMetaManager.registerStorageEngineChangeHook(hook);
        }

        for (int i = 0; i <= 9; i++) {
            suffixList.add(String.valueOf(i));
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            suffixList.add(String.valueOf(c));
        }
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            // 哪台机器加了分片，哪台机器初始化，并且在批量添加的时候只有最后一个存储引擎才会导致扩容发生
            if (before == null && after != null && after.getCreatedBy() == iMetaManager.getIginxId() && after.isNeedReAllocate()) {
                needReAllocate.set(true);
                logger.info("新的可写节点进入集群，集群需要重新分片");
            }
            // TODO: 针对节点退出的情况缩容
        };
    }

    @Override
    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(DataStatement statement) {
        List<FragmentMeta> fragmentList = new ArrayList<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();

        TimeInterval timeInterval = new TimeInterval(0, Long.MAX_VALUE);
        List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();
        int storageEngineNum = storageEngineList.size();
        int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);

        String[] historicalPrefixList = ConfigDescriptor.getInstance().getConfig().getHistoricalPrefixList().split(",");
        Arrays.sort(historicalPrefixList);
        int expectedStorageUnitNum = ConfigDescriptor.getInstance().getConfig().getExpectedStorageUnitNum();

        List<String> prefixList = new ArrayList<>();
        List<TimeSeriesInterval> timeSeriesIntervalList = new ArrayList<>();
        for (String historicalPrefix : historicalPrefixList) {
            for (String suffix : suffixList) {
                if (!prefixList.contains(historicalPrefix + suffix)) {
                    prefixList.add(historicalPrefix + suffix);
                }
            }
        }
        Collections.sort(prefixList);
        int prefixNum = prefixList.size();
        prefixList.add(null);
        timeSeriesIntervalList.add(new TimeSeriesInterval(null, prefixList.get(0)));
        for (int i = 0; i < expectedStorageUnitNum; i++) {
            timeSeriesIntervalList.add(new TimeSeriesInterval(
                prefixList.get(i * prefixNum / expectedStorageUnitNum),
                prefixList.get((i + 1) * prefixNum / expectedStorageUnitNum)));
        }

        String masterId;
        StorageUnitMeta storageUnit;
        for (int i = 0; i < timeSeriesIntervalList.size(); i++) {
            masterId = RandomStringUtils.randomAlphanumeric(16);
            storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(i % storageEngineNum).getId(), masterId, true);
            for (int j = i + 1; j < i + replicaNum; j++) {
                storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(j % storageEngineNum).getId(), masterId, false));
            }
            storageUnitList.add(storageUnit);
            fragmentList.add(new FragmentMeta(timeSeriesIntervalList.get(i), timeInterval, masterId));
        }

        return new Pair<>(fragmentList, storageUnitList);
    }

    @Override
    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(DataStatement statement) {
        long startTime;
        if (statement.getType() == StatementType.INSERT) {
            startTime = ((InsertStatement) statement).getEndTime() + TimeUnit.SECONDS.toMillis(ConfigDescriptor.getInstance().getConfig().getDisorderMargin()) * 2 + 1;
        } else {
            throw new IllegalArgumentException("function generateFragmentsAndStorageUnits only use insert statement for now.");
        }
        List<String> prefixList = sampler.samplePrefix(iMetaManager.getWriteableStorageEngineList().size() - 1);

        List<FragmentMeta> fragmentList = new ArrayList<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();

        int storageEngineNum = iMetaManager.getStorageEngineNum();
        int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
        List<Long> storageEngineIdList;
        Pair<FragmentMeta, StorageUnitMeta> pair;
        int index = 0;

        // [startTime, +∞) & [startPath, endPath)
        int splitNum = Math.max(Math.min(storageEngineNum, prefixList.size() - 1), 0);
        for (int i = 0; i < splitNum; i++) {
            storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
            pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(prefixList.get(i), prefixList.get(i + 1), startTime, Long.MAX_VALUE, storageEngineIdList);
            fragmentList.add(pair.k);
            storageUnitList.add(pair.v);
        }

        // [startTime, +∞) & [endPath, null)
        storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
        pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(prefixList.get(prefixList.size() - 1), null, startTime, Long.MAX_VALUE, storageEngineIdList);
        fragmentList.add(pair.k);
        storageUnitList.add(pair.v);

        // [startTime, +∞) & (null, startPath)
        storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
        pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(null, prefixList.get(0), startTime, Long.MAX_VALUE, storageEngineIdList);
        fragmentList.add(pair.k);
        storageUnitList.add(pair.v);

        return new Pair<>(fragmentList, storageUnitList);
    }

    private List<Long> generateStorageEngineIdList(int startIndex, int num) {
        List<Long> storageEngineIdList = new ArrayList<>();
        List<StorageEngineMeta> storageEngines = iMetaManager.getWriteableStorageEngineList();
        for (int i = startIndex; i < startIndex + num; i++) {
            storageEngineIdList.add(storageEngines.get(i % storageEngines.size()).getId());
        }
        return storageEngineIdList;
    }

    private Pair<FragmentMeta, StorageUnitMeta> generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(String startPath, String endPath, long startTime, long endTime, List<Long> storageEngineList) {
        String masterId = RandomStringUtils.randomAlphanumeric(16);
        StorageUnitMeta storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(0), masterId, true);
        FragmentMeta fragment = new FragmentMeta(startPath, endPath, startTime, endTime, masterId);
        for (int i = 1; i < storageEngineList.size(); i++) {
            storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(i), masterId, false));
        }
        return new Pair<>(fragment, storageUnit);
    }

    @Override
    public boolean isNeedReAllocate() {
        return needReAllocate.getAndSet(false);
    }

    @Override
    public void setNeedReAllocate(boolean needReAllocate) {
        this.needReAllocate.set(needReAllocate);
    }
}
