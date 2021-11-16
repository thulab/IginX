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
package cn.edu.tsinghua.iginx.policy.naive;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.policy.IFragmentGenerator;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class NaiveFragmentGenerator implements IFragmentGenerator {

    private IMetaManager iMetaManager;

    public NaiveFragmentGenerator(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
    }

    @Override
    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(List<String> paths, TimeInterval timeInterval) {
        if (ConfigDescriptor.getInstance().getConfig().getClients().indexOf(",") > 0) {
            Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> pair = generateInitialFragmentsAndStorageUnitsByClients(paths, timeInterval);
            return new Pair<>(pair.k.values().stream().flatMap(List::stream).collect(Collectors.toList()), pair.v);
        } else
            return generateInitialFragmentsAndStorageUnitsDefault(paths, timeInterval);
    }

    /**
     * This storage unit initialization method is used when no information about workloads is provided
     */
    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnitsDefault(List<String> paths, TimeInterval timeInterval) {
        List<FragmentMeta> fragmentList = new ArrayList<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();

        int storageEngineNum = iMetaManager.getStorageEngineNum();
        int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
        List<Long> storageEngineIdList;
        Pair<FragmentMeta, StorageUnitMeta> pair;
        int index = 0;

        // [startTime, +∞) & [startPath, endPath)
        int splitNum = Math.max(Math.min(storageEngineNum, paths.size() - 1), 0);
        for (int i = 0; i < splitNum; i++) {
            storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
            pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(paths.get(i * (paths.size() - 1) / splitNum), paths.get((i + 1) * (paths.size() - 1) / splitNum), timeInterval.getStartTime(), Long.MAX_VALUE, storageEngineIdList);
            fragmentList.add(pair.k);
            storageUnitList.add(pair.v);
        }

        // [startTime, +∞) & [endPath, null)
        storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
        pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(paths.get(paths.size() - 1), null, timeInterval.getStartTime(), Long.MAX_VALUE, storageEngineIdList);
        fragmentList.add(pair.k);
        storageUnitList.add(pair.v);

        // [0, startTime) & (-∞, +∞)
        // 一般情况下该范围内几乎无数据，因此作为一个分片处理
        // TODO 考虑大规模插入历史数据的情况
        if (timeInterval.getStartTime() != 0) {
            storageEngineIdList = generateStorageEngineIdList(index++, replicaNum);
            pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(null, null, 0, timeInterval.getStartTime(), storageEngineIdList);
            fragmentList.add(pair.k);
            storageUnitList.add(pair.v);
        }

        // [startTime, +∞) & (null, startPath)
        storageEngineIdList = generateStorageEngineIdList(index, replicaNum);
        pair = generateFragmentAndStorageUnitByTimeSeriesIntervalAndTimeInterval(null, paths.get(0), timeInterval.getStartTime(), Long.MAX_VALUE, storageEngineIdList);
        fragmentList.add(pair.k);
        storageUnitList.add(pair.v);

        return new Pair<>(fragmentList, storageUnitList);
    }

    /**
     * This storage unit initialization method is used when clients are provided, such as in TPCx-IoT tests
     */
    public Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnitsByClients(List<String> paths, TimeInterval timeInterval) {
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();

        List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();
        int storageEngineNum = storageEngineList.size();

        String[] clients = ConfigDescriptor.getInstance().getConfig().getClients().split(",");
        int instancesNumPerClient = ConfigDescriptor.getInstance().getConfig().getInstancesNumPerClient();
        int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), storageEngineNum);
        int fragmentNum = clients.length * 30 / replicaNum;
        String[] prefixes = new String[fragmentNum + 1];
        for (int i = 0; i < fragmentNum + 1; i++) {
            prefixes[i] = "d_" + String.format("%04d", instancesNumPerClient * i / fragmentNum);
        }
        Arrays.sort(prefixes);

        List<FragmentMeta> fragmentMetaList;
        String masterId;
        StorageUnitMeta storageUnit;
        for (int i = 0; i < clients.length; i++) {
            for (int j = 0; j < 30 / replicaNum; j++) {
                fragmentMetaList = new ArrayList<>();
                masterId = RandomStringUtils.randomAlphanumeric(16);
                storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(i % storageEngineNum).getId(), masterId, true);
                for (int k = i + 1; k < i + replicaNum; k++) {
                    storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(k % storageEngineNum).getId(), masterId, false));
                }
                storageUnitList.add(storageUnit);
                int index = i * 30 / replicaNum + j;
                fragmentMetaList.add(new FragmentMeta(prefixes[index], prefixes[index + 1], 0, Long.MAX_VALUE, masterId));
                fragmentMap.put(new TimeSeriesInterval(prefixes[index], prefixes[index + 1]), fragmentMetaList);
            }
        }

        fragmentMetaList = new ArrayList<>();
        masterId = RandomStringUtils.randomAlphanumeric(16);
        storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(0).getId(), masterId, true);
        for (int i = 1; i < replicaNum; i++) {
            storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(i).getId(), masterId, false));
        }
        storageUnitList.add(storageUnit);
        fragmentMetaList.add(new FragmentMeta(null, prefixes[0], 0, Long.MAX_VALUE, masterId));
        fragmentMap.put(new TimeSeriesInterval(null, prefixes[0]), fragmentMetaList);

        fragmentMetaList = new ArrayList<>();
        masterId = RandomStringUtils.randomAlphanumeric(16);
        storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(storageEngineNum - 1).getId(), masterId, true);
        for (int i = 1; i < replicaNum; i++) {
            storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(storageEngineNum - 1 - i).getId(), masterId, false));
        }
        storageUnitList.add(storageUnit);
        fragmentMetaList.add(new FragmentMeta(prefixes[fragmentNum], null, 0, Long.MAX_VALUE, masterId));
        fragmentMap.put(new TimeSeriesInterval(prefixes[fragmentNum], null), fragmentMetaList);

        return new Pair<>(fragmentMap, storageUnitList);
    }

    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(List<String> prefixList, long startTime) {
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

    @Override
    public void init(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
    }

    @Override
    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnitsForResharding(long startTime) {
        // 无新增 storage unit
        List<FragmentMeta> fragments = new ArrayList<>();
        List<StorageUnitMeta> storageUnits = new ArrayList<>();
        Map<TimeSeriesInterval, FragmentMeta> latestFragments = iMetaManager.getLatestFragmentMap();
        for (FragmentMeta oldFragment : latestFragments.values()) {
            StorageUnitMeta masterStorageUnit = iMetaManager.getStorageUnit(oldFragment.getMasterStorageUnitId());
            FragmentMeta newFragment = new FragmentMeta(
                    oldFragment.getTsInterval().getStartTimeSeries(),
                    oldFragment.getTsInterval().getEndTimeSeries(),
                    startTime,
                    Long.MAX_VALUE,
                    masterStorageUnit
            );
            newFragment.setInitialFragment(false);
            fragments.add(newFragment);
        }
        fragments.get(fragments.size() - 1).setLastOfBatch(true);

//        // 有新增 storage unit
//        List<FragmentMeta> fragments = new ArrayList<>();
//        List<StorageUnitMeta> storageUnits = new ArrayList<>();
//        Map<TimeSeriesInterval, FragmentMeta> latestFragments = iMetaManager.getLatestFragmentMap();
//        for (FragmentMeta oldFragment : latestFragments.values()) {
//            StorageUnitMeta masterStorageUnit = iMetaManager.getStorageUnit(oldFragment.getMasterStorageUnitId());
//            String masterId = RandomStringUtils.randomAlphanumeric(16);
//            StorageUnitMeta storageUnit = new StorageUnitMeta(masterId, masterStorageUnit.getStorageEngineId(), masterId, true);
//            for (StorageUnitMeta replica : masterStorageUnit.getReplicas()) {
//                storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), replica.getStorageEngineId(), masterId, false));
//            }
//            storageUnits.add(storageUnit);
//
//            FragmentMeta newFragment = new FragmentMeta(
//                    oldFragment.getTsInterval().getStartTimeSeries(),
//                    oldFragment.getTsInterval().getEndTimeSeries(),
//                    startTime,
//                    Long.MAX_VALUE,
//                    masterId
//            );
//            newFragment.setInitialFragment(false);
//            fragments.add(newFragment);
//        }
//        fragments.get(fragments.size() - 1).setLastOfBatch(true);
//        storageUnits.get(storageUnits.size() - 1).setLastOfBatch(true);

        // TODO 利用 FragmentStatistics
//        for (Map.Entry<FragmentMeta, FragmentStatistics> entry : iMetaManager.getActiveFragmentStatistics().entrySet()) {
//            FragmentMeta fragment = entry.getKey();
//            StorageUnitMeta masterStorageUnit = iMetaManager.getStorageUnit(fragment.getMasterStorageUnitId());
//            String masterId = RandomStringUtils.randomAlphanumeric(16);
//            StorageUnitMeta storageUnit = new StorageUnitMeta(masterId, masterStorageUnit.getStorageEngineId(), masterId, true);
//            for (StorageUnitMeta replica : masterStorageUnit.getReplicas()) {
//                storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), replica.getStorageEngineId(), masterId, false));
//            }
//            storageUnits.add(storageUnit);
//
//            FragmentMeta newFragment = new FragmentMeta(
//                    fragment.getTsInterval().getStartTimeSeries(),
//                    fragment.getTsInterval().getEndTimeSeries(),
//                    startTime,
//                    Long.MAX_VALUE,
//                    masterId
//            );
//            newFragment.setInitialFragment(false);
//            fragments.add(newFragment);
//        }
        return new Pair<>(fragments, storageUnits);
    }

    private List<Long> generateStorageEngineIdList(int startIndex, int num) {
        List<Long> storageEngineIdList = new ArrayList<>();
        List<StorageEngineMeta> storageEngines = iMetaManager.getStorageEngineList();
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

}
