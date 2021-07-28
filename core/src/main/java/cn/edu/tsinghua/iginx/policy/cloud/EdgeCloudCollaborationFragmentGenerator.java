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
package cn.edu.tsinghua.iginx.policy.cloud;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class EdgeCloudCollaborationFragmentGenerator {

    private final IMetaManager iMetaManager;

    public EdgeCloudCollaborationFragmentGenerator(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
    }

    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(long startTime) {
        List<IginxMeta> iginxList = iMetaManager.getIginxList();
        List<StorageEngineMeta> storageEngineList = iMetaManager.getStorageEngineList();

        List<String> edges = getEdges(iginxList, storageEngineList);
        Map<String, List<StorageEngineMeta>> groupedStorageEngineLists = storageEngineList.stream().collect(Collectors.groupingBy(e -> e.getExtraParams().getOrDefault("edgeName", "")));
        int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), groupedStorageEngineLists.getOrDefault("", Collections.emptyList()).size() + 1); // 最多备份数 = 云端服务节点树 + 1
        List<Pair<Long, Integer>> storageEngineFragmentCounts = storageEngineList.stream().filter(e -> e.getExtraParams().getOrDefault("edgeName", "").equals(""))
                .map(e -> new Pair<>(e.getId(), e.getStorageUnitList().size())).collect(Collectors.toList()); // 记录每个云端存储单元已经分配的分片的个数

        List<FragmentMeta> fragmentList = new ArrayList<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();
        for (String edge : edges) {
            List<StorageEngineMeta> edgeStorageEngineList = groupedStorageEngineLists.get(edge);
            if (edgeStorageEngineList == null) { // 边缘端部署了 iginx，没有部署 iotdb，所有的备份均存储在云端
                List<Long> storageEngineIds = selectStorageEngines(storageEngineFragmentCounts, replicaNum);
                Pair<FragmentMeta, StorageUnitMeta> pair = generateFragmentAndStorageUnit(lowerBound(edge), upperBound(edge), startTime, storageEngineIds);
                fragmentList.add(pair.k);
                storageUnitList.add(pair.v);
            } else { // 边缘端部署了 iginx 也部署了 iotdb，每个分片在本地存储一个备份
                List<String> partitions = partitionWithinEdge(edge, edgeStorageEngineList.size());
                for (int i = 0; i < partitions.size() - 1; i++) {
                    List<Long> storageEngineIds = new ArrayList<>(Collections.singletonList(edgeStorageEngineList.get(i).getId()));
                    storageEngineIds.addAll(selectStorageEngines(storageEngineFragmentCounts, replicaNum - 1)); // 主本已经有了
                    Pair<FragmentMeta, StorageUnitMeta> pair = generateFragmentAndStorageUnit(partitions.get(i), partitions.get(i + 1), startTime, storageEngineIds);
                    fragmentList.add(pair.k);
                    storageUnitList.add(pair.v);
                }
            }

        }
        // 对于边缘端形成的空隙
        if (edges.size() == 0) {
            // 只有一个大空隙
            List<Long> storageEngineIds = selectStorageEngines(storageEngineFragmentCounts, replicaNum);
            Pair<FragmentMeta, StorageUnitMeta> pair = generateFragmentAndStorageUnit(null, null, startTime, storageEngineIds);
            fragmentList.add(pair.k);
            storageUnitList.add(pair.v);
        } else {
            // 上空隙
            List<Long> storageEngineIds = selectStorageEngines(storageEngineFragmentCounts, replicaNum);
            Pair<FragmentMeta, StorageUnitMeta> pair = generateFragmentAndStorageUnit(null,  lowerBound(edges.get(0)), startTime, storageEngineIds);
            fragmentList.add(pair.k);
            storageUnitList.add(pair.v);

            // 下空隙
            storageEngineIds = selectStorageEngines(storageEngineFragmentCounts, replicaNum);
            pair = generateFragmentAndStorageUnit(upperBound(edges.get(edges.size() - 1)), null, startTime, storageEngineIds);
            fragmentList.add(pair.k);
            storageUnitList.add(pair.v);

            // 中间的空隙
            for (int i = 0; i < edges.size() - 1; i++) {
                storageEngineIds = selectStorageEngines(storageEngineFragmentCounts, replicaNum);
                pair = generateFragmentAndStorageUnit(upperBound(edges.get(i)), lowerBound(edges.get(i + 1)), startTime, storageEngineIds);
                fragmentList.add(pair.k);
                storageUnitList.add(pair.v);
            }
        }
        return new Pair<>(fragmentList, storageUnitList);
    }

    private List<String> getEdges(List<IginxMeta> iginxList, List<StorageEngineMeta> storageEngineList) {
        Set<String> edges = new HashSet<>();
        for (IginxMeta iginx: iginxList) {
            String edge = iginx.getExtraParams().get("edge_name");
            if (edge != null && !edge.equals("")) {
                edges.add(edge);
            }
        }
        for (StorageEngineMeta storageEngine: storageEngineList) {
            String edge = storageEngine.getExtraParams().get("edgeName");
            if (edge != null && !edge.equals("")) {
                edges.add(edge);
            }
        }
        return edges.stream().sorted().collect(Collectors.toList());
    }

    private List<Long> selectStorageEngines(List<Pair<Long, Integer>> storageEngineFragmentCounts, int count) {
        storageEngineFragmentCounts = storageEngineFragmentCounts.stream().sorted(Comparator.comparingInt(o -> o.v)).collect(Collectors.toList());
        List<Long> storageEngines = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            if (i >= storageEngineFragmentCounts.size()) {
                break;
            }
            storageEngines.add(storageEngineFragmentCounts.get(i).k);
            storageEngineFragmentCounts.get(i).v++;
        }
        return storageEngines;
    }

    private List<String> partitionWithinEdge(String edge, int partition) {
        if (partition < 1) {
            return Collections.emptyList();
        }
        return Arrays.asList(lowerBound(edge), upperBound(edge));
    }

    // 根据时间和序列区间以及一组 storageEngineList 来生成分片和存储单元
    private Pair<FragmentMeta, StorageUnitMeta> generateFragmentAndStorageUnit(String startPath, String endPath, long startTime, List<Long> storageEngineList) {
        String masterId = RandomStringUtils.randomAlphanumeric(16);
        StorageUnitMeta storageUnit = new StorageUnitMeta(masterId, storageEngineList.get(0), masterId, true);
        FragmentMeta fragment = new FragmentMeta(startPath, endPath, startTime, Long.MAX_VALUE, masterId);
        for (int i = 1; i < storageEngineList.size(); i++) {
            storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineList.get(i), masterId, false));
        }
        return new Pair<>(fragment, storageUnit);
    }

    private static String lowerBound(String string) {
        return string + '.' + (char)('A' - 1);
    }

    private static String upperBound(String string) {
        return string + '.' + (char)('z' + 1);
    }

}
