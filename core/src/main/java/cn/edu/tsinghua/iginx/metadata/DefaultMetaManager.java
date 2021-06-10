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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultMetaManager implements IMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMetaManager.class);

    private final IMetaCache cache;

    private final IMetaStorage storage;

    private long id;

    private final List<StorageEngineChangeHook> storageEngineChangeHooks;

    public DefaultMetaManager() {
        cache = DefaultMetaCache.getInstance();
        storage = ZooKeeperMetaStorage.getInstance();
        storageEngineChangeHooks = Collections.synchronizedList(new ArrayList<>());

        try {
            initIginx();
            initStorageEngine();
            initStorageUnit();
            initFragment();
            initSchemaMapping();
        } catch (MetaStorageException e) {
            logger.error("init meta manager error: ", e);
            System.exit(-1);
        }
    }

    private void initIginx() throws MetaStorageException {
        storage.registerIginxChangeHook((id, iginx) -> {
            if (iginx == null) {
                cache.removeIginx(id);
            } else {
                cache.addIginx(iginx);
            }
        });
        for (IginxMeta iginx: storage.loadIginx().values()) {
            cache.addIginx(iginx);
        }
        IginxMeta iginx = new IginxMeta(0L, ConfigDescriptor.getInstance().getConfig().getIp(),
                ConfigDescriptor.getInstance().getConfig().getPort(), null);
        id = storage.registerIginx(iginx);
    }

    private void initStorageEngine() throws MetaStorageException {
        storage.registerStorageChangeHook((id, storageEngine) -> {
            if (storageEngine != null) {
                cache.addStorageEngine(storageEngine);
                for (StorageEngineChangeHook hook: storageEngineChangeHooks) {
                    hook.onChanged(null, storageEngine);
                }
            }
        });
        for (StorageEngineMeta storageEngine: storage.loadStorageEngine(resolveStorageEngineFromConf()).values()) {
            cache.addStorageEngine(storageEngine);
        }

    }

    private void initStorageUnit() throws MetaStorageException {
        storage.registerStorageUnitChangeHook((id, storageUnit) -> {
            if (storageUnit == null) {
                return;
            }
            if (storageUnit.getCreatedBy() == DefaultMetaManager.this.id) { // 不存在
                return;
            }
            StorageUnitMeta originStorageUnitMeta = cache.getStorageUnit(id);
            if (originStorageUnitMeta == null) {
                if (!storageUnit.isMaster()) { // 需要加入到主节点的子节点列表中
                    StorageUnitMeta masterStorageUnitMeta = cache.getStorageUnit(storageUnit.getMasterId());
                    if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
                        logger.error("unexpected storage unit " + storageUnit.toString() + ", because it does not has a master storage unit");
                    } else {
                        masterStorageUnitMeta.addReplica(storageUnit);
                    }
                }
            } else {
                if (storageUnit.isMaster()) {
                    storageUnit.setReplicas(originStorageUnitMeta.getReplicas());
                } else {
                    StorageUnitMeta masterStorageUnitMeta = cache.getStorageUnit(storageUnit.getMasterId());
                    if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
                        logger.error("unexpected storage unit " + storageUnit.toString() + ", because it does not has a master storage unit");
                    } else {
                        masterStorageUnitMeta.removeReplica(originStorageUnitMeta);
                        masterStorageUnitMeta.addReplica(storageUnit);
                    }
                }
            }
            if (originStorageUnitMeta != null) {
                cache.updateStorageUnit(storageUnit);
                cache.getStorageEngine(storageUnit.getStorageEngineId()).removeStorageUnit(originStorageUnitMeta.getId());
            } else {
                cache.addStorageUnit(storageUnit);
            }
            cache.getStorageEngine(storageUnit.getStorageEngineId()).addStorageUnit(storageUnit);
        });
        for (StorageUnitMeta storageUnit: storage.loadStorageUnit().values()) {
            cache.addStorageUnit(storageUnit);
            cache.getStorageEngine(storageUnit.getStorageEngineId()).addStorageUnit(storageUnit);
        }
    }

    private void initFragment() throws MetaStorageException {
        storage.registerFragmentChangeHook((create, fragment) -> {
            if (fragment == null)
                return;
            if (create && fragment.getCreatedBy() == DefaultMetaManager.this.id) {
                return;
            }
            if (!create && fragment.getUpdatedBy() == DefaultMetaManager.this.id) {
                return;
            }
            if (create) {
                cache.addFragment(fragment);
            } else {
                cache.updateFragment(fragment);
            }
            cache.updateFragment(fragment);
        });
        cache.initFragment(storage.loadFragment());
    }

    private void initSchemaMapping() throws MetaStorageException {
        storage.registerSchemaMappingChangeHook((schema, schemaMapping) -> {
            if (schemaMapping == null || schemaMapping.size() == 0) {
                cache.removeSchemaMapping(schema);
            } else {
                cache.addOrUpdateSchemaMapping(schema, schemaMapping);
            }
        });
        for (Map.Entry<String, Map<String, Integer>> schemaEntry: storage.loadSchemaMapping().entrySet()) {
            cache.addOrUpdateSchemaMapping(schemaEntry.getKey(), schemaEntry.getValue());
        }
    }

    @Override
    public boolean addStorageEngine(StorageEngineMeta storageEngineMeta) {
        try {
            storageEngineMeta.setId(storage.addStorageEngine(storageEngineMeta));
            cache.addStorageEngine(storageEngineMeta);
            return true;
        } catch (MetaStorageException e) {
            logger.error("add storage engine error: ", e);
        }
        return false;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return new ArrayList<>(cache.getStorageEngineList());
    }

    @Override
    public StorageEngineMeta getStorageEngine(long id) {
        return cache.getStorageEngine(id);
    }

    @Override
    public StorageUnitMeta getStorageUnit(String id) {
        return cache.getStorageUnit(id);
    }

    @Override
    public Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids) {
        return cache.getStorageUnits(ids);
    }

    @Override
    public List<IginxMeta> getIginxList() {
        return new ArrayList<>(cache.getIginxList());
    }

    @Override
    public long getIginxId() {
        return id;
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        return cache.getFragmentMapByTimeSeriesInterval(tsInterval);
    }

    @Override
    public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        return cache.getLatestFragmentMapByTimeSeriesInterval(tsInterval);
    }

    @Override
    public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMap() {
        return cache.getLatestFragmentMap();
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval, TimeInterval timeInterval) {
        return cache.getFragmentMapByTimeSeriesIntervalAndTimeInterval(tsInterval, timeInterval);
    }

    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesName(String tsName) {
        return cache.getFragmentListByTimeSeriesName(tsName);
    }

    @Override
    public FragmentMeta getLatestFragmentByTimeSeriesName(String tsName) {
        return cache.getLatestFragmentByTimeSeriesName(tsName);
    }

    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval) {
        return cache.getFragmentListByTimeSeriesNameAndTimeInterval(tsName, timeInterval);
    }

    @Override
    public boolean createFragments(List<FragmentMeta> fragments) {
        try {
            storage.lockFragment();
            Map<TimeSeriesInterval, FragmentMeta> latestFragments = getLatestFragmentMap();
            for (FragmentMeta originalFragmentMeta : latestFragments.values()) {
                FragmentMeta fragmentMeta = originalFragmentMeta.endFragmentMeta(fragments.get(0).getTimeInterval().getStartTime());
                // 在更新分片时，先更新本地
                fragmentMeta.setUpdatedBy(id);
                cache.updateFragment(fragmentMeta);
                storage.updateFragment(fragmentMeta);
            }
            for (FragmentMeta fragmentMeta : fragments) {
                fragmentMeta.setCreatedBy(id);
                cache.addFragment(fragmentMeta);
                storage.addFragment(fragmentMeta);
            }
            return true;
        } catch (MetaStorageException e) {
            logger.error("create fragment error: ", e);
        } finally {
            try {
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("release fragment lock error: ", e);
            }
        }
        return false;
    }

    @Override
    public boolean hasFragment() {
        return cache.hasFragment();
    }

    protected Map<String, StorageUnitMeta> tryInitStorageUnits(List<StorageUnitMeta> storageUnits) {
        try {
            if (cache.hasStorageUnit()) {
                return null;
            }
            storage.lockStorageUnit();
            Map<String, StorageUnitMeta> fakeIdToStorageUnit = new HashMap<>(); // 假名翻译工具
            for (StorageUnitMeta masterStorageUnit: storageUnits) {
                masterStorageUnit.setCreatedBy(id);
                String fakeName = masterStorageUnit.getId();
                String actualName = storage.addStorageUnit();
                StorageUnitMeta actualMasterStorageUnit = masterStorageUnit.renameStorageUnitMeta(actualName, actualName);
                storage.updateStorageUnit(actualMasterStorageUnit);
                fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
                for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
                    slaveStorageUnit.setCreatedBy(id);
                    String slaveFakeName = slaveStorageUnit.getId();
                    String slaveActualName = storage.addStorageUnit();
                    StorageUnitMeta actualSlaveStorageUnit = slaveStorageUnit.renameStorageUnitMeta(slaveActualName, actualName);
                    actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
                    storage.updateStorageUnit(actualSlaveStorageUnit);
                    cache.addStorageUnit(actualSlaveStorageUnit);
                    fakeIdToStorageUnit.put(slaveFakeName, actualSlaveStorageUnit);
                }
                cache.addStorageUnit(actualMasterStorageUnit);
            }
            return fakeIdToStorageUnit;
        } catch (MetaStorageException e) {
            logger.error("encounter error when init storage units: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("encounter error when release storage unit lock: ", e);
            }
        }
        return null;
    }

    @Override
    public boolean createInitialFragmentsAndStorageUnits(List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments) {
        Map<String, StorageUnitMeta> fakeIdToStorageUnit = tryInitStorageUnits(storageUnits);
        if (fakeIdToStorageUnit == null) {
            return false;
        }
        try {
            storage.lockFragment();
            if (cache.hasFragment()) {
                return false;
            }
            initialFragments.sort(Comparator.comparingLong(o -> o.getTimeInterval().getStartTime()));
            for (FragmentMeta fragmentMeta : initialFragments) {
                String tsIntervalName = fragmentMeta.getTsInterval().toString();
                String timeIntervalName = fragmentMeta.getTimeInterval().toString();
                // 针对本机创建的分片，直接将其加入到本地
                fragmentMeta.setCreatedBy(id);
                StorageUnitMeta storageUnit = fakeIdToStorageUnit.get(fragmentMeta.getFakeStorageUnitId());
                if (storageUnit.isMaster()) {
                    fragmentMeta.setMasterStorageUnit(storageUnit);
                } else {
                    fragmentMeta.setMasterStorageUnit(getStorageUnit(storageUnit.getMasterId()));
                }
                storage.addFragment(fragmentMeta);
                cache.addFragment(fragmentMeta);
            }
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when init fragment: ", e);
        } finally {
            try {
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("encounter error when release fragment lock: ", e);
            }
        }
        return false;
    }

    @Override
    public List<Long> selectStorageEngineIdList() {
        List<Long> storageEngineIdList = getStorageEngineList().stream().map(StorageEngineMeta::getId).collect(Collectors.toList());
        if (storageEngineIdList.size() <= 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum()) {
            return storageEngineIdList;
        }
        Random random = new Random();
        for (int i = 0; i < storageEngineIdList.size(); i++) {
            int next = random.nextInt(storageEngineIdList.size());
            Long value = storageEngineIdList.get(next);
            storageEngineIdList.set(next, storageEngineIdList.get(i));
            storageEngineIdList.set(i, value);
        }
        return storageEngineIdList.subList(0, 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum());
    }

    @Override
    public Map<String, Long> selectStorageUnitsToMigrate(List<Long> addedStorageEngineIdList) {
        Map<String, Long> migrationMap = new HashMap<>();
        Map<Long, List<String>> storageEngineIdToCoveredStorageUnitIdList = new HashMap<>();
        List<StorageEngineMeta> storageEngineList = cache.getStorageEngineList();
        int avg = (int) Math.ceil((double) (int) storageEngineList.stream().map(StorageEngineMeta::getStorageUnitList).count()
                / (storageEngineList.size() + addedStorageEngineIdList.size()));
        for (StorageEngineMeta storageEngineMeta : storageEngineList) {
            int cnt = storageEngineMeta.getStorageUnitList().size() - avg;
            for (int i = 0; i < cnt; i++) {
                for (Long addedStorageEngineId : addedStorageEngineIdList) {
                    if (storageEngineIdToCoveredStorageUnitIdList.containsKey(addedStorageEngineId) &&
                            storageEngineIdToCoveredStorageUnitIdList.get(addedStorageEngineId).contains(storageEngineMeta.getStorageUnitList().get(i).getMasterId())) {
                        continue;
                    }
                    List<String> coveredStorageUnitIdList = storageEngineIdToCoveredStorageUnitIdList.computeIfAbsent(addedStorageEngineId, v -> new ArrayList<>());
                    if (coveredStorageUnitIdList.size() >= avg) {
                        continue;
                    }
                    storageEngineIdToCoveredStorageUnitIdList.get(addedStorageEngineId).add(storageEngineMeta.getStorageUnitList().get(i).getMasterId());
                    migrationMap.put(storageEngineMeta.getStorageUnitList().get(i).getId(), addedStorageEngineId);
                }
            }
        }
        return migrationMap;
    }

    @Override
    public boolean migrateStorageUnit(String storageUnitId, long targetStorageEngineId) {
        try {
            storage.lockStorageUnit();
            StorageUnitMeta storageUnit = cache.getStorageUnit(storageUnitId);
            if (storageUnit == null) {
                return false;
            }
            String sourceDir = cache.getStorageEngine(storageUnit.getStorageEngineId()).getExtraParams().getOrDefault("dataDir", "/");
            String targetDir = cache.getStorageEngine(targetStorageEngineId).getExtraParams().getOrDefault("dataDir", "/");
            String cmd = String.format("migrate.sh %s %s %s", sourceDir, storageUnitId, targetDir);
            Process process = Runtime.getRuntime().exec(cmd);
            if (process.waitFor() != 0) {
                logger.error("migrate error: {} from {} to {}", storageUnitId, sourceDir, targetDir);
                return false;
            }
            storageUnit = storageUnit.migrateStorageUnitMeta(targetStorageEngineId);
            storage.updateStorageUnit(storageUnit);
            cache.updateStorageUnit(storageUnit);
            return true;
        } catch (Exception e) {
            logger.error("encounter error when migrate storage unit: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("encounter error when release storage unit lock: ", e);
            }
        }
        return false;
    }

    @Override
    public Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(List<String> paths, TimeInterval timeInterval) {
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();

        if (paths.size() + 1 < getStorageEngineList().size()) {
            // TODO 请求中 paths 数量很少，例如：只有 1 条
            List<FragmentMeta> leftFragmentList = new ArrayList<>();
            StorageUnitMeta topStorageUnit;
            List<FragmentMeta> rightFragmentList = new ArrayList<>();
            StorageUnitMeta bottomStorageUnit;

            List<Long> storageEngineIdList = selectStorageEngineIdList();
            String topId = RandomStringUtils.randomAlphanumeric(16);
            topStorageUnit = new StorageUnitMeta(topId, storageEngineIdList.get(0), topId, true);
            for (int i = 1; i < storageEngineIdList.size(); i++) {
                topStorageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineIdList.get(i), topId, false));
            }
            storageUnitList.add(topStorageUnit);
            leftFragmentList.add(new FragmentMeta(paths.get(paths.size() / 2), null, timeInterval.getStartTime(), Long.MAX_VALUE, topId));

            storageEngineIdList = selectStorageEngineIdList();
            String bottomId = RandomStringUtils.randomAlphanumeric(16);
            bottomStorageUnit = new StorageUnitMeta(bottomId, storageEngineIdList.get(0), bottomId, true);
            for (int i = 1; i < storageEngineIdList.size(); i++) {
                bottomStorageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineIdList.get(i), bottomId, false));
            }
            storageUnitList.add(bottomStorageUnit);
            rightFragmentList.add(new FragmentMeta(null, paths.get(paths.size() / 2), timeInterval.getStartTime(), Long.MAX_VALUE, bottomId));

            if (timeInterval.getStartTime() != 0) {
                leftFragmentList.add(new FragmentMeta(paths.get(paths.size() / 2), null, 0, timeInterval.getStartTime(), topId));
                rightFragmentList.add(new FragmentMeta(null, paths.get(paths.size() / 2), 0, timeInterval.getStartTime(), bottomId));
            }
            fragmentMap.put(new TimeSeriesInterval(paths.get(paths.size() / 2), null), leftFragmentList);
            fragmentMap.put(new TimeSeriesInterval(null, paths.get(paths.size() / 2)), rightFragmentList);
        } else {
            // 处理[startTime, +∞) & (-∞, +∞)
            List<TimeSeriesInterval> tsIntervalList = splitTimeSeriesSpace(paths, 1);
            int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), getStorageEngineList().size());
            List<FragmentMeta> fragmentMetaList;
            String masterId;
            StorageUnitMeta storageUnit;
            for (int i = 0; i < tsIntervalList.size(); i++) {
                fragmentMetaList = new ArrayList<>();
                masterId = RandomStringUtils.randomAlphanumeric(16);
                storageUnit = new StorageUnitMeta(masterId, getStorageEngineList().get((i * replicaNum) % getStorageEngineList().size()).getId(), masterId, true);
                for (int j = i * replicaNum + 1; j < (i + 1) * replicaNum; j++) {
                    storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), getStorageEngineList().get(j % getStorageEngineList().size()).getId(), masterId, false));
                }
                storageUnitList.add(storageUnit);
                fragmentMetaList.add(new FragmentMeta(tsIntervalList.get(i).getStartTimeSeries(), tsIntervalList.get(i).getEndTimeSeries(), timeInterval.getStartTime(), Long.MAX_VALUE, masterId));
                fragmentMap.put(tsIntervalList.get(i), fragmentMetaList);
            }

            // [0, startTime) & (-∞, +∞) 几乎无数据，作为一个分片处理即可
            fragmentMetaList = new ArrayList<>();
            masterId = RandomStringUtils.randomAlphanumeric(16);
            storageUnit = new StorageUnitMeta(masterId, getStorageEngineList().get(0).getId(), masterId, true);
            for (int i = 1; i < replicaNum; i++) {
                storageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), getStorageEngineList().get(i).getId(), masterId, false));
            }
            storageUnitList.add(storageUnit);
            fragmentMetaList.add(new FragmentMeta(null, null, 0, timeInterval.getStartTime(), masterId));
            fragmentMap.put(new TimeSeriesInterval(null, null), fragmentMetaList);
        }

        return new Pair<>(fragmentMap, storageUnitList);
    }

    public Pair<Map<TimeSeriesInterval, List<FragmentMeta>>, List<StorageUnitMeta>> generateFragmentsAndStorageUnits(String startPath, long startTime) {
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();
        List<FragmentMeta> leftFragmentList = new ArrayList<>();
        StorageUnitMeta topStorageUnit;
        List<FragmentMeta> rightFragmentList = new ArrayList<>();
        StorageUnitMeta bottomStorageUnit;

        List<Long> storageEngineIdList = selectStorageEngineIdList();
        String topId = RandomStringUtils.randomAlphanumeric(16);
        topStorageUnit = new StorageUnitMeta(topId, storageEngineIdList.get(0), topId, true);
        for (int i = 1; i < storageEngineIdList.size(); i++) {
            topStorageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineIdList.get(i), topId, false));
        }
        storageUnitList.add(topStorageUnit);
        leftFragmentList.add(new FragmentMeta(startPath, null, startTime, Long.MAX_VALUE, topId));

        storageEngineIdList = selectStorageEngineIdList();
        String bottomId = RandomStringUtils.randomAlphanumeric(16);
        bottomStorageUnit = new StorageUnitMeta(bottomId, storageEngineIdList.get(0), bottomId, true);
        for (int i = 1; i < storageEngineIdList.size(); i++) {
            bottomStorageUnit.addReplica(new StorageUnitMeta(RandomStringUtils.randomAlphanumeric(16), storageEngineIdList.get(i), bottomId, false));
        }
        storageUnitList.add(bottomStorageUnit);
        rightFragmentList.add(new FragmentMeta(null, startPath, startTime, Long.MAX_VALUE, bottomId));

        if (startTime != 0) {
            leftFragmentList.add(new FragmentMeta(startPath, null, 0, startTime, topId));
            rightFragmentList.add(new FragmentMeta(null, startPath, 0, startTime, bottomId));
        }
        fragmentMap.put(new TimeSeriesInterval(startPath, null), leftFragmentList);
        fragmentMap.put(new TimeSeriesInterval(null, startPath), rightFragmentList);

        return new Pair<>(fragmentMap, storageUnitList);
    }

    @Override
    public Pair<List<FragmentMeta>, List<StorageUnitMeta>> generateInitialFragmentsAndStorageUnits(List<String> prefixList, long startTime) {
        List<FragmentMeta> fragmentMetaList = new ArrayList<>();
        // TODO 新建 StorageUnit
        List<StorageUnitMeta> storageUnitMetaList = new ArrayList<>();
        prefixList = prefixList.stream().filter(Objects::nonNull).sorted(String::compareTo).collect(Collectors.toList());
        String previousPrefix;
        String prefix = null;
        for (String s : prefixList) {
            previousPrefix = prefix;
            prefix = s;
            fragmentMetaList.add(new FragmentMeta(previousPrefix, prefix, startTime, Long.MAX_VALUE));
        }
        fragmentMetaList.add(new FragmentMeta(prefix, null, startTime, Long.MAX_VALUE));
        return new Pair<>(fragmentMetaList, storageUnitMetaList);
    }

    @Override
    public void registerStorageEngineChangeHook(StorageEngineChangeHook hook) {
        if (hook != null) {
            this.storageEngineChangeHooks.add(hook);
        }
    }

    @Override
    public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {
        try {
            storage.updateSchemaMapping(schema, schemaMapping);
            if (schemaMapping == null) {
                cache.removeSchemaMapping(schema);
            } else {
                cache.addOrUpdateSchemaMapping(schema, schemaMapping);
            }
        } catch (MetaStorageException e) {
            logger.error("update schema mapping error: ", e);
        }
    }

    @Override
    public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {
        Map<String, Integer> schemaMapping = cache.getSchemaMapping(schema);
        if (value == -1) {
            schemaMapping.remove(key);
        } else {
            schemaMapping.put(key, value);
        }
        try {
            storage.updateSchemaMapping(schema, schemaMapping);
            if (value == -1) {
                cache.removeSchemaMappingItem(schema, key);
            } else {
                cache.addOrUpdateSchemaMappingItem(schema, key, value);
            }
        } catch (MetaStorageException e) {
            logger.error("update schema mapping error: ", e);
        }
    }

    @Override
    public Map<String, Integer> getSchemaMapping(String schema) {
        return cache.getSchemaMapping(schema);
    }

    @Override
    public int getSchemaMappingItem(String schema, String key) {
        return cache.getSchemaMappingItem(schema, key);
    }

    private List<StorageEngineMeta> resolveStorageEngineFromConf() {
        List<StorageEngineMeta> storageEngineMetaList = new ArrayList<>();
        String[] storageEngineStrings = ConfigDescriptor.getInstance().getConfig().getStorageEngineList().split(",");
        for (int i = 0; i < storageEngineStrings.length; i++) {
            String[] storageEngineParts = storageEngineStrings[i].split("#");
            String ip = storageEngineParts[0];
            int port = Integer.parseInt(storageEngineParts[1]);
            StorageEngine storageEngine = StorageEngine.fromString(storageEngineParts[2]);
            Map<String, String> extraParams = new HashMap<>();
            for (int j = 3; j < storageEngineParts.length; j++) {
                String[] KAndV = storageEngineParts[j].split("=");
                if (KAndV.length != 2) {
                    logger.error("unexpected storage engine meta info: " + storageEngineStrings[i]);
                    continue;
                }
                extraParams.put(KAndV[0], KAndV[1]);
            }
            storageEngineMetaList.add(new StorageEngineMeta(i, ip, port, extraParams, storageEngine));
        }
        return storageEngineMetaList;
    }

    /**
     * TODO 目前假设 paths 数量足够多
     * 根据给定的 paths 将时间序列空间分割为 multiple * storageEngineNum 个子空间
     */
    private List<TimeSeriesInterval> splitTimeSeriesSpace(List<String> paths, int multiple) {
        List<TimeSeriesInterval> tsIntervalList = new ArrayList<>();
        int num = Math.min(Math.max(1, multiple * getStorageEngineList().size() - 2), paths.size() - 1);
        for (int i = 0; i < num; i++) {
            tsIntervalList.add(new TimeSeriesInterval(paths.get(i * (paths.size() - 1) / num), paths.get((i + 1) * (paths.size() - 1) / num)));
        }
        tsIntervalList.add(new TimeSeriesInterval(null, paths.get(0)));
        tsIntervalList.add(new TimeSeriesInterval(paths.get(paths.size() - 1), null));
        return tsIntervalList;
    }
}
