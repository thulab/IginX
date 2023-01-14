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

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.write.*;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.policy.simple.TimeSeriesCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DefaultMetaCache implements IMetaCache {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMetaCache.class.getName());

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static DefaultMetaCache INSTANCE = null;

    // 分片列表的缓存
    private final List<Pair<TimeSeriesRange, List<FragmentMeta>>> sortedFragmentMetaLists;

    private final Map<TimeSeriesRange, List<FragmentMeta>> fragmentMetaListMap;

    private final List<FragmentMeta> dummyFragments;

    private int fragmentCacheSize;

    private final int fragmentCacheMaxSize;

    private final boolean enableFragmentCacheControl = config.isEnableMetaCacheControl();

    private long minTimestamp = 0L;

    private final ReadWriteLock fragmentLock;

    // 数据单元的缓存
    private final Map<String, StorageUnitMeta> storageUnitMetaMap;

    // 已有数据对应的数据单元
    private final Map<String, StorageUnitMeta> dummyStorageUnitMetaMap;

    private final ReadWriteLock storageUnitLock;

    // iginx 的缓存
    private final Map<Long, IginxMeta> iginxMetaMap;

    // 数据后端的缓存
    private final Map<Long, StorageEngineMeta> storageEngineMetaMap;

    // schemaMapping 的缓存
    private final Map<String, Map<String, Integer>> schemaMappings;

    // user 的缓存
    private final Map<String, UserMeta> userMetaMap;

    // 时序列信息版本号的缓存
    private final Map<Integer, Integer> timeSeriesVersionMap;

    private final ReadWriteLock insertRecordLock = new ReentrantReadWriteLock();

    private final Map<String, TimeSeriesCalDO> timeSeriesCalDOConcurrentHashMap = new ConcurrentHashMap<>();

    private final Random random = new Random();

    // transform task 的缓存
    private final Map<String, TransformTaskMeta> transformTaskMetaMap;

    private DefaultMetaCache() {
        if (enableFragmentCacheControl) {
            long sizeOfFragment = FragmentMeta.sizeOf();
            fragmentCacheMaxSize = (int) ((long)(config.getFragmentCacheThreshold() * 1024) / sizeOfFragment);
            fragmentCacheSize = 0;
        } else {
            fragmentCacheMaxSize = -1;
        }

        // 分片相关
        sortedFragmentMetaLists = new ArrayList<>();
        fragmentMetaListMap = new HashMap<>();
        dummyFragments = new ArrayList<>();
        fragmentLock = new ReentrantReadWriteLock();
        // 数据单元相关
        storageUnitMetaMap = new HashMap<>();
        dummyStorageUnitMetaMap = new HashMap<>();
        storageUnitLock = new ReentrantReadWriteLock();
        // iginx 相关
        iginxMetaMap = new ConcurrentHashMap<>();
        // 数据后端相关
        storageEngineMetaMap = new ConcurrentHashMap<>();
        // schemaMapping 相关
        schemaMappings = new ConcurrentHashMap<>();
        // user 相关
        userMetaMap = new ConcurrentHashMap<>();
        // 时序列信息版本号相关
        timeSeriesVersionMap = new ConcurrentHashMap<>();
        // transform task 相关
        transformTaskMetaMap = new ConcurrentHashMap<>();
    }

    public static DefaultMetaCache getInstance() {
        if (INSTANCE == null) {
            synchronized (DefaultMetaCache.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultMetaCache();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public boolean enableFragmentCacheControl() {
        return enableFragmentCacheControl;
    }

    @Override
    public long getFragmentMinTimestamp() {
        return minTimestamp;
    }

    private static List<Pair<TimeSeriesRange, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesRange, List<FragmentMeta>>> fragmentSeriesList, TimeSeriesRange tsInterval) {
        List<Pair<TimeSeriesRange, List<FragmentMeta>>> resultList = new ArrayList<>();
        if (fragmentSeriesList.isEmpty()) {
            return resultList;
        }
        int index = 0;
        while (index < fragmentSeriesList.size() && !fragmentSeriesList.get(index).k.isCompletelyAfter(tsInterval)) {
            if (fragmentSeriesList.get(index).k.isIntersect(tsInterval)) {
                resultList.add(fragmentSeriesList.get(index));
            }
            index++;
        }
        return resultList;
    }

    private static List<Pair<TimeSeriesRange, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesRange, List<FragmentMeta>>> fragmentSeriesList, String tsName) {
        List<Pair<TimeSeriesRange, List<FragmentMeta>>> resultList = new ArrayList<>();
        if (fragmentSeriesList.isEmpty()) {
            return resultList;
        }
        int index = 0;
        while (index < fragmentSeriesList.size() && !fragmentSeriesList.get(index).k.isAfter(tsName)) {
            if (fragmentSeriesList.get(index).k.isContain(tsName)) {
                resultList.add(fragmentSeriesList.get(index));
            }
            index++;
        }
        return resultList;
    }

    private static List<FragmentMeta> searchFragmentList(List<FragmentMeta> fragmentMetaList, TimeInterval timeInterval) {
        List<FragmentMeta> resultList = new ArrayList<>();
        if (fragmentMetaList.isEmpty()) {
            return resultList;
        }
        int index = 0;
        while (index < fragmentMetaList.size() && !fragmentMetaList.get(index).getTimeInterval().isAfter(timeInterval)) {
            if (fragmentMetaList.get(index).getTimeInterval().isIntersect(timeInterval)) {
                resultList.add(fragmentMetaList.get(index));
            }
            index++;
        }
        return resultList;
    }

    private static List<FragmentMeta> searchFragmentList(List<FragmentMeta> fragmentMetaList, String storageUnitId) {
        List<FragmentMeta> resultList = new ArrayList<>();
        if (fragmentMetaList.isEmpty()) {
            return resultList;
        }
        for (FragmentMeta meta : fragmentMetaList) {
            if (meta.getMasterStorageUnitId().equals(storageUnitId)) {
                resultList.add(meta);
            }
        }
        return resultList;
    }

    @Override
    public void initFragment(Map<TimeSeriesRange, List<FragmentMeta>> fragmentListMap) {
        storageUnitLock.readLock().lock();
        fragmentListMap.values().forEach(e -> e.forEach(f -> f.setMasterStorageUnit(storageUnitMetaMap.get(f.getMasterStorageUnitId()))));
        storageUnitLock.readLock().unlock();
        fragmentLock.writeLock().lock();
        sortedFragmentMetaLists.addAll(fragmentListMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toList()));
        fragmentListMap.forEach(fragmentMetaListMap::put);
        if (enableFragmentCacheControl) {
            // 统计分片总数
            fragmentCacheSize = sortedFragmentMetaLists.stream().mapToInt(e -> e.v.size()).sum();
            while (fragmentCacheSize > fragmentCacheMaxSize) {
                kickOffHistoryFragment();
            }
        }
        fragmentLock.writeLock().unlock();
    }

    private void kickOffHistoryFragment() {
        long nextMinTimestamp = 0L;
        for (List<FragmentMeta> fragmentList: fragmentMetaListMap.values()) {
            FragmentMeta fragment = fragmentList.get(0);
            if (fragment.getTimeInterval().getStartTime() == minTimestamp) {
                fragmentList.remove(0);
                nextMinTimestamp = fragment.getTimeInterval().getEndTime();
                fragmentCacheSize--;
            }
        }
        if (nextMinTimestamp == 0L || nextMinTimestamp == Long.MAX_VALUE) {
            logger.error("unexpected next min timestamp " + nextMinTimestamp + "!");
            System.exit(-1);
        }
        minTimestamp = nextMinTimestamp;
    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        // 更新 fragmentMetaListMap
        List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.computeIfAbsent(fragmentMeta.getTsInterval(), v -> new ArrayList<>());
        if (fragmentMetaList.size() == 0) {
            // 更新 sortedFragmentMetaLists
            updateSortedFragmentsList(fragmentMeta.getTsInterval(), fragmentMetaList);
        }
        fragmentMetaList.add(fragmentMeta);
        if (enableFragmentCacheControl) {
            if (fragmentMeta.getTimeInterval().getStartTime() < minTimestamp) {
                minTimestamp = fragmentMeta.getTimeInterval().getStartTime();
            }
            fragmentCacheSize++;
            while (fragmentCacheSize > fragmentCacheMaxSize) {
                kickOffHistoryFragment();
            }
        }

        fragmentLock.writeLock().unlock();
    }

    private void updateSortedFragmentsList(TimeSeriesRange tsInterval, List<FragmentMeta> fragmentMetas) {
        Pair<TimeSeriesRange, List<FragmentMeta>> pair = new Pair<>(tsInterval, fragmentMetas);
        if (sortedFragmentMetaLists.size() == 0) {
            sortedFragmentMetaLists.add(pair);
            return;
        }
        int left = 0, right = sortedFragmentMetaLists.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            TimeSeriesRange midTsInterval = sortedFragmentMetaLists.get(mid).k;
            if (tsInterval.compareTo(midTsInterval) < 0) {
                right = mid - 1;
            } else if (tsInterval.compareTo(midTsInterval) > 0) {
                left = mid + 1;
            } else {
                throw new RuntimeException("unexpected fragment");
            }
        }
        if (left == sortedFragmentMetaLists.size()) {
            sortedFragmentMetaLists.add(pair);
        } else {
            sortedFragmentMetaLists.add(left, pair);
        }

    }

    @Override
    public void updateFragment(FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        // 更新 fragmentMetaListMap
        List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.get(fragmentMeta.getTsInterval());
        fragmentMetaList.set(fragmentMetaList.size() - 1, fragmentMeta);
        fragmentLock.writeLock().unlock();
    }

    @Override
    public void updateFragmentByTsInterval(TimeSeriesRange tsInterval,
        FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        try {
            // 更新 fragmentMetaListMap
            List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.get(tsInterval);
            fragmentMetaList.set(fragmentMetaList.size() - 1, fragmentMeta);
            fragmentMetaListMap.put(fragmentMeta.getTsInterval(), fragmentMetaList);
            fragmentMetaListMap.remove(tsInterval);

            for (Pair<TimeSeriesRange, List<FragmentMeta>> timeSeriesIntervalListPair : sortedFragmentMetaLists) {
                if (timeSeriesIntervalListPair.getK().equals(tsInterval)) {
                    timeSeriesIntervalListPair.k = fragmentMeta.getTsInterval();
                }
            }
        } finally {
            fragmentLock.writeLock().unlock();
        }
    }

    @Override
    public void deleteFragmentByTsInterval(TimeSeriesRange tsInterval, FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        try {
            // 更新 fragmentMetaListMap
            List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.get(tsInterval);
            fragmentMetaList.remove(fragmentMeta);
            if (fragmentMetaList.size() == 0) {
                fragmentMetaListMap.remove(tsInterval);
            }
            for (int index = 0; index < sortedFragmentMetaLists.size(); index++) {
                if (sortedFragmentMetaLists.get(index).getK().equals(tsInterval)) {
                    sortedFragmentMetaLists.get(index).getV().remove(fragmentMeta);
                    if (sortedFragmentMetaLists.get(index).getV().isEmpty()) {
                        sortedFragmentMetaLists.remove(index);
                    }
                    break;
                }
            }
        } finally {
            fragmentLock.writeLock().unlock();
        }
    }

    @Override
    public Map<TimeSeriesRange, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(TimeSeriesRange tsInterval) {
        Map<TimeSeriesRange, List<FragmentMeta>> resultMap = new HashMap<>();
        fragmentLock.readLock().lock();
        searchFragmentSeriesList(sortedFragmentMetaLists, tsInterval).forEach(e -> resultMap.put(e.k, e.v));
        fragmentLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public List<FragmentMeta> getDummyFragmentsByTimeSeriesInterval(TimeSeriesRange tsInterval) {
        fragmentLock.readLock().lock();
        List<FragmentMeta> results = new ArrayList<>();
        for (FragmentMeta fragmentMeta: dummyFragments) {
            if (fragmentMeta.isValid() && fragmentMeta.getTsInterval().isIntersect(tsInterval)) {
                results.add(fragmentMeta);
            }
        }
        fragmentLock.readLock().unlock();
        return results;
    }

    @Override
    public Map<TimeSeriesRange, FragmentMeta> getLatestFragmentMap() {
        Map<TimeSeriesRange, FragmentMeta> latestFragmentMap = new HashMap<>();
        fragmentLock.readLock().lock();
        sortedFragmentMetaLists.stream().map(e -> e.v.get(e.v.size() - 1)).filter(e -> e.getTimeInterval().getEndTime() == Long.MAX_VALUE)
            .forEach(e -> latestFragmentMap.put(e.getTsInterval(), e));
        fragmentLock.readLock().unlock();
        return latestFragmentMap;
    }

    @Override
    public Map<TimeSeriesRange, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(TimeSeriesRange tsInterval) {
        Map<TimeSeriesRange, FragmentMeta> latestFragmentMap = new HashMap<>();
        fragmentLock.readLock().lock();
        searchFragmentSeriesList(sortedFragmentMetaLists, tsInterval).stream().map(e -> e.v.get(e.v.size() - 1)).filter(e -> e.getTimeInterval().getEndTime() == Long.MAX_VALUE)
            .forEach(e -> latestFragmentMap.put(e.getTsInterval(), e));
        fragmentLock.readLock().unlock();
        return latestFragmentMap;
    }

    @Override
    public Map<TimeSeriesRange, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesRange tsInterval, TimeInterval timeInterval) {
        Map<TimeSeriesRange, List<FragmentMeta>> resultMap = new HashMap<>();
        fragmentLock.readLock().lock();
        searchFragmentSeriesList(sortedFragmentMetaLists, tsInterval).forEach(e -> {
            List<FragmentMeta> fragmentMetaList = searchFragmentList(e.v, timeInterval);
            if (!fragmentMetaList.isEmpty()) {
                resultMap.put(e.k, fragmentMetaList);
            }
        });
        fragmentLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public List<FragmentMeta> getDummyFragmentsByTimeSeriesIntervalAndTimeInterval(TimeSeriesRange tsInterval, TimeInterval timeInterval) {
        fragmentLock.readLock().lock();
        List<FragmentMeta> results = new ArrayList<>();
        for (FragmentMeta fragmentMeta: dummyFragments) {
            if (fragmentMeta.isValid() && fragmentMeta.getTsInterval().isIntersect(tsInterval) && fragmentMeta.getTimeInterval().isIntersect(timeInterval)) {
                results.add(fragmentMeta);
            }
        }
        fragmentLock.readLock().unlock();
        return results;
    }

    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesName(String tsName) {
        List<FragmentMeta> resultList;
        fragmentLock.readLock().lock();
        resultList = searchFragmentSeriesList(sortedFragmentMetaLists, tsName).stream().map(e -> e.v).flatMap(List::stream).sorted((o1, o2) -> {
            if (o1.getTsInterval().getStartTimeSeries() == null && o2.getTsInterval().getStartTimeSeries() == null)
                return 0;
            else if (o1.getTsInterval().getStartTimeSeries() == null)
                return -1;
            else if (o2.getTsInterval().getStartTimeSeries() == null)
                return 1;
            return o1.getTsInterval().getStartTimeSeries().compareTo(o2.getTsInterval().getStartTimeSeries());
        }).collect(Collectors.toList());
        fragmentLock.readLock().unlock();
        return resultList;
    }

    @Override
    public FragmentMeta getLatestFragmentByTimeSeriesName(String tsName) {
        FragmentMeta result;
        fragmentLock.readLock().lock();
        result = searchFragmentSeriesList(sortedFragmentMetaLists, tsName).stream().map(e -> e.v).flatMap(List::stream)
            .filter(e -> e.getTimeInterval().getEndTime() == Long.MAX_VALUE).findFirst().orElse(null);
        fragmentLock.readLock().unlock();
        return result;
    }

    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval) {
        List<FragmentMeta> resultList;
        fragmentLock.readLock().lock();
        List<FragmentMeta> fragmentMetas = searchFragmentSeriesList(sortedFragmentMetaLists, tsName).stream().map(e -> e.v).flatMap(List::stream)
            .sorted(Comparator.comparingLong(o -> o.getTimeInterval().getStartTime())).collect(Collectors.toList());
        resultList = searchFragmentList(fragmentMetas, timeInterval);
        fragmentLock.readLock().unlock();
        return resultList;
    }

    @Override
    public List<FragmentMeta> getFragmentListByStorageUnitId(String storageUnitId) {
        List<FragmentMeta> resultList;
        fragmentLock.readLock().lock();
        List<FragmentMeta> fragmentMetas = sortedFragmentMetaLists.stream().map(e -> e.v).flatMap(List::stream)
            .sorted(Comparator.comparingLong(o -> o.getTimeInterval().getStartTime())).collect(Collectors.toList());
        resultList = searchFragmentList(fragmentMetas, storageUnitId);
        fragmentLock.readLock().unlock();
        return resultList;
    }

    @Override
    public boolean hasFragment() {
        return !sortedFragmentMetaLists.isEmpty() || (enableFragmentCacheControl && minTimestamp != 0L);
    }

    @Override
    public boolean hasStorageUnit() {
        return !storageUnitMetaMap.isEmpty();
    }

    @Override
    public void initStorageUnit(Map<String, StorageUnitMeta> storageUnits) {
        storageUnitLock.writeLock().lock();
        for (StorageUnitMeta storageUnit : storageUnits.values()) {
            storageUnitMetaMap.put(storageUnit.getId(), storageUnit);
            getStorageEngine(storageUnit.getStorageEngineId()).addStorageUnit(storageUnit);
        }
        storageUnitLock.writeLock().unlock();
    }

    @Override
    public StorageUnitMeta getStorageUnit(String id) {
        StorageUnitMeta storageUnit;
        storageUnitLock.readLock().lock();
        storageUnit = storageUnitMetaMap.get(id);
        if (storageUnit == null) {
            storageUnit = dummyStorageUnitMetaMap.get(id);
        }
        storageUnitLock.readLock().unlock();
        return storageUnit;
    }

    @Override
    public Map<String, StorageUnitMeta> getStorageUnits(Set<String> ids) {
        Map<String, StorageUnitMeta> resultMap = new HashMap<>();
        storageUnitLock.readLock().lock();
        for (String id : ids) {
            StorageUnitMeta storageUnit = storageUnitMetaMap.get(id);
            if (storageUnit != null) {
                resultMap.put(id, storageUnit);
            } else {
                storageUnit = dummyStorageUnitMetaMap.get(id);
                if (storageUnit != null) {
                    resultMap.put(id, storageUnit);
                }
            }
        }
        storageUnitLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public List<StorageUnitMeta> getStorageUnits() {
        List<StorageUnitMeta> storageUnitMetaList;
        storageUnitLock.readLock().lock();
        storageUnitMetaList = new ArrayList<>(storageUnitMetaMap.values());
        storageUnitMetaList.addAll(dummyStorageUnitMetaMap.values());
        storageUnitLock.readLock().unlock();
        return storageUnitMetaList;
    }

    @Override
    public void addStorageUnit(StorageUnitMeta storageUnitMeta) {
        storageUnitLock.writeLock().lock();
        storageUnitMetaMap.put(storageUnitMeta.getId(), storageUnitMeta);
        storageUnitLock.writeLock().unlock();
    }

    @Override
    public void updateStorageUnit(StorageUnitMeta storageUnitMeta) {
        storageUnitLock.writeLock().lock();
        storageUnitMetaMap.put(storageUnitMeta.getId(), storageUnitMeta);
        storageUnitLock.writeLock().unlock();
    }

    @Override
    public List<IginxMeta> getIginxList() {
        return new ArrayList<>(iginxMetaMap.values());
    }

    @Override
    public void addIginx(IginxMeta iginxMeta) {
        iginxMetaMap.put(iginxMeta.getId(), iginxMeta);
    }

    @Override
    public void removeIginx(long id) {
        iginxMetaMap.remove(id);
    }

    @Override
    public void addStorageEngine(StorageEngineMeta storageEngineMeta) {
        storageUnitLock.writeLock().lock();
        fragmentLock.writeLock().lock();
        if (!storageEngineMetaMap.containsKey(storageEngineMeta.getId())) {
            storageEngineMetaMap.put(storageEngineMeta.getId(), storageEngineMeta);
            if (storageEngineMeta.isHasData()) {
                StorageUnitMeta dummyStorageUnit = storageEngineMeta.getDummyStorageUnit();
                FragmentMeta dummyFragment = storageEngineMeta.getDummyFragment();
                dummyFragment.setMasterStorageUnit(dummyStorageUnit);
                dummyStorageUnitMetaMap.put(dummyStorageUnit.getId(), dummyStorageUnit);
                dummyFragments.add(dummyFragment);
            }
        }
        fragmentLock.writeLock().unlock();
        storageUnitLock.writeLock().unlock();
    }

    @Override
    public boolean updateStorageEngine(long storageID, StorageEngineMeta storageEngineMeta) {
        storageUnitLock.writeLock().lock();
        fragmentLock.writeLock().lock();

        if (!storageEngineMetaMap.containsKey(storageID)) {
            logger.error("No corresponding storage engine needs to be updated");
            return false;
        }
        String dummyStorageUnitID = StorageUnitMeta.generateDummyStorageUnitID(storageID);
        boolean ifOriHasData = storageEngineMetaMap.get(storageID).isHasData();
        if (storageEngineMeta.isHasData()) { // 设置相关元数据信息
            StorageUnitMeta dummyStorageUnit = storageEngineMeta.getDummyStorageUnit();
            FragmentMeta dummyFragment = storageEngineMeta.getDummyFragment();
            dummyFragment.setMasterStorageUnit(dummyStorageUnit);
            dummyStorageUnitMetaMap.put(dummyStorageUnit.getId(), dummyStorageUnit);
            if (ifOriHasData) { // 更新 dummyFragments 数据
                dummyFragments.removeIf(e -> e.getMasterStorageUnitId().equals(dummyStorageUnitID));
            } else {
                dummyFragments.add(dummyFragment);
            }
        } else if (ifOriHasData) { // 原来没有，则移除
            dummyFragments.removeIf(e -> e.getMasterStorageUnitId().equals(dummyStorageUnitID));
            dummyStorageUnitMetaMap.remove(dummyStorageUnitID);
        }
        storageEngineMetaMap.put(storageEngineMeta.getId(), storageEngineMeta);

        fragmentLock.writeLock().unlock();
        storageUnitLock.writeLock().unlock();
        return true;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return new ArrayList<>(this.storageEngineMetaMap.values());
    }

    @Override
    public StorageEngineMeta getStorageEngine(long id) {
        return this.storageEngineMetaMap.get(id);
    }

    @Override
    public List<FragmentMeta> getFragments() {
        List<FragmentMeta> fragments = new ArrayList<>();
        this.fragmentLock.readLock().lock();
        for (Pair<TimeSeriesRange, List<FragmentMeta>> pair: sortedFragmentMetaLists) {
            fragments.addAll(pair.v);
        }
        this.fragmentLock.readLock().unlock();
        return fragments;
    }

    @Override
    public Map<String, Integer> getSchemaMapping(String schema) {
        if (this.schemaMappings.get(schema) == null)
            return null;
        return new HashMap<>(this.schemaMappings.get(schema));
    }

    @Override
    public int getSchemaMappingItem(String schema, String key) {
        Map<String, Integer> schemaMapping = schemaMappings.get(schema);
        if (schemaMapping == null) {
            return -1;
        }
        return schemaMapping.getOrDefault(key, -1);
    }

    @Override
    public void removeSchemaMapping(String schema) {
        schemaMappings.remove(schema);
    }

    @Override
    public void removeSchemaMappingItem(String schema, String key) {
        Map<String, Integer> schemaMapping = schemaMappings.get(schema);
        if (schemaMapping != null) {
            schemaMapping.remove(key);
        }
    }

    @Override
    public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {
        Map<String, Integer> mapping = schemaMappings.computeIfAbsent(schema, e -> new ConcurrentHashMap<>());
        mapping.putAll(schemaMapping);
    }

    @Override
    public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {
        Map<String, Integer> mapping = schemaMappings.computeIfAbsent(schema, e -> new ConcurrentHashMap<>());
        mapping.put(key, value);
    }

    @Override
    public void addOrUpdateUser(UserMeta userMeta) {
        userMetaMap.put(userMeta.getUsername(), userMeta);
    }

    @Override
    public void removeUser(String username) {
        userMetaMap.remove(username);
    }

    @Override
    public List<UserMeta> getUser() {
        return userMetaMap.values().stream().map(UserMeta::copy).collect(Collectors.toList());
    }

    @Override
    public List<UserMeta> getUser(List<String> usernames) {
        List<UserMeta> users = new ArrayList<>();
        for (String username : usernames) {
            UserMeta user = userMetaMap.get(username);
            if (user != null) {
                users.add(user.copy());
            }
        }
        return users;
    }

    @Override
    public void timeSeriesIsUpdated(int node, int version) {
        timeSeriesVersionMap.put(node, version);
    }

    @Override
    public void saveTimeSeriesData(InsertStatement statement) {
        insertRecordLock.writeLock().lock();
        long now = System.currentTimeMillis();

        RawData data = statement.getRawData();
        List<String> paths = data.getPaths();
        if (data.isColumnData()) {
            DataView view = new ColumnDataView(data, 0, data.getPaths().size(), 0, data.getKeys().size());
            for (int i = 0; i < view.getPathNum(); i++) {
                long minn = Long.MAX_VALUE;
                long maxx = Long.MIN_VALUE;
                long totalByte = 0L;
                int count = 0;
                BitmapView bitmapView = view.getBitmapView(i);
                for (int j = 0; j < view.getTimeSize(); j++) {
                    if (bitmapView.get(j)) {
                        minn = Math.min(minn, view.getKey(j));
                        maxx = Math.max(maxx, view.getKey(j));
                        if (view.getDataType(i) == DataType.BINARY) {
                            totalByte += ((byte[]) view.getValue(i, j)).length;
                        } else {
                            totalByte += transDatatypeToByte(view.getDataType(i));
                        }
                        count++;
                    }
                }
                if (count > 0) {
                    updateTimeSeriesCalDOConcurrentHashMap(paths.get(i), now, minn, maxx, totalByte, count);
                }
            }
        } else {
            DataView view = new RowDataView(data, 0, data.getPaths().size(), 0, data.getKeys().size());
            long[] totalByte = new long[view.getPathNum()];
            int[] count = new int[view.getPathNum()];
            long[] minn = new long[view.getPathNum()];
            long[] maxx = new long[view.getPathNum()];
            Arrays.fill(minn, Long.MAX_VALUE);
            Arrays.fill(maxx, Long.MIN_VALUE);

            for (int i = 0; i < view.getTimeSize(); i++) {
                BitmapView bitmapView = view.getBitmapView(i);
                int index = 0;
                for (int j = 0; j < view.getPathNum(); j++) {
                    if (bitmapView.get(j)) {
                        minn[j] = Math.min(minn[j], view.getKey(i));
                        maxx[j] = Math.max(maxx[j], view.getKey(i));
                        if (view.getDataType(j) == DataType.BINARY) {
                            totalByte[j] += ((byte[]) view.getValue(i, index)).length;
                        } else {
                            totalByte[j] += transDatatypeToByte(view.getDataType(j));
                        }
                        count[j]++;
                        index++;
                    }
                }
            }
            for (int i = 0; i < count.length; i++) {
                if (count[i] > 0) {
                    updateTimeSeriesCalDOConcurrentHashMap(paths.get(i), now, minn[i], maxx[i], totalByte[i], count[i]);
                }
            }
        }
        insertRecordLock.writeLock().unlock();
    }

    private void updateTimeSeriesCalDOConcurrentHashMap(String path, long now, long minn, long maxx, long totalByte, int count) {
        TimeSeriesCalDO timeSeriesCalDO = new TimeSeriesCalDO();
        timeSeriesCalDO.setTimeSeries(path);
        if (timeSeriesCalDOConcurrentHashMap.containsKey(path)) {
            timeSeriesCalDO = timeSeriesCalDOConcurrentHashMap.get(path);
        }
        timeSeriesCalDO.merge(now, minn, maxx, count, totalByte);
        timeSeriesCalDOConcurrentHashMap.put(path, timeSeriesCalDO);
    }

    private long transDatatypeToByte(DataType dataType) {
        switch (dataType) {
            case BOOLEAN:
                return 1;
            case INTEGER:
            case FLOAT:
                return 4;
            case LONG:
            case DOUBLE:
                return 8;
            default:
                return 0;
        }
    }

    @Override
    public List<TimeSeriesCalDO> getMaxValueFromTimeSeries() {
        insertRecordLock.readLock().lock();
        List<TimeSeriesCalDO> ret = timeSeriesCalDOConcurrentHashMap.values().stream()
            .filter(e -> random.nextDouble() < config.getCachedTimeseriesProb()).collect(Collectors.toList());
        insertRecordLock.readLock().unlock();
        return ret;
    }

    @Override
    public double getSumFromTimeSeries() {
        insertRecordLock.readLock().lock();
        double ret = timeSeriesCalDOConcurrentHashMap.values().stream().mapToDouble(TimeSeriesCalDO::getValue).sum();
        insertRecordLock.readLock().unlock();
        return ret;
    }

    @Override
    public Map<Integer, Integer> getTimeseriesVersionMap() {
        return timeSeriesVersionMap;
    }

    @Override
    public void addOrUpdateTransformTask(TransformTaskMeta transformTask) {
        transformTaskMetaMap.put(transformTask.getName(), transformTask);
    }

    @Override
    public void dropTransformTask(String name) {
        transformTaskMetaMap.remove(name);
    }

    @Override
    public TransformTaskMeta getTransformTask(String name) {
        return transformTaskMetaMap.getOrDefault(name, null);
    }

    @Override
    public List<TransformTaskMeta> getTransformTasks() {
        return transformTaskMetaMap.values().stream().map(TransformTaskMeta::copy).collect(Collectors.toList());
    }
}
