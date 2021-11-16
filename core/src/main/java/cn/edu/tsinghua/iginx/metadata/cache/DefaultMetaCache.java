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

import cn.edu.tsinghua.iginx.metadata.entity.FragmentStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesIntervalStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class DefaultMetaCache implements IMetaCache {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMetaCache.class);

    private static DefaultMetaCache INSTANCE = null;

    // 分片列表的缓存
    private final List<Pair<TimeSeriesInterval, List<FragmentMeta>>> sortedFragmentMetaLists;

    private final Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMetaListMap;

    private final ReadWriteLock fragmentLock;

    // 数据单元的缓存
    private final Map<String, StorageUnitMeta> storageUnitMetaMap;

    private final ReadWriteLock storageUnitLock;

    // iginx 的缓存
    private final Map<Long, IginxMeta> iginxMetaMap;

    // 数据后端的缓存
    private final Map<Long, StorageEngineMeta> storageEngineMetaMap;

    // schemaMapping 的缓存
    private final Map<String, Map<String, Integer>> schemaMappings;

    // 统计信息的缓存
    private final Map<FragmentMeta, FragmentStatistics> activeFragmentStatisticsMap;

    private final Map<FragmentMeta, FragmentStatistics> deltaActiveFragmentStatisticsMap;

    private final ReadWriteLock activeFragmentStatisticsLock;

    private final Map<Long, IginxStatistics> activeIginxStatisticsMap;

    private final Set<String> activeSeparatorStatistics;

    private final Map<Long, StorageEngineStatistics> activeStorageEngineStatisticsMap;

    private final Map<String, TimeSeriesStatistics> activeTimeSeriesStatisticsMap;

    // user 的缓存
    private final Map<String, UserMeta> userMetaMap;

    // 重分片过程中 fragment 的缓存
    // 为了解决重分片过程中 fragment 的创建先于 storage unit 的问题
    private final Map<String, List<FragmentMeta>> reshardFragmentListMap;

    private final ReadWriteLock reshardFragmentLock;

    private DefaultMetaCache() {
        // 分片相关
        sortedFragmentMetaLists = new ArrayList<>();
        fragmentMetaListMap = new HashMap<>();
        fragmentLock = new ReentrantReadWriteLock();
        // 数据单元相关
        storageUnitMetaMap = new HashMap<>();
        storageUnitLock = new ReentrantReadWriteLock();
        // iginx 相关
        iginxMetaMap = new ConcurrentHashMap<>();
        // 数据后端相关
        storageEngineMetaMap = new ConcurrentHashMap<>();
        // schemaMapping 相关
        schemaMappings = new ConcurrentHashMap<>();
        // 统计信息相关
        activeFragmentStatisticsMap = new ConcurrentHashMap<>();
        deltaActiveFragmentStatisticsMap = new ConcurrentHashMap<>();
        activeFragmentStatisticsLock = new ReentrantReadWriteLock();

        activeIginxStatisticsMap = new ConcurrentHashMap<>();

        activeSeparatorStatistics = new ConcurrentSkipListSet<>();

        activeStorageEngineStatisticsMap = new ConcurrentHashMap<>();

        activeTimeSeriesStatisticsMap = new ConcurrentHashMap<>();



        // user 相关
        userMetaMap = new ConcurrentHashMap<>();
        // 重分片中的 fragment 相关
        reshardFragmentListMap = new ConcurrentHashMap<>();
        reshardFragmentLock = new ReentrantReadWriteLock();
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

    private static List<Pair<TimeSeriesInterval, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList, TimeSeriesInterval tsInterval) {
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
        if (fragmentSeriesList.isEmpty()) {
            return resultList;
        }
        int index = 0;
        while(index < fragmentSeriesList.size() && !fragmentSeriesList.get(index).k.isCompletelyAfter(tsInterval)) {
            if (fragmentSeriesList.get(index).k.isIntersect(tsInterval)) {
                resultList.add(fragmentSeriesList.get(index));
            }
            index++;
        }
        return resultList;
    }

    private static List<Pair<TimeSeriesInterval, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList, String tsName) {
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
        if (fragmentSeriesList.isEmpty()) {
            return resultList;
        }
        int index = 0;
        while(index < fragmentSeriesList.size() && !fragmentSeriesList.get(index).k.isAfter(tsName)) {
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
        while(index < fragmentMetaList.size() && !fragmentMetaList.get(index).getTimeInterval().isAfter(timeInterval)) {
            if (fragmentMetaList.get(index).getTimeInterval().isIntersect(timeInterval)) {
                resultList.add(fragmentMetaList.get(index));
            }
            index++;
        }
        return resultList;
    }

    @Override
    public void initFragment(Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap) {
        storageUnitLock.readLock().lock();
        fragmentListMap.values().forEach(e -> e.forEach(f -> f.setMasterStorageUnit(storageUnitMetaMap.get(f.getMasterStorageUnitId()))));
        storageUnitLock.readLock().unlock();
        fragmentLock.writeLock().lock();
        sortedFragmentMetaLists.addAll(fragmentListMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
                .map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toList()));
        fragmentMetaListMap.putAll(fragmentListMap);
        fragmentLock.writeLock().unlock();
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
        fragmentLock.writeLock().unlock();
    }

    private void updateSortedFragmentsList(TimeSeriesInterval tsInterval, List<FragmentMeta> fragmentMetas) {
        Pair<TimeSeriesInterval, List<FragmentMeta>> pair = new Pair<>(tsInterval, fragmentMetas);
        if (sortedFragmentMetaLists.size() == 0) {
            sortedFragmentMetaLists.add(pair);
            return;
        }
        int left = 0, right = sortedFragmentMetaLists.size() - 1;
        while(left <= right) {
            int mid = (left + right) / 2;
            TimeSeriesInterval midTsInterval = sortedFragmentMetaLists.get(mid).k;
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
    public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        Map<TimeSeriesInterval, List<FragmentMeta>> resultMap = new HashMap<>();
        fragmentLock.readLock().lock();
        searchFragmentSeriesList(sortedFragmentMetaLists, tsInterval).forEach(e -> resultMap.put(e.k, e.v));
        fragmentLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMap() {
        Map<TimeSeriesInterval, FragmentMeta> latestFragmentMap = new HashMap<>();
        fragmentLock.readLock().lock();
        sortedFragmentMetaLists.stream().map(e -> e.v.get(e.v.size() - 1)).filter(e -> e.getTimeInterval().getEndTime() == Long.MAX_VALUE)
                .forEach(e -> latestFragmentMap.put(e.getTsInterval(), e));
        fragmentLock.readLock().unlock();
        return latestFragmentMap;
    }

    @Override
    public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        Map<TimeSeriesInterval, FragmentMeta> latestFragmentMap = new HashMap<>();
        fragmentLock.readLock().lock();
        searchFragmentSeriesList(sortedFragmentMetaLists, tsInterval).stream().map(e -> e.v.get(e.v.size() - 1)).filter(e -> e.getTimeInterval().getEndTime() == Long.MAX_VALUE)
                .forEach(e -> latestFragmentMap.put(e.getTsInterval(), e));
        fragmentLock.readLock().unlock();
        return latestFragmentMap;
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval, TimeInterval timeInterval) {
        Map<TimeSeriesInterval, List<FragmentMeta>> resultMap = new HashMap<>();
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
    public boolean hasFragment() {
        return !sortedFragmentMetaLists.isEmpty();
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
            }
        }
        storageUnitLock.readLock().unlock();
        return resultMap;
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
        storageEngineMetaMap.put(storageEngineMeta.getId(), storageEngineMeta);
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
    public void initActiveFragmentStatistics(Map<FragmentMeta, FragmentStatistics> statisticsMap) {
        activeFragmentStatisticsLock.writeLock().lock();
        activeFragmentStatisticsMap.putAll(statisticsMap);
        activeFragmentStatisticsLock.writeLock().unlock();
    }

    @Override
    public void addOrUpdateActiveFragmentStatistics(Map<FragmentMeta, FragmentStatistics> statisticsMap) {
        statisticsMap.forEach((key, value) -> activeFragmentStatisticsMap.computeIfAbsent(key, e -> new FragmentStatistics()).update(value));
    }

    @Override
    public Map<FragmentMeta, FragmentStatistics> getActiveFragmentStatistics() {
        return new HashMap<>(activeFragmentStatisticsMap);
    }

    @Override
    public void clearActiveFragmentStatistics() {
        activeFragmentStatisticsMap.clear();
    }

    @Override
    public void addOrUpdateDeltaActiveFragmentStatistics(Map<FragmentMeta, FragmentStatistics> statisticsMap) {
        deltaActiveFragmentStatisticsMap.putAll(statisticsMap);
    }

    @Override
    public Map<FragmentMeta, FragmentStatistics> getDeltaActiveFragmentStatistics() {
        return deltaActiveFragmentStatisticsMap;
    }

    @Override
    public void clearDeltaActiveFragmentStatistics() {
        deltaActiveFragmentStatisticsMap.clear();
    }

    @Override
    public void addOrUpdateActiveIginxStatistics(long id, Map<Long, StorageEngineStatistics> statisticsMap) {
        double totalDensity = statisticsMap.values().stream().mapToDouble(StorageEngineStatistics::getDensity).sum();
        activeIginxStatisticsMap.put(id, new IginxStatistics(totalDensity));
    }

    @Override
    public Map<Long, IginxStatistics> getActiveIginxStatistics() {
        return new HashMap<>(activeIginxStatisticsMap);
    }

    @Override
    public double getMinimalActiveIginxStatistics() {
        return activeIginxStatisticsMap.values().stream().filter(x -> x.getDensity() != 0.0).mapToDouble(IginxStatistics::getDensity).min().orElse(0.0);
    }

    @Override
    public void clearActiveIginxStatistics() {
        activeIginxStatisticsMap.clear();
    }

    @Override
    public void addOrUpdateActiveSeparatorStatistics(Set<String> separators) {
        activeSeparatorStatistics.addAll(separators);
    }

    @Override
    public Set<String> getActiveSeparatorStatistics() {
        return new TreeSet<>(activeSeparatorStatistics);
    }

    @Override
    public void clearActiveSeparatorStatistics() {
        activeSeparatorStatistics.clear();
    }

    @Override
    public Map<String, TimeSeriesStatistics> getActiveTimeSeriesStatistics() {
        return new HashMap<>(activeTimeSeriesStatisticsMap);
    }

    @Override
    public void clearActiveTimeSeriesStatistics() {
        activeTimeSeriesStatisticsMap.clear();
    }

    @Override
    public void addOrUpdateActiveTimeSeriesIntervalStatistics(Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> statisticsMap) {

    }

    @Override
    public Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> getActiveTimeSeriesIntervalStatistics() {
        return null;
    }

    @Override
    public void clearActiveTimeSeriesIntervalStatistics() {

    }

    @Override
    public Set<String> separateActiveTimeSeriesStatisticsByDensity(double density) {
        // TODO
        return null;
    }

    @Override
    public Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> separateActiveTimeSeriesStatisticsBySeparators(Set<String> separators) {
        return null;
    }

    @Override
    public void addOrUpdateActiveTimeSeriesStatistics(Map<String, TimeSeriesStatistics> statisticsMap) {
        statisticsMap.forEach((key, value) -> activeTimeSeriesStatisticsMap.computeIfAbsent(key, e -> new TimeSeriesStatistics()).update(value));
        for (TimeSeriesStatistics timeSeriesStatistics : statisticsMap.values()) {
            long storageEngineId = timeSeriesStatistics.getStorageEngineId();
            if (activeStorageEngineStatisticsMap.containsKey(storageEngineId)) {
                activeStorageEngineStatisticsMap.get(storageEngineId).updateByTimeSeriesStatistics(timeSeriesStatistics);
            } else {
                // TODO 先只考虑写入，且先不考虑存储后端的计算能力
                activeStorageEngineStatisticsMap.put(storageEngineId, new StorageEngineStatistics(timeSeriesStatistics.getWriteBytes(), 1.0, timeSeriesStatistics.getWriteBytes()));
            }
        }
    }

    @Override
    public Map<Long, StorageEngineStatistics> getActiveStorageEngineStatistics() {
        return new HashMap<>(activeStorageEngineStatisticsMap);
    }

    @Override
    public void addOrUpdateActiveStorageEngineStatistics(Map<Long, StorageEngineStatistics> statisticsMap) {
        statisticsMap.forEach((key, value) -> activeStorageEngineStatisticsMap.computeIfAbsent(key, e -> new StorageEngineStatistics()).updateByStorageEngineStatistics(value));
    }

    @Override
    public void clearActiveStorageEngineStatistics() {
        activeStorageEngineStatisticsMap.clear();
    }

    @Override
    public void addReshardFragment(FragmentMeta fragment) {
        reshardFragmentLock.writeLock().lock();
        reshardFragmentListMap.computeIfAbsent(fragment.getMasterStorageUnitId(), e -> {
            List<FragmentMeta> fragments = new ArrayList<>();
            fragments.add(fragment);
            return fragments;
        });
        reshardFragmentLock.writeLock().unlock();
    }

    @Override
    public List<FragmentMeta> getReshardFragmentsByStorageUnitId(String storageUnitId) {
        reshardFragmentLock.readLock().lock();
        List<FragmentMeta> fragments = reshardFragmentListMap.get(storageUnitId);
        reshardFragmentLock.readLock().unlock();
        return fragments;
    }

    @Override
    public void removeReshardFragmentsByStorageUnitId(String storageUnitId) {
        reshardFragmentLock.writeLock().lock();
        reshardFragmentListMap.remove(storageUnitId);
        reshardFragmentLock.writeLock().unlock();
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
}
