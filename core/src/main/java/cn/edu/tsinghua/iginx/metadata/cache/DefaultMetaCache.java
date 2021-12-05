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
import cn.edu.tsinghua.iginx.plan.InsertColumnRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.InsertRowRecordsPlan;
import cn.edu.tsinghua.iginx.policy.simple.TimeSeriesCalDO;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
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
    private final Map<Long, IginxStatistics> activeIginxStatisticsMap;

    private final Set<String> activeSeparatorStatistics;

    private final Map<Long, StorageEngineStatistics> activeStorageEngineStatisticsMap;

    private final Map<String, TimeSeriesStatistics> activeTimeSeriesStatisticsMap;

    private final Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> activeTimeSeriesIntervalStatisticsMap;

    // user 的缓存
    private final Map<String, UserMeta> userMetaMap;

    // 重分片过程中 fragment 的缓存
    // 为了解决重分片过程中 fragment 的创建先于 storage unit 的问题
    private final Map<String, List<FragmentMeta>> reshardFragmentListMap;

    private final ReadWriteLock reshardFragmentLock;
    // 时序列信息版本号的缓存
    private final Map<Integer, Integer> timeseriesVersionMap;

    private final ReadWriteLock insertRecordLock = new ReentrantReadWriteLock();

    private final Map<String, TimeSeriesCalDO> timeSeriesCalDOConcurrentHashMap = new ConcurrentHashMap<>();

    private Random random = new Random();

    private Config config = ConfigDescriptor.getInstance().getConfig();

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
        activeIginxStatisticsMap = new ConcurrentHashMap<>();
        activeSeparatorStatistics = new ConcurrentSkipListSet<>();
        activeStorageEngineStatisticsMap = new ConcurrentHashMap<>();
        activeTimeSeriesStatisticsMap = new ConcurrentSkipListMap<>();
        activeTimeSeriesIntervalStatisticsMap = new ConcurrentSkipListMap<>();
        // user 相关
        userMetaMap = new ConcurrentHashMap<>();
        // 重分片中的 fragment 相关
        reshardFragmentListMap = new ConcurrentHashMap<>();
        reshardFragmentLock = new ReentrantReadWriteLock();
        timeseriesVersionMap = new ConcurrentHashMap<>();
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
            if (o1.getTsInterval().getStartTimeSeries() == null && o2.getTsInterval().getStartTimeSeries() == null) {
                return 0;
            }
            else if (o1.getTsInterval().getStartTimeSeries() == null) {
                return -1;
            }
            else if (o2.getTsInterval().getStartTimeSeries() == null) {
                return 1;
            }
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
//        return Collections.unmodifiableList(new ArrayList<>(iginxMetaMap.values()));
        return new CopyOnWriteArrayList<>(iginxMetaMap.values());
    }

    @Override
    public void addIginx(IginxMeta iginxMeta) {
        iginxMetaMap.put(iginxMeta.getId(), iginxMeta);
    }

    @Override
    public void removeIginx(long id) {
        logger.info("remove iginx {}", id);
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
        if (this.schemaMappings.get(schema) == null) {
            return null;
        }
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
    public void addOrUpdateActiveIginxStatistics(long id, Map<Long, StorageEngineStatistics> statisticsMap) {
        double totalDensity = statisticsMap.values().stream().mapToDouble(StorageEngineStatistics::getDensity).sum();
        activeIginxStatisticsMap.put(id, new IginxStatistics(totalDensity));
    }

    @Override
    public Map<Long, IginxStatistics> getActiveIginxStatistics() {
        return new HashMap<>(activeIginxStatisticsMap);
    }

    @Override
    public double getMinActiveIginxStatistics() {
        return activeIginxStatisticsMap.values().stream().filter(x -> x.getDensity() != 0.0).mapToDouble(IginxStatistics::getDensity).min().orElse(0.0);
    }

    @Override
    public void clearActiveIginxStatistics() {
        activeIginxStatisticsMap.clear();
    }

    @Override
    public void addOrUpdateActiveSeparatorStatistics(Set<String> separators) {
        logger.info("separators = {}", separators);
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
        return new TreeMap<>(activeTimeSeriesStatisticsMap);
    }

    @Override
    public void clearActiveTimeSeriesStatistics() {
        activeTimeSeriesStatisticsMap.clear();
    }

    @Override
    public void addOrUpdateActiveTimeSeriesIntervalStatistics(Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> statisticsMap) {
        statisticsMap.forEach((key, value) -> activeTimeSeriesIntervalStatisticsMap.computeIfAbsent(key, e -> new TimeSeriesIntervalStatistics()).update(value));
        logger.info("1 activeTimeSeriesIntervalStatisticsMap = {}", activeTimeSeriesIntervalStatisticsMap);
    }

    @Override
    public Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> getActiveTimeSeriesIntervalStatistics() {
        logger.info("activeTimeSeriesIntervalStatisticsMap = {}", activeTimeSeriesIntervalStatisticsMap);
        return new TreeMap<>(activeTimeSeriesIntervalStatisticsMap);
    }

    @Override
    public void clearActiveTimeSeriesIntervalStatistics() {
        activeTimeSeriesIntervalStatisticsMap.clear();
    }

    @Override
    public Set<String> separateActiveTimeSeriesStatisticsByDensity(double density, Map<String, TimeSeriesStatistics> statisticsMap) {
        logger.info("1 density = {}", density);
        logger.info("1 statisticsMap = {}", statisticsMap);
        Set<String> separators = new TreeSet<>();
        double tempSum = 0.0;
        String tempTimeSeries = null;
        for (Map.Entry<String, TimeSeriesStatistics> entry : statisticsMap.entrySet()) {
            logger.info("1 entry = {}", entry);
            double currDensity = entry.getValue().getDensity();
            logger.info("1 tempSum + currDensity = {}", tempSum + currDensity);
            if (tempSum + currDensity >= density) {
                if (tempSum + currDensity - density > density - tempSum && tempTimeSeries != null) {
                    separators.add(tempTimeSeries);
                } else {
                    separators.add(entry.getKey());
                }
                tempSum = 0.0;
            } else {
                tempSum += currDensity;
            }
            tempTimeSeries = entry.getKey();
        }
        return separators;
    }

    @Override
    public Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> separateActiveTimeSeriesStatisticsBySeparators(Map<String, TimeSeriesStatistics> timeSeriesStatisticsMap, Set<String> separatorStatistics) {
        logger.info("timeSeriesStatisticsMap = {}", timeSeriesStatisticsMap);
        logger.info("separatorStatistics = {}", separatorStatistics);
        Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> statisticsMap = new TreeMap<>();
        String prevTimeSeries = null;
        String nextTimeSeries = null;
        double tempSum = 0.0;
        boolean needToGetNext = true;
        int tempCount = 0;
        Iterator<Map.Entry<String, TimeSeriesStatistics>> it = timeSeriesStatisticsMap.entrySet().iterator();
        Map.Entry<String, TimeSeriesStatistics> currEntry = null;
        for (String separator : separatorStatistics) {
            nextTimeSeries = separator;
            while (it.hasNext()) {
                if (needToGetNext) {
                    currEntry = it.next();
                }
                if (currEntry == null || currEntry.getKey().compareTo(separator) >= 0) {
                    break;
                } else {
                    tempSum += currEntry.getValue().getDensity();
                    needToGetNext = true;
                    tempCount++;
                }
            }
            statisticsMap.put(new TimeSeriesInterval(prevTimeSeries, nextTimeSeries), new TimeSeriesIntervalStatistics(tempSum));
            prevTimeSeries = nextTimeSeries;
            tempSum = 0.0;
            if (tempCount == 0) {
                needToGetNext = false;
            }
            tempCount = 0;
        }
        while (it.hasNext()) {
            currEntry = it.next();
            tempSum += currEntry.getValue().getDensity();
        }
        statisticsMap.put(new TimeSeriesInterval(nextTimeSeries, null), new TimeSeriesIntervalStatistics(tempSum));
        return statisticsMap;
    }

    @Override
    public void addOrUpdateActiveTimeSeriesStatistics(Map<String, TimeSeriesStatistics> statisticsMap) {
        // 更新本地的 activeTimeSeriesStatisticsMap
        statisticsMap.forEach((key, value) -> activeTimeSeriesStatisticsMap.computeIfAbsent(key, e -> new TimeSeriesStatistics()).update(value));
        // 更新本地的 activeStorageEngineStatisticsMap
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
        return new ConcurrentHashMap<>(activeStorageEngineStatisticsMap);
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

    @Override
    public void timeseriesIsUpdated(int node, int version) {
        timeseriesVersionMap.put(node, version);
    }

    long transDatatypeToByte(DataType dataType) {
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
    public void saveTimeSeriesData(InsertRecordsPlan plan) {
        insertRecordLock.writeLock().lock();
        Long now = System.currentTimeMillis();
        List<String> timeseries = plan.getPaths();
        List<Bitmap> bitmaps = plan.getBitmapList();
        int n = plan.getTimestamps().length;
        int m = plan.getPathsNum();
        if (plan instanceof InsertColumnRecordsPlan) {
            for (int i = 0; i < m; i++) {
                Long minn = Long.MAX_VALUE;
                Long maxx = Long.MIN_VALUE;
                Long totalbyte = 0L;
                Integer count = 0;
                for (int j = 0; j < n; j++) {
                    if (bitmaps.get(i).get(j)) {
                        minn = Math.min(minn, plan.getTimestamp(j));
                        maxx = Math.max(maxx, plan.getTimestamp(j));
                        if (plan.getDataType(i) == DataType.BINARY) {
                            totalbyte += ((byte[]) plan.getValues(i)[count]).length;
                        } else {
                            totalbyte += transDatatypeToByte(plan.getDataType(i));
                        }
                        count++;
                    }
                }
                if (count > 0) {
                    TimeSeriesCalDO timeSeriesCalDO = new TimeSeriesCalDO();
                    timeSeriesCalDO.setTimeSeries(timeseries.get(i));
                    if (timeSeriesCalDOConcurrentHashMap.containsKey(timeseries.get(i))) {
                        timeSeriesCalDO = timeSeriesCalDOConcurrentHashMap.get(timeseries.get(i));
                    }
                    timeSeriesCalDO.merge(now, minn, maxx, count, totalbyte);
                    timeSeriesCalDOConcurrentHashMap.put(timeseries.get(i), timeSeriesCalDO);
                }
            }
        } else if (plan instanceof InsertRowRecordsPlan) {
            for (int i = 0; i < m; i++)
            {
                Long minn = Long.MAX_VALUE;
                Long maxx = Long.MIN_VALUE;
                Long totalbyte = 0L;
                Integer count = 0;
                for (int j = 0; j < n; j++) {
                    if (bitmaps.get(j).get(i)) {
                        minn = Math.min(minn, plan.getTimestamp(j));
                        maxx = Math.max(maxx, plan.getTimestamp(j));
                        count++;
                        if (plan.getDataType(i) == DataType.BINARY) {
                            totalbyte += ((byte[]) plan.getValues(j)[i]).length;
                        } else {
                            totalbyte += transDatatypeToByte(plan.getDataType(i));
                        }
                    }
                }
                if (count > 0)
                {
                    TimeSeriesCalDO timeSeriesCalDO = new TimeSeriesCalDO();
                    timeSeriesCalDO.setTimeSeries(timeseries.get(i));
                    if (timeSeriesCalDOConcurrentHashMap.containsKey(timeseries.get(i)))
                    {
                        timeSeriesCalDO = timeSeriesCalDOConcurrentHashMap.get(timeseries.get(i));
                    }
                    timeSeriesCalDO.merge(now, minn, maxx, count, totalbyte);
                    timeSeriesCalDOConcurrentHashMap.put(timeseries.get(i), timeSeriesCalDO);
                }
            }
        }
        insertRecordLock.writeLock().unlock();
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
        return timeseriesVersionMap;
    }

}
