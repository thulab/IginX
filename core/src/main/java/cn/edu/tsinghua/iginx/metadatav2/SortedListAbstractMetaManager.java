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
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SortedListAbstractMetaManager extends AbstractMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(SortedListAbstractMetaManager.class);

    private static SortedListAbstractMetaManager INSTANCE = null;

    private final ReadWriteLock fragmentLock = new ReentrantReadWriteLock();

    private final List<Pair<TimeSeriesInterval, List<FragmentMeta>>> sortedFragmentMetaLists = new ArrayList<>();

    private final Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMetaListMap = new HashMap<>();

    @Override
    protected void initFragment(Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap) {
        fragmentLock.writeLock().lock();
        sortedFragmentMetaLists.addAll(fragmentListMap.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toList()));
        fragmentListMap.forEach(fragmentMetaListMap::put);
        fragmentLock.writeLock().unlock();
    }

    @Override
    protected void addFragment(FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.computeIfAbsent(fragmentMeta.getTsInterval(), v -> new ArrayList<>());
        fragmentMetaList.add(fragmentMeta);
        fragmentLock.writeLock().unlock();
    }

    @Override
    protected void updateFragment(FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
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
        resultList = searchFragmentList(searchFragmentSeriesList(sortedFragmentMetaLists, tsName).stream().map(e -> e.v).flatMap(List::stream).sorted((o1, o2) -> {
            if (o1.getTsInterval().getStartTimeSeries() == null && o2.getTsInterval().getStartTimeSeries() == null)
                return 0;
            else if (o1.getTsInterval().getStartTimeSeries() == null)
                return -1;
            else if (o2.getTsInterval().getStartTimeSeries() == null)
                return 1;
            return o1.getTsInterval().getStartTimeSeries().compareTo(o2.getTsInterval().getStartTimeSeries());
        }).collect(Collectors.toList()), timeInterval);
        fragmentLock.readLock().unlock();
        return resultList;
    }

    @Override
    public boolean hasFragment() {
        return !sortedFragmentMetaLists.isEmpty();
    }

    private List<Pair<TimeSeriesInterval, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList, TimeSeriesInterval tsInterval) {
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
        if (fragmentSeriesList.isEmpty()) {
            return resultList;
        }
        int left = 0, right = fragmentSeriesList.size();
        while (left <= right) {
            int mid = (left + right) / 2;
            if (fragmentSeriesList.get(mid).k.isBefore(tsInterval.getStartTimeSeries())) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        while (left < fragmentSeriesList.size() && fragmentSeriesList.get(left).k.isIntersect(tsInterval)) {
            resultList.add(fragmentSeriesList.get(left));
            left++;
        }
        return resultList;
    }

    private List<Pair<TimeSeriesInterval, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList, String tsName) {
        List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
        if (fragmentSeriesList.isEmpty()) {
            return resultList;
        }
        int left = 0, right = fragmentSeriesList.size();
        while (left <= right) {
            int mid = (left + right) / 2;
            if (fragmentSeriesList.get(mid).k.isBefore(tsName)) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        while (left < fragmentSeriesList.size() && fragmentSeriesList.get(left).k.isContain(tsName)) {
            resultList.add(fragmentSeriesList.get(left));
            left++;
        }
        return resultList;
    }

    private List<FragmentMeta> searchFragmentList(List<FragmentMeta> fragmentMetaList, TimeInterval timeInterval) {
        List<FragmentMeta> resultList = new ArrayList<>();
        if (fragmentMetaList.isEmpty()) {
            return resultList;
        }
        int left = 0, right = fragmentMetaList.size();
        while (left <= right) {
            int mid = (left + right) / 2;
            if (fragmentMetaList.get(mid).getTimeInterval().isBefore(timeInterval)) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        while (left < fragmentMetaList.size() && fragmentMetaList.get(left).getTimeInterval().isIntersect(timeInterval)) {
            resultList.add(fragmentMetaList.get(left));
            left++;
        }
        return resultList;
    }

    public void shutdown() {
        this.iginxCache.close();
        this.storageEngineCache.close();
        this.fragmentCache.close();
        synchronized (SortedListAbstractMetaManager.class) {
            SortedListAbstractMetaManager.INSTANCE = null;
        }
    }

    public static SortedListAbstractMetaManager getInstance() {
        if (INSTANCE == null) {
            synchronized (SortedListAbstractMetaManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new SortedListAbstractMetaManager();
                }
            }
        }
        return INSTANCE;
    }
}
