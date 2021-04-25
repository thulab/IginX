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

import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SortedListMetaManager extends AbstractMetaManager {

    private static SortedListMetaManager INSTANCE = null;

    private List<Pair<Long, List<FragmentMeta>>> sortedFragmentMetaLists;

    private Map<Long, List<FragmentMeta>> fragmentMetaListMap;

    private final ReadWriteLock fragmentLock = new ReentrantReadWriteLock();

    private SortedListMetaManager() {
        if (sortedFragmentMetaLists == null) {
            sortedFragmentMetaLists = new ArrayList<>();
        }
        if (fragmentMetaListMap == null) {
            fragmentMetaListMap = new HashMap<>();
        }
    }

    @Override
    protected void initFragment(Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap) {
        fragmentMetaListMap = new HashMap<>();

        sortedFragmentMetaLists = fragmentListMap.values().stream().flatMap(List::stream)
                .collect(Collectors.groupingBy(e -> e.getTimeInterval().getStartTime()))
                .entrySet().stream().map(e -> new Pair<>(e.getKey(), e.getValue()))
                .sorted(Comparator.comparingLong(o -> o.k)).collect(Collectors.toList());

        for (int i = 0; i < sortedFragmentMetaLists.size(); i++) {
            Pair<Long, List<FragmentMeta>> pair = sortedFragmentMetaLists.get(i);
            if (i != 0) {
                pair.v.addAll(sortedFragmentMetaLists.get(i - 1).v.stream()
                        .filter(e -> e.getTimeInterval().getEndTime() > pair.k).collect(Collectors.toList()));
            }
            pair.v.sort(Comparator.comparing(FragmentMeta::getTsInterval));

            fragmentMetaListMap.put(pair.k, pair.v);
        }
    }

    @Override
    protected void addFragment(FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        long timestamp = fragmentMeta.getTimeInterval().getStartTime();
        List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.computeIfAbsent(timestamp, k -> new ArrayList<>());
        if (fragmentMetaList.size() == 0) { // 之前没有
            sortedFragmentMetaLists.add(searchSortedFragmentMetaLists(sortedFragmentMetaLists, timestamp) + 1, new Pair<>(timestamp, fragmentMetaList));
        }
        insertIntoSortedFragmentMetaList(fragmentMetaList, fragmentMeta);
        fragmentLock.writeLock().unlock();
    }

    @Override
    protected void updateFragment(FragmentMeta fragmentMeta) {
        fragmentLock.writeLock().lock();
        long timestamp = fragmentMeta.getTimeInterval().getStartTime();
        if (fragmentMetaListMap.get(timestamp) == null) {
            fragmentLock.writeLock().unlock();
            return; // 更新不存在的分片
        }
        // 二分查找到分片的位置
        int index = searchSortedFragmentMetaLists(sortedFragmentMetaLists, timestamp);
        if (index == -1) {
            fragmentLock.writeLock().unlock();
            return; // 不应该没找到分片
        }
        for (int i = index; i < sortedFragmentMetaLists.size(); i++) {
            deleteFromSortedFragmentMetaList(sortedFragmentMetaLists.get(i).v, fragmentMeta);
        }
        insertIntoSortedFragmentMetaList(sortedFragmentMetaLists.get(index).v, fragmentMeta);
        fragmentLock.writeLock().unlock();
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        Map<TimeSeriesInterval, List<FragmentMeta>> resultMap;
        fragmentLock.readLock().lock();
        resultMap = sortedFragmentMetaLists.stream().map(e -> e.v).map(e -> searchSortedFragmentMetaList(e, tsInterval)).flatMap(List::stream)
                .distinct().collect(Collectors.groupingBy(FragmentMeta::getTsInterval));
        fragmentLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        Map<TimeSeriesInterval, FragmentMeta> resultMap = new HashMap<>();
        fragmentLock.readLock().lock();
        if (sortedFragmentMetaLists.size() != 0) {
            searchSortedFragmentMetaList(sortedFragmentMetaLists.get(sortedFragmentMetaLists.size() - 1).v, tsInterval)
                    .forEach(e -> resultMap.put(e.getTsInterval(), e));
        }
        fragmentLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMap() {
        Map<TimeSeriesInterval, FragmentMeta> resultMap = new HashMap<>();
        fragmentLock.readLock().lock();
        if (sortedFragmentMetaLists.size() != 0) {
            sortedFragmentMetaLists.get(sortedFragmentMetaLists.size() - 1).v
                    .forEach(e -> resultMap.put(e.getTsInterval(), e));
        }
        fragmentLock.readLock().unlock();
        return resultMap;
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval, TimeInterval timeInterval) {
        Set<FragmentMeta> resultSet = new HashSet<>();
        fragmentLock.readLock().lock();
        for (int i = Math.max(searchSortedFragmentMetaLists(sortedFragmentMetaLists, timeInterval.getStartTime()), 0);
             i < sortedFragmentMetaLists.size() && sortedFragmentMetaLists.get(i).k <= timeInterval.getEndTime(); i++) {
            resultSet.addAll(searchSortedFragmentMetaList(sortedFragmentMetaLists.get(i).v, tsInterval));
        }
        fragmentLock.readLock().unlock();
        return resultSet.stream().collect(Collectors.groupingBy(FragmentMeta::getTsInterval));
    }


    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval) {
        List<FragmentMeta> resultList = new ArrayList<>();
        fragmentLock.readLock().lock();
        long nextTimestamp = 0L;
        for (int i = Math.max(searchSortedFragmentMetaLists(sortedFragmentMetaLists, timeInterval.getStartTime()), 0);
             i < sortedFragmentMetaLists.size() && sortedFragmentMetaLists.get(i).k <= timeInterval.getEndTime(); i++) {
            if (sortedFragmentMetaLists.get(i).k < nextTimestamp)
                continue;
            FragmentMeta fragmentMeta = searchSortedFragmentMetaList(sortedFragmentMetaLists.get(i).v, tsName);
            if (fragmentMeta != null) {
                nextTimestamp = fragmentMeta.getTimeInterval().getEndTime();
                resultList.add(fragmentMeta);
            }
        }
        fragmentLock.readLock().unlock();
        return resultList;
    }

    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesName(String tsName) {
        List<FragmentMeta> resultList = new ArrayList<>();
        fragmentLock.readLock().lock();
        long nextTimestamp = 0L;
        for (Pair<Long, List<FragmentMeta>> sortedFragmentMetaList : sortedFragmentMetaLists) {
            if (sortedFragmentMetaList.k < nextTimestamp)
                continue;
            FragmentMeta fragmentMeta = searchSortedFragmentMetaList(sortedFragmentMetaList.v, tsName);
            if (fragmentMeta != null) {
                nextTimestamp = fragmentMeta.getTimeInterval().getEndTime();
                resultList.add(fragmentMeta);
            }
        }
        fragmentLock.readLock().unlock();
        return resultList;
    }

    @Override
    public FragmentMeta getLatestFragmentByTimeSeriesName(String tsName) {
        FragmentMeta fragment = null;
        fragmentLock.readLock().lock();
        if (sortedFragmentMetaLists.size() != 0) {
            fragment = searchSortedFragmentMetaList(sortedFragmentMetaLists.get(sortedFragmentMetaLists.size() - 1).v, tsName);
        }
        fragmentLock.readLock().unlock();
        return fragment;
    }

    @Override
    public boolean hasFragment() {
        return sortedFragmentMetaLists.size() != 0;
    }

    // 找到最后一个小于等于 timestamp 的位置
    private static int searchSortedFragmentMetaLists(List<Pair<Long, List<FragmentMeta>>> sortedFragmentMetaLists, long timestamp) {
        if (sortedFragmentMetaLists.size() == 0) {
            return -1;
        }
        int left = 0, right = sortedFragmentMetaLists.size() - 1;
        while (left < right) {
            int mid = (left + right + 1) / 2;
            if (sortedFragmentMetaLists.get(mid).k <= timestamp) {
                left = mid;
            } else {
                right = mid - 1;
            }
        }
        if (sortedFragmentMetaLists.get(left).k <= timestamp) {
            return left;
        }
        return -1;
    }

    // 在排好序的分片列表中插入一个分片
    private static void insertIntoSortedFragmentMetaList(List<FragmentMeta> sortedFragmentList, FragmentMeta fragment) {
        if (sortedFragmentList.size() == 0 ||
                sortedFragmentList.get(sortedFragmentList.size() - 1).getTsInterval().compareTo(fragment.getTsInterval()) < 0) {
            sortedFragmentList.add(fragment);
            return;
        }
        int left = 0, right = sortedFragmentList.size() - 1;
        while (left < right) {
            int mid = (left + right) / 2;
            if (sortedFragmentList.get(mid).getTsInterval().compareTo(fragment.getTsInterval()) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        sortedFragmentList.add(left, fragment);
    }

    // 在排好序的分片列表中删除一个分片
    private static void deleteFromSortedFragmentMetaList(List<FragmentMeta> sortedFragmentList, FragmentMeta fragment) {
        if (sortedFragmentList.size() == 0 ||
                sortedFragmentList.get(sortedFragmentList.size() - 1).getTsInterval().compareTo(fragment.getTsInterval()) < 0) {
            return;
        }
        int left = 0, right = sortedFragmentList.size() - 1;
        while (left <= right) {
            int mid = (left + right) / 2;
            if (sortedFragmentList.get(mid).getTsInterval().compareTo(fragment.getTsInterval()) == 0) {
                sortedFragmentList.remove(mid);
                return;
            } else if (sortedFragmentList.get(mid).getTsInterval().compareTo(fragment.getTsInterval()) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
    }

    // 在排好序的分片列表中搜索与给定分片相交的所有分片
    private static List<FragmentMeta> searchSortedFragmentMetaList(List<FragmentMeta> fragmentMetaList, TimeSeriesInterval tsInterval) {
        List<FragmentMeta> resultList = new ArrayList<>();
        for (FragmentMeta fragmentMeta: fragmentMetaList) {
            if (fragmentMeta.getTsInterval().isIntersect(tsInterval))
                resultList.add(fragmentMeta);
        }
        return resultList;
    }

    // 在排好序的分片列表中搜索包含某个时间序列的分片
    private static FragmentMeta searchSortedFragmentMetaList(List<FragmentMeta> fragmentMetaList, String tsName) {
        for (FragmentMeta fragmentMeta: fragmentMetaList) {
            if (fragmentMeta.getTsInterval().isContain(tsName))
                return fragmentMeta;
        }
        return null;
    }

    public static SortedListMetaManager getInstance() {
        if (INSTANCE == null) {
            synchronized (SortedListMetaManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new SortedListMetaManager();
                }
            }
        }
        return INSTANCE;
    }

}
