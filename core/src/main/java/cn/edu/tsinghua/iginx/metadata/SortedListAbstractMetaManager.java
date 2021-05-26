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
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SortedListAbstractMetaManager extends AbstractMetaManager {

	private static SortedListAbstractMetaManager INSTANCE = null;

	private List<Pair<TimeSeriesInterval, List<FragmentMeta>>> sortedFragmentMetaLists;

	private Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMetaListMap;

	private ReadWriteLock fragmentLock;

	private SortedListAbstractMetaManager() {
		if (sortedFragmentMetaLists == null) {
			sortedFragmentMetaLists = new ArrayList<>();
		}
		if (fragmentMetaListMap == null) {
			fragmentMetaListMap = new HashMap<>();
		}
		if (fragmentLock == null) {
			fragmentLock = new ReentrantReadWriteLock();
		}
	}

	public static List<Pair<TimeSeriesInterval, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList, TimeSeriesInterval tsInterval) {
		List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
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

	public static List<Pair<TimeSeriesInterval, List<FragmentMeta>>> searchFragmentSeriesList(List<Pair<TimeSeriesInterval, List<FragmentMeta>>> fragmentSeriesList, String tsName) {
		List<Pair<TimeSeriesInterval, List<FragmentMeta>>> resultList = new ArrayList<>();
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

	public static List<FragmentMeta> searchFragmentList(List<FragmentMeta> fragmentMetaList, TimeInterval timeInterval) {
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

	@Override
	protected void initFragment(Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap) {
		sortedFragmentMetaLists = new ArrayList<>();
		fragmentMetaListMap = new HashMap<>();
		if (fragmentLock == null) {
			fragmentLock = new ReentrantReadWriteLock();
		}
		fragmentLock.writeLock().lock();
		sortedFragmentMetaLists.addAll(fragmentListMap.entrySet().stream().sorted(Map.Entry.comparingByKey())
				.map(e -> new Pair<>(e.getKey(), e.getValue())).collect(Collectors.toList()));
		fragmentListMap.forEach(fragmentMetaListMap::put);
		fragmentLock.writeLock().unlock();
	}

	@Override
	protected void addFragment(FragmentMeta fragmentMeta) {
		fragmentLock.writeLock().lock();
		// 更新 fragmentMetaListMap
		List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.computeIfAbsent(fragmentMeta.getTsInterval(), v -> new ArrayList<>());
		if (fragmentMetaList.size() == 0) {
			// 更新 sortedFragmentMetaLists
			updateSortedFragmentsList(fragmentMeta.getTsInterval(), fragmentMetaList);
		}
		fragmentMetaList.add(fragmentMeta);
		fragmentLock.writeLock().unlock();
		for (FragmentReplicaMeta fragmentReplicaMeta : fragmentMeta.getReplicaMetas().values()) {
			storageEngineMetaMap.get(fragmentReplicaMeta.getStorageEngineId()).addFragmentReplicaMeta(fragmentReplicaMeta);
		}
	}

	public void updateSortedFragmentsList(TimeSeriesInterval tsInterval, List<FragmentMeta> fragmentMetas) {
		Pair<TimeSeriesInterval, List<FragmentMeta>> pair = new Pair<>(tsInterval, fragmentMetas);
		if (sortedFragmentMetaLists.size() == 0) {
			sortedFragmentMetaLists.add(pair);
			return;
		}
		int left = 0, right = sortedFragmentMetaLists.size() - 1;
		while (left <= right) {
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
	protected void updateFragment(FragmentMeta fragmentMeta) {
		fragmentLock.writeLock().lock();
		// 更新 fragmentMetaListMap
		List<FragmentMeta> fragmentMetaList = fragmentMetaListMap.get(fragmentMeta.getTsInterval());
		fragmentMetaList.set(fragmentMetaList.size() - 1, fragmentMeta);
		fragmentLock.writeLock().unlock();
		for (FragmentReplicaMeta replicaMeta : fragmentMeta.getReplicaMetas().values()) {
			long storageEngineId = replicaMeta.getStorageEngineId();
			StorageEngineMeta storageEngineMeta = storageEngineMetaMap.get(storageEngineId);
			storageEngineMeta.endLatestFragmentReplicaMetas(replicaMeta.getTsInterval(), replicaMeta.getTimeInterval().getEndTime());
		}
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

	public void shutdown() {
		this.iginxCache.close();
		this.storageEngineCache.close();
		this.fragmentCache.close();
		synchronized (SortedListAbstractMetaManager.class) {
			SortedListAbstractMetaManager.INSTANCE = null;
		}
	}
}
