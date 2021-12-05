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
package cn.edu.tsinghua.iginx.policy.simple;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.plan.*;
import cn.edu.tsinghua.iginx.plan.downsample.*;
import cn.edu.tsinghua.iginx.policy.IPlanSplitter;
import cn.edu.tsinghua.iginx.split.SplitInfo;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

class SimplePlanSplitter implements IPlanSplitter {

    private static final Logger logger = LoggerFactory.getLogger(SimplePlanSplitter.class);

    private final IMetaManager iMetaManager;

    private final SimplePolicy policy;

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    List<String> getNewFragment(Map<String, Double> data) {
        List<String> ret = new ArrayList<>();
        int n = data.size();
        int m = iMetaManager.getStorageEngineNum() * config.getFragmentPerEngine();
        if (config.isEnableStorageGroupValueLimit()) {
            double totalValue = data.values().stream().mapToDouble(Double::doubleValue).sum();
            m = new Double(Math.ceil(totalValue / config.getStorageGroupValueLimit() / iMetaManager.getStorageEngineNum())).
                    intValue() * iMetaManager.getStorageEngineNum();
        }
        List<Pair<String, Double>> tmp = data.entrySet().stream().map(entry -> new Pair<String, Double>(entry.getKey(), entry.getValue())).
                sorted(Comparator.comparing(Pair::getK)).collect(Collectors.toList());
        List<Double> sum = new ArrayList<>();
        sum.add(0.0);
        for (int i = 0; i < n; i++) {
            sum.add(sum.get(i) + tmp.get(i).v);
        }
        double[][] dp = new double[n + 1][m + 1];
        int[][] last = new int[n + 1][m + 1];
        for (int i = 0; i <= n; i++) {
            for (int j = 0; j <= m; j++) {
                dp[i][j] = Double.MAX_VALUE / 10;
                last[i][j] = -1;
            }
        }
        dp[0][0] = 0;
        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                for (int k = 0; k < i; k++) {
                    double tmpValue = dp[k][j - 1] + Math.pow(sum.get(i) - sum.get(k), 2);
                    if (tmpValue < dp[i][j]) {
                        dp[i][j] = tmpValue;
                        last[i][j] = k;
                    }
                }
            }
        }
        int tmpn = n, tmpm = m;
        while (last[tmpn][tmpm] > 0) {
            tmpn = last[tmpn][tmpm];
            tmpm --;
            ret.add(tmp.get(tmpn - 1).k);
        }
        Collections.reverse(ret);
        return ret;
    }

    public SimplePlanSplitter(SimplePolicy policy, IMetaManager iMetaManager) {
        this.policy = policy;
        this.iMetaManager = iMetaManager;
    }

    public static List<TimeInterval> splitTimeIntervalForDownsampleQuery(List<TimeInterval> timeIntervals,
                                                                         long beginTime, long endTime, long precision) {
        List<TimeInterval> resultList = new ArrayList<>();
        for (TimeInterval timeInterval : timeIntervals) {
            long midIntervalBeginTime = Math.max(timeInterval.getStartTime(), beginTime);
            long midIntervalEndTime = Math.min(timeInterval.getEndTime(), endTime);
            if (timeInterval.getStartTime() > beginTime && (timeInterval.getStartTime() - beginTime) % precision != 0) { // 只有非第一个分片才有可能创建前缀分片
                long prefixIntervalEndTime = Math.min(midIntervalBeginTime + precision - (timeInterval.getStartTime() - beginTime) % precision, midIntervalEndTime);
                resultList.add(new TimeInterval(midIntervalBeginTime, prefixIntervalEndTime));
                midIntervalBeginTime = prefixIntervalEndTime;
            }
            if ((midIntervalEndTime - midIntervalBeginTime) >= precision) {
                midIntervalEndTime -= (midIntervalEndTime - midIntervalBeginTime) % precision;
                resultList.add(new TimeInterval(midIntervalBeginTime, midIntervalEndTime));
            } else {
                midIntervalEndTime = midIntervalBeginTime;
            }
            if (midIntervalEndTime != Math.min(timeInterval.getEndTime(), endTime)) {
                resultList.add(new TimeInterval(midIntervalEndTime, Math.min(timeInterval.getEndTime(), endTime)));
            }
        }
        return resultList;
    }

    @Override
    public List<SplitInfo> getSplitDeleteColumnsPlanResults(DeleteColumnsPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesInterval(plan.getTsInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    logger.info("add storage unit id {} to duplicate remove set.", storageUnit.getId());
                    infoList.add(new SplitInfo(new TimeInterval(0L, Long.MAX_VALUE), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }


    @Override
    public List<SplitInfo> getSplitInsertColumnRecordsPlanResults(InsertColumnRecordsPlan plan) {
        iMetaManager.saveTimeSeriesData(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        if (fragmentMap.isEmpty()) {
            //on startup
            policy.setNeedReAllocate(false);
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.getIFragmentGenerator().generateInitialFragmentsAndStorageUnits(plan.getPaths(), plan.getTimeInterval());
            iMetaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
        } else if (policy.isNeedReAllocate()) {
            Map<String, Double> data = iMetaManager.getTimeseriesData();
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.getIFragmentGenerator().generateFragmentsAndStorageUnits(getNewFragment(data), plan.getEndTime() + TimeUnit.SECONDS.toMillis(ConfigDescriptor.getInstance().getConfig().getDisorderMargin()) * 2 + 1);
            iMetaManager.createFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            logger.info("create fragment");
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        String sign = RandomStringUtils.randomAlphanumeric(3);
        for (SplitInfo splitInfo: infoList) {
            logger.info("sign: {}, splitinfo: {}", sign, splitInfo);
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitInsertNonAlignedColumnRecordsPlanResults(InsertNonAlignedColumnRecordsPlan plan) {
        return getSplitInsertColumnRecordsPlanResults(plan);
    }

    @Override
    public List<SplitInfo> getSplitInsertRowRecordsPlanResults(InsertRowRecordsPlan plan) {
        iMetaManager.saveTimeSeriesData(plan);
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        if (fragmentMap.isEmpty()) {
            policy.setNeedReAllocate(false);
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.getIFragmentGenerator().generateInitialFragmentsAndStorageUnits(plan.getPaths(), plan.getTimeInterval());
            iMetaManager.createInitialFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
        } else if (policy.isNeedReAllocate()) {
            Map<String, Double> data = iMetaManager.getTimeseriesData();
            Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = policy.getIFragmentGenerator().generateFragmentsAndStorageUnits(getNewFragment(data), plan.getEndTime() + TimeUnit.SECONDS.toMillis(ConfigDescriptor.getInstance().getConfig().getDisorderMargin()) * 2 + 1);
            iMetaManager.createFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
            logger.info("create fragment");
        }
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitInsertNonAlignedRowRecordsPlanResults(InsertNonAlignedRowRecordsPlan plan) {
        return getSplitInsertRowRecordsPlanResults(plan);
    }

    @Override
    public List<SplitInfo> getSplitDeleteDataInColumnsPlanResults(DeleteDataInColumnsPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, false);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitQueryDataPlanResults(QueryDataPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    private List<SplitInfo> getSplitResultsForDownsamplePlan(DownsampleQueryPlan plan, IginxPlan.IginxPlanType intervalQueryPlan) {
        // 初始化结果集
        List<SplitInfo> infoList = new ArrayList<>();
        // 对查出来的分片结果按照开始时间进行排序、分组
        List<List<FragmentMeta>> fragmentMetasList = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval()).values().stream().flatMap(List::stream)
                .sorted(Comparator.comparingLong(e -> e.getTimeInterval().getStartTime()))
                .collect(Collectors.groupingBy(e -> e.getTimeInterval().getStartTime())).entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getKey)).map(Map.Entry::getValue).collect(Collectors.toList());
        // 聚合精度，后续会多次用到
        long precision = plan.getPrecision();
        // 计算子查询时间片列表
        List<TimeInterval> planTimeIntervals = splitTimeIntervalForDownsampleQuery(fragmentMetasList.stream()
                .map(e -> e.get(0).getTimeInterval()).collect(Collectors.toList()), plan.getStartTime(), plan.getEndTime(), plan.getPrecision());
        // 所属的合并组的组号
        int combineGroup = 0;
        int index = 0;
        long timespan = 0L;
        for (List<FragmentMeta> fragmentMetas : fragmentMetasList) {
            long endTime = fragmentMetas.get(0).getTimeInterval().getEndTime();
            while (index < planTimeIntervals.size() && planTimeIntervals.get(index).getEndTime() <= endTime) {
                TimeInterval timeInterval = planTimeIntervals.get(index++);
                if (timeInterval.getSpan() >= precision) {
                    // 对于聚合子查询，清空 timespan，并且在计划全部加入之后增加组号
                    for (FragmentMeta fragment : fragmentMetas) {
                        List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                        for (StorageUnitMeta storageUnit : storageUnitList) {
                            infoList.add(new SplitInfo(timeInterval, fragment.getTsInterval(), storageUnit, plan.getIginxPlanType(), combineGroup));
                        }
                    }
                    timespan = 0L;
                    combineGroup += 1;
                } else {
                    for (FragmentMeta fragment : fragmentMetas) {
                        List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                        for (StorageUnitMeta storageUnit : storageUnitList) {
                            infoList.add(new SplitInfo(timeInterval, fragment.getTsInterval(), storageUnit, intervalQueryPlan, combineGroup));
                        }
                    }
                    timespan += timeInterval.getSpan();
                    if (timespan >= precision) {
                        timespan = 0L;
                        combineGroup += 1;
                    }
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitDownsampleMaxQueryPlanResults(DownsampleMaxQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.MAX);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleMinQueryPlanResults(DownsampleMinQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.MIN);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleSumQueryPlanResults(DownsampleSumQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.SUM);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleCountQueryPlanResults(DownsampleCountQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.COUNT);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleAvgQueryPlanResults(DownsampleAvgQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.AVG);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleFirstQueryPlanResults(DownsampleFirstValueQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.FIRST_VALUE);
    }

    @Override
    public List<SplitInfo> getSplitDownsampleLastQueryPlanResults(DownsampleLastValueQueryPlan plan) {
        return getSplitResultsForDownsamplePlan(plan, IginxPlan.IginxPlanType.LAST_VALUE);
    }

    @Override
    public List<SplitInfo> getValueFilterQueryPlanResults(ValueFilterQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getLastQueryPlanResults(LastQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitMaxQueryPlanResults(MaxQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitMinQueryPlanResults(MinQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitSumQueryPlanResults(SumQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitCountQueryPlanResults(CountQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitAvgQueryPlanResults(AvgQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = iMetaManager.getFragmentMapByTimeSeriesIntervalAndTimeInterval(
                plan.getTsInterval(), plan.getTimeInterval());
        for (Map.Entry<TimeSeriesInterval, List<FragmentMeta>> entry : fragmentMap.entrySet()) {
            for (FragmentMeta fragment : entry.getValue()) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), entry.getKey(), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitFirstQueryPlanResults(FirstValueQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FragmentMeta> fragmentList = iMetaManager.getFragmentListByTimeSeriesNameAndTimeInterval(path, plan.getTimeInterval());
            for (FragmentMeta fragment : fragmentList) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path, true), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<SplitInfo> getSplitLastQueryPlanResults(LastValueQueryPlan plan) {
        List<SplitInfo> infoList = new ArrayList<>();
        for (String path : plan.getPaths()) {
            List<FragmentMeta> fragmentList = iMetaManager.getFragmentListByTimeSeriesNameAndTimeInterval(path, plan.getTimeInterval());
            for (FragmentMeta fragment : fragmentList) {
                List<StorageUnitMeta> storageUnitList = selectStorageUnitList(fragment, true);
                for (StorageUnitMeta storageUnit : storageUnitList) {
                    infoList.add(new SplitInfo(fragment.getTimeInterval(), new TimeSeriesInterval(path, path, true), storageUnit, fragment));
                }
            }
        }
        return infoList;
    }

    @Override
    public List<Long> getSplitShowColumnsPlanResult() {
        return iMetaManager.getStorageEngineList().stream().map(StorageEngineMeta::getId).collect(Collectors.toList());
    }

    @Override
    public List<StorageUnitMeta> selectStorageUnitList(FragmentMeta fragment, boolean isQuery) {
        List<StorageUnitMeta> storageUnitList = new ArrayList<>();
        // TODO 暂时设置为只查主
        storageUnitList.add(fragment.getMasterStorageUnit());
        if (!isQuery) {
            storageUnitList.addAll(fragment.getMasterStorageUnit().getReplicas());
        }
        return storageUnitList;
    }
}
