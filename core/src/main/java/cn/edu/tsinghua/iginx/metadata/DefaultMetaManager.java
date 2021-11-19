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
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.cache.DefaultMetaCache;
import cn.edu.tsinghua.iginx.metadata.cache.IMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesIntervalStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesStatistics;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.etcd.ETCDMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.file.FileMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.zk.ZooKeeperMetaStorage;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;
import cn.edu.tsinghua.iginx.policy.IFragmentGenerator;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.EXECUTING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.JUDGING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.NON_RESHARDING;

public class DefaultMetaManager implements IMetaManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMetaManager.class);
    private static DefaultMetaManager INSTANCE;
    private final IMetaCache cache;

    private final IMetaStorage storage;
    private final List<StorageEngineChangeHook> storageEngineChangeHooks;

    private long id;

    private final ScheduledExecutorService reshardService;

    private Queue<Thread> waitingReshardThreadsQueue = new ConcurrentLinkedQueue<>();

    // 当前活跃的最大的结束时间
    private AtomicLong maxActiveEndTime = new AtomicLong(-1L);

    // 当前活跃的开始时间
    private AtomicLong activeStartTime = new AtomicLong(-1L);

    // 上一次重分片的结束时间
    private AtomicLong lastReshardTime = new AtomicLong(-1L);

    private AtomicInteger maxActiveEndTimeStatisticsCounter = new AtomicInteger(0);

    private AtomicInteger activeSeparatorStatisticsCounter = new AtomicInteger(0);

    private AtomicInteger activeStorageEngineStatisticsCounter = new AtomicInteger(0);

    private AtomicInteger activeTimeSeriesIntervalStatisticsCounter = new AtomicInteger(0);

    // 重分片状态
    private ReshardStatus reshardStatus = NON_RESHARDING;

    // 在重分片过程中，是否为提出者
    private boolean isProposer = false;

    // 在重分片过程中，是否已经推了本地的统计数据的更新
    // TODO 不确定是否有用
    private boolean hasPushedStatistics = false;

    // 在重分片过程中，是否已经进行了重分片判断
    private boolean hasJudgedResharding = false;

    private final Object judgedResharding = new Object();

    // 在重分片过程中，是否已经提交了创建分片和存储单元的任务
    private boolean hasCommittedCreatingTask = false;

    private final Object commitCreatingTask = new Object();

    // 在重分片过程中，是否已经创建了分片
    private boolean hasCreatedFragments = false;

    // 在重分片过程中，是否已经创建了存储单元
    private boolean hasCreatedStorageUnits = false;

    private boolean needToCreateStorageUnits = false;

    private final Object terminateResharding = new Object();

    private DefaultMetaManager() {
        cache = DefaultMetaCache.getInstance();

        switch (ConfigDescriptor.getInstance().getConfig().getMetaStorage()) {
            case Constants.ZOOKEEPER_META:
                logger.info("use zookeeper as meta storage.");
                storage = ZooKeeperMetaStorage.getInstance();
                break;
            case Constants.FILE_META:
                logger.info("use file as meta storage");
                storage = FileMetaStorage.getInstance();
                break;
            case Constants.ETCD_META:
                logger.info("use etcd as meta storage");
                storage = ETCDMetaStorage.getInstance();
                break;
            case "":
                //without configuration, file storage should be the safe choice
                logger.info("doesn't specify meta storage, use file as meta storage.");
                storage = FileMetaStorage.getInstance();
                break;
            default:
                //without configuration, file storage should be the safe choice
                logger.info("unknown meta storage, use file as meta storage.");
                storage = FileMetaStorage.getInstance();
                break;
        }

        storageEngineChangeHooks = Collections.synchronizedList(new ArrayList<>());

        try {
            initIginx();
            initStorageEngine();
            initStorageUnit();
            initFragment();
            initSchemaMapping();
            initUser();
            initMaxActiveEndTimeStatistics();
            initMinActiveIginxStatistics();
            initActiveSeparatorStatistics();
            initActiveStorageEngineStatistics();
            initActiveTimeSeriesIntervalStatistics();
            initReshardStatus();
            initReshardCounter();
        } catch (MetaStorageException e) {
            logger.error("encounter error when initiating meta manager: ", e);
            System.exit(-1);
        }

        reshardService = new ScheduledThreadPoolExecutor(1);
        if (ConfigDescriptor.getInstance().getConfig().isEnableGlobalStatistics()) {
            reshardService.scheduleAtFixedRate(
                () -> {
                    try {
                        if (!reshardStatus.equals(NON_RESHARDING)) {
                            return;
                        }
                        logger.info("System.currentTimeMillis() = {} lastReshardTime.get() = {} ConfigDescriptor.getInstance().getConfig().getGlobalStatisticsCollectInterval() * 1000 = {}", System.currentTimeMillis(), lastReshardTime.get(), ConfigDescriptor.getInstance().getConfig().getGlobalStatisticsCollectInterval() * 1000);
                        if (System.currentTimeMillis() - lastReshardTime.get() < ConfigDescriptor.getInstance().getConfig().getGlobalStatisticsCollectInterval() * 1000) {
                            return;
                        }
                        storage.lockReshardStatus();
                        // 提议进入重分片流程，返回值为 true 代表提议成功，本节点成为 proposer；为 false 代表提议失败，说明已有其他节点提议成功
                        if (storage.proposeToReshard()) {
                            reshardStatus = JUDGING;
                            isProposer = true;
                            logger.info("iginx node {} propose to reshard", id);
                            // 在重分片判断阶段，proposer 节点不需要推送本地的存储后端统计信息
                        }
                        storage.releaseReshardStatus();
                    } catch (MetaStorageException e) {
                        logger.error("encounter error when proposing to reshard: ", e);
                    }
                },
                ConfigDescriptor.getInstance().getConfig().getGlobalStatisticsCollectInterval(),
                ConfigDescriptor.getInstance().getConfig().getGlobalStatisticsCollectInterval(),
                TimeUnit.SECONDS
            );
        }
    }

    public static DefaultMetaManager getInstance() {
        if (INSTANCE == null) {
            synchronized (DefaultMetaManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DefaultMetaManager();
                }
            }
        }
        return INSTANCE;
    }

    private void initIginx() throws MetaStorageException {
        storage.registerIginxChangeHook((iginxId, iginx) -> {
            if (iginx == null) {
                cache.removeIginx(iginxId);
            } else {
                cache.addIginx(iginx);
            }
        });
        for (IginxMeta iginx : storage.loadIginx().values()) {
            cache.addIginx(iginx);
        }
        IginxMeta iginx = new IginxMeta(0L, ConfigDescriptor.getInstance().getConfig().getIp(),
                ConfigDescriptor.getInstance().getConfig().getPort(), null);
        id = storage.registerIginx(iginx);
        SnowFlakeUtils.init(id);
    }

    private void initStorageEngine() throws MetaStorageException {
        storage.registerStorageChangeHook((iginxId, storageEngine) -> {
            if (storageEngine != null) {
                cache.addStorageEngine(storageEngine);
                for (StorageEngineChangeHook hook : storageEngineChangeHooks) {
                    hook.onChanged(null, storageEngine);
                }
            }
        });
        for (StorageEngineMeta storageEngine : storage.loadStorageEngine(resolveStorageEngineFromConf()).values()) {
            cache.addStorageEngine(storageEngine);
        }
    }

    private void initStorageUnit() throws MetaStorageException {
        storage.registerStorageUnitChangeHook((storageUnitId, storageUnit) -> {
            if (storageUnit == null) {
                return;
            }
            if (storageUnit.getCreatedBy() == DefaultMetaManager.this.id) { // 本地创建的
                return;
            }
            if (storageUnit.isInitialStorageUnit()) { // 初始分片不通过异步事件更新
                return;
            }
            if (!cache.hasStorageUnit()) {
                return;
            }
            if (reshardStatus.equals(EXECUTING)) {
                needToCreateStorageUnits = true;
            }
            StorageUnitMeta originStorageUnitMeta = cache.getStorageUnit(storageUnitId);
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
                cache.getStorageEngine(originStorageUnitMeta.getStorageEngineId()).removeStorageUnit(originStorageUnitMeta.getId());
            } else {
                cache.addStorageUnit(storageUnit);
            }
            cache.getStorageEngine(storageUnit.getStorageEngineId()).addStorageUnit(storageUnit);
            List<FragmentMeta> fragments = cache.getReshardFragmentsByStorageUnitId(storageUnitId);
            if (fragments != null) {
                for (FragmentMeta fragment : fragments) {
                    createFragment(storageUnit, fragment, true);
                }
                cache.removeReshardFragmentsByStorageUnitId(storageUnitId);
            }
            synchronized (terminateResharding) {
                try {
                    if (reshardStatus.equals(EXECUTING) && storageUnit.isLastOfBatch() && !hasCreatedStorageUnits) {
                        hasCreatedStorageUnits = true;
                        if (hasCreatedFragments) {
                            logger.info("iginx node {} increment reshard counter", id);
                            storage.lockReshardCounter();
                            storage.incrementReshardCounter();
                            storage.releaseReshardCounter();
                        }
                    }
                } catch (MetaStorageException e) {
                    logger.error("encounter error when updating reshard counter: ", e);
                }
            }
        });
    }

    private void initFragment() throws MetaStorageException {
        storage.registerFragmentChangeHook((create, fragment) -> {
            if (fragment == null) {
                return;
            }
            if (create) {
                activeStartTime.set(fragment.getTimeInterval().getStartTime());
            }
            if (create && fragment.getCreatedBy() == DefaultMetaManager.this.id) {
                return;
            }
            if (!create && fragment.getUpdatedBy() == DefaultMetaManager.this.id) {
                return;
            }
            if (create && fragment.isInitialFragment()) { // 初始分片的创建不通过异步事件更新
                return;
            }
            if (!cache.hasFragment()) {
                return;
            }
            if (cache.getStorageUnit(fragment.getMasterStorageUnitId()) == null) {
                cache.addReshardFragment(fragment);
                return;
            }
            if (reshardStatus.equals(EXECUTING) && !needToCreateStorageUnits) {
                hasCreatedStorageUnits = true;
            }
            createFragment(cache.getStorageUnit(fragment.getMasterStorageUnitId()), fragment, create);
        });
    }

    private void initSchemaMapping() throws MetaStorageException {
        storage.registerSchemaMappingChangeHook((schema, schemaMapping) -> {
            if (schemaMapping == null || schemaMapping.size() == 0) {
                cache.removeSchemaMapping(schema);
            } else {
                cache.addOrUpdateSchemaMapping(schema, schemaMapping);
            }
        });
        for (Map.Entry<String, Map<String, Integer>> schemaEntry : storage.loadSchemaMapping().entrySet()) {
            cache.addOrUpdateSchemaMapping(schemaEntry.getKey(), schemaEntry.getValue());
        }
    }

    private void initUser() throws MetaStorageException {
        storage.registerUserChangeHook((username, user) -> {
            if (user == null) {
                cache.removeUser(username);
            } else {
                cache.addOrUpdateUser(user);
            }
        });
        for (UserMeta user : storage.loadUser(resolveUserFromConf())) {
            cache.addOrUpdateUser(user);
        }
    }

    private void initMaxActiveEndTimeStatistics() throws MetaStorageException {
        storage.registerMaxActiveEndTimeStatisticsChangeHook((iginxId, endTime) -> {
            if (endTime <= 0L) {
                return;
            }
            if (iginxId == DefaultMetaManager.this.id) {
                return;
            }
            updateMaxActiveEndTime(endTime);
            int updatedCounter = maxActiveEndTimeStatisticsCounter.incrementAndGet();
            if (isProposer) {
                logger.info("iginx node {}(proposer) increment max active end time statistics counter {}", this.id, updatedCounter);
            } else {
                logger.info("iginx node {} increment max active end time statistics counter {}", this.id, updatedCounter);
            }
        });
    }

    private void initMinActiveIginxStatistics() throws MetaStorageException {
        storage.registerMinActiveIginxStatisticsChangeHook(density -> {
            try {
                if (density == 0.0) {
                    // TODO 处理密度为0的情况
                    return;
                }
                Set<String> separators = cache.separateActiveTimeSeriesStatisticsByDensity(0.5 * density / getStorageEngineNum());
                cache.addOrUpdateActiveSeparatorStatistics(separators);
                storage.lockActiveSeparatorStatistics();
                storage.addOrUpdateActiveSeparatorStatistics(id, separators);
                storage.releaseActiveSeparatorStatistics();
                if (isProposer) {
                    logger.info("iginx node {}(proposer) push active separator statistics {}", id, separators);
                } else {
                    logger.info("iginx node {} push active separator statistics {}", id, separators);
                }
            } catch (MetaStorageException e) {
                logger.error("encounter error when adding active separator statistics: ", e);
            }
        });
    }

    private void initActiveSeparatorStatistics() throws MetaStorageException {
        storage.registerActiveSeparatorStatisticsChangeHook((iginxId, separators) -> {
            try {
                if (separators == null) {
                    return;
                }
                if (iginxId == DefaultMetaManager.this.id) {
                    return;
                }
                cache.addOrUpdateActiveSeparatorStatistics(separators);
                int updatedCounter = activeSeparatorStatisticsCounter.incrementAndGet();
                if (isProposer) {
                    logger.info("iginx node {}(proposer) increment active separator statistics counter: {}", id, updatedCounter);
                } else {
                    logger.info("iginx node {} increment active separator statistics counter: {}", id, updatedCounter);
                }
                if (updatedCounter == getIginxList().size() - 1) {
                    Map<TimeSeriesInterval, TimeSeriesIntervalStatistics> statisticsMap = cache.separateActiveTimeSeriesStatisticsBySeparators();
                    cache.addOrUpdateActiveTimeSeriesIntervalStatistics(statisticsMap);
                    if (!isProposer) {
                        storage.lockActiveTimeSeriesIntervalStatistics();
                        storage.addOrUpdateActiveTimeSeriesIntervalStatistics(id, statisticsMap);
                        storage.releaseActiveTimeSeriesIntervalStatistics();
                        logger.info("iginx node {} push active time series interval statistics", id);
                    }
                }
            } catch (MetaStorageException e) {
                logger.error("encounter error when adding or updating active time series interval statistics: ", e);
            }
        });
    }

    private void initActiveStorageEngineStatistics() throws MetaStorageException {
        storage.registerActiveStorageEngineStatisticsChangeHook((iginxId, statisticsMap) -> {
            try {
                if (statisticsMap == null) {
                    return;
                }
                if (isProposer && reshardStatus.equals(JUDGING)) {
                    cache.addOrUpdateActiveStorageEngineStatistics(statisticsMap);
                    cache.addOrUpdateActiveIginxStatistics(iginxId, statisticsMap);
                    int updatedCounter = activeStorageEngineStatisticsCounter.incrementAndGet();
                    logger.info("iginx node {}(proposer) pull active storage engine statistics {} from iginx node {}", id, statisticsMap, iginxId);
                    logger.info("iginx node {}(proposer) increment active storage engine statistics counter: {}", id, updatedCounter);
                    synchronized (judgedResharding) {
                        if (updatedCounter == getIginxList().size() - 1 && !hasJudgedResharding) {
                            hasJudgedResharding = true;
                            if (needToReshard()) {
                                storage.lockReshardStatus();
                                storage.updateReshardStatus(EXECUTING);
                                storage.releaseReshardStatus();
                                logger.info("iginx node {}(proposer) decide to enter resharding executing phase", id);
                            } else {
                                storage.lockReshardStatus();
                                storage.updateReshardStatus(NON_RESHARDING);
                                storage.releaseReshardStatus();
                                logger.info("iginx node {}(proposer) decide to quit resharding", id);
                            }
                        }
                    }
                }
            } catch (MetaStorageException e) {
                logger.error("encounter error when updating reshard status: ", e);
            }
        });
    }

    private void initActiveTimeSeriesIntervalStatistics() {
        storage.registerActiveTimeSeriesIntervalStatisticsChangeHook(statisticsMap -> {
            if (statisticsMap == null) {
                return;
            }
            if (isProposer) {
                cache.addOrUpdateActiveTimeSeriesIntervalStatistics(statisticsMap);
                int updatedCounter = activeTimeSeriesIntervalStatisticsCounter.incrementAndGet();
                logger.info("iginx node {}(proposer) increment active time series interval statistics counter: {}", id, updatedCounter);
                synchronized (commitCreatingTask) {
                    if (maxActiveEndTimeStatisticsCounter.get() == getIginxList().size() - 1
                            && updatedCounter == getIginxList().size() - 1
                            && !hasCommittedCreatingTask) {
                        hasCommittedCreatingTask = true;
                        IFragmentGenerator fragmentGenerator = PolicyManager.getInstance()
                                .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName()).getIFragmentGenerator();
                        Pair<List<FragmentMeta>, List<StorageUnitMeta>> fragmentsAndStorageUnits = fragmentGenerator.generateFragmentsAndStorageUnitsForResharding(maxActiveEndTime.get(), cache.getActiveTimeSeriesIntervalStatistics());
                        logger.info("iginx node {}(proposer) push fragments: {}", id, fragmentsAndStorageUnits.k);
                        logger.info("iginx node {}(proposer) push storage units: {}", id, fragmentsAndStorageUnits.v);
                        createFragmentsAndStorageUnits(fragmentsAndStorageUnits.v, fragmentsAndStorageUnits.k);
                    }
                }
            }
        });
    }

    private void initReshardStatus() throws MetaStorageException {
        storage.registerReshardStatusHook(status -> {
            try {
                reshardStatus = status;
                if (!isProposer && reshardStatus.equals(JUDGING)) {
                    // 在重分片判断阶段，accepter 节点推送本地的存储后端统计信息
                    storage.lockActiveStorageEngineStatistics();
                    storage.addOrUpdateActiveStorageEngineStatistics(id, cache.getActiveStorageEngineStatistics());
                    storage.releaseActiveStorageEngineStatistics();
//                    hasPushedStatistics = true;
                    logger.info("iginx node {} start to reshard", id);
                    logger.info("iginx node {} push active storage engine statistics", id);
                }
                if (reshardStatus.equals(EXECUTING)) {
                    storage.lockMaxActiveEndTimeStatistics();
                    storage.addOrUpdateMaxActiveEndTimeStatistics(id, maxActiveEndTime.get());
                    storage.releaseMaxActiveEndTimeStatistics();
                    if (isProposer) {
                        logger.info("iginx node {}(proposer) push max active end time statistics {}", id, maxActiveEndTime.get());
                        storage.lockMinActiveIginxStatistics();
                        storage.addOrUpdateMinActiveIginxStatistics(cache.getMinActiveIginxStatistics());
                        storage.releaseMinActiveIginxStatistics();
                        logger.info("iginx node {}(proposer) push min active iginx statistics {}", id, cache.getMinActiveIginxStatistics());
                    } else {
                        logger.info("iginx node {} push max active end time statistics {}", id, maxActiveEndTime.get());
                    }
                }
                if (reshardStatus.equals(NON_RESHARDING)) {
                    if (isProposer) {
                        logger.info("iginx node {}(proposer) finish to reshard", id);
                    } else {
                        logger.info("iginx node {} finish to reshard", id);
                    }

                    isProposer = false;
                    hasPushedStatistics = false;
                    hasJudgedResharding = false;
                    hasCommittedCreatingTask = false;
                    hasCreatedFragments = false;
                    hasCreatedStorageUnits = false;
                    needToCreateStorageUnits = false;

                    lastReshardTime.set(System.currentTimeMillis());
                    maxActiveEndTimeStatisticsCounter.set(0);
                    activeSeparatorStatisticsCounter.set(0);
                    activeStorageEngineStatisticsCounter.set(0);
                    activeTimeSeriesIntervalStatisticsCounter.set(0);

                    releaseWaitingReshardThreads();
                }
            } catch (MetaStorageException e) {
                logger.error("encounter error when switching reshard status: ", e);
            }
        });
        storage.lockReshardStatus();
        storage.removeReshardStatus();
        storage.releaseReshardStatus();
    }

    private void initReshardCounter() throws MetaStorageException {
        storage.registerReshardCounterChangeHook(counter -> {
            try {
                if (counter <= 0) {
                    return;
                }
                if (isProposer && counter == getIginxList().size() - 1) {
                    storage.lockReshardCounter();
                    storage.resetReshardCounter();
                    storage.releaseReshardCounter();

                    storage.lockReshardStatus();
                    storage.updateReshardStatus(NON_RESHARDING);
                    storage.releaseReshardStatus();
                }
            } catch (MetaStorageException e) {
                logger.error("encounter error when updating reshard counter: ", e);
            }
        });
        storage.lockReshardCounter();
        storage.removeReshardCounter();
        storage.releaseReshardCounter();
    }

    @Override
    public boolean addStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
        try {
            for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
                storageEngineMeta.setId(storage.addStorageEngine(storageEngineMeta));
                cache.addStorageEngine(storageEngineMeta);
            }
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when adding storage engines:", e);
        }
        return false;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return new ArrayList<>(cache.getStorageEngineList());
    }

    @Override
    public int getStorageEngineNum() {
        return cache.getStorageEngineList().size();
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
    public boolean createFragmentsAndStorageUnits(List<StorageUnitMeta> storageUnits, List<FragmentMeta> fragments) {
        try {
            storage.lockFragment();
            storage.lockStorageUnit();

            Map<String, StorageUnitMeta> fakeIdToStorageUnit = new HashMap<>(); // 假名翻译工具

            for (StorageUnitMeta masterStorageUnit : storageUnits) {
                masterStorageUnit.setCreatedBy(id);
                String fakeName = masterStorageUnit.getId();
                String actualName = storage.addStorageUnit();
                StorageUnitMeta actualMasterStorageUnit = masterStorageUnit.renameStorageUnitMeta(actualName, actualName, false);
                cache.updateStorageUnit(actualMasterStorageUnit);
                storage.updateStorageUnit(actualMasterStorageUnit);
                fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
                for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
                    slaveStorageUnit.setCreatedBy(id);
                    String slaveFakeName = slaveStorageUnit.getId();
                    String slaveActualName = storage.addStorageUnit();
                    StorageUnitMeta actualSlaveStorageUnit = slaveStorageUnit.renameStorageUnitMeta(slaveActualName, actualName, false);
                    actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
                    cache.updateStorageUnit(actualSlaveStorageUnit);
                    storage.updateStorageUnit(actualSlaveStorageUnit);
                    fakeIdToStorageUnit.put(slaveFakeName, actualSlaveStorageUnit);
                }
            }
            if (storageUnits.isEmpty()) {
                hasCreatedStorageUnits = true;
            }

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
                fragmentMeta.setInitialFragment(false);
                StorageUnitMeta storageUnit;
                if (storageUnits.isEmpty()) {
                    storageUnit = cache.getStorageUnit(fragmentMeta.getMasterStorageUnitId());
                } else {
                    storageUnit = fakeIdToStorageUnit.get(fragmentMeta.getFakeStorageUnitId());
                }
                if (storageUnit.isMaster()) {
                    fragmentMeta.setMasterStorageUnit(storageUnit);
                } else {
                    fragmentMeta.setMasterStorageUnit(getStorageUnit(storageUnit.getMasterId()));
                }
                fragmentMeta.setMasterStorageUnitId(storageUnit.getMasterId());
                cache.addFragment(fragmentMeta);
                storage.addFragment(fragmentMeta);
            }
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when creating fragment: ", e);
        } finally {
            try {
                storage.releaseFragment();
                storage.releaseStorageUnit();
            } catch (MetaStorageException e) {
                logger.error("encounter error when releasing fragment lock: ", e);
            }
        }
        return false;
    }

    @Override
    public boolean hasFragment() {
        return cache.hasFragment();
    }

    @Override
    public boolean createInitialFragmentsAndStorageUnits(List<StorageUnitMeta> storageUnits, List<FragmentMeta> initialFragments) { // 必须同时初始化 fragment 和 cache，并且这个方法的主体部分在任意时刻只能由某个 iginx 的某个线程执行
        if (cache.hasFragment() && cache.hasStorageUnit()) {
            return false;
        }
        try {
            storage.lockFragment();
            storage.lockStorageUnit();

            // 接下来的部分只有一个 iginx 的一个线程执行
            if (cache.hasFragment() && cache.hasStorageUnit()) {
                return false;
            }
            // 查看一下服务器上是不是已经有了
            Map<String, StorageUnitMeta> globalStorageUnits = storage.loadStorageUnit();
            if (globalStorageUnits != null && !globalStorageUnits.isEmpty()) { // 服务器上已经有人创建过了，本地只需要加载
                Map<TimeSeriesInterval, List<FragmentMeta>> globalFragmentMap = storage.loadFragment();
                cache.initStorageUnit(globalStorageUnits);
                cache.initFragment(globalFragmentMap);
                return false;
            }

            // 确实没有人创建过，以我为准
            Map<String, StorageUnitMeta> fakeIdToStorageUnit = new HashMap<>(); // 假名翻译工具
            for (StorageUnitMeta masterStorageUnit : storageUnits) {
                masterStorageUnit.setCreatedBy(id);
                String fakeName = masterStorageUnit.getId();
                String actualName = storage.addStorageUnit();
                StorageUnitMeta actualMasterStorageUnit = masterStorageUnit.renameStorageUnitMeta(actualName, actualName, true);
                storage.updateStorageUnit(actualMasterStorageUnit);
                fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
                for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
                    slaveStorageUnit.setCreatedBy(id);
                    String slaveFakeName = slaveStorageUnit.getId();
                    String slaveActualName = storage.addStorageUnit();
                    StorageUnitMeta actualSlaveStorageUnit = slaveStorageUnit.renameStorageUnitMeta(slaveActualName, actualName, true);
                    actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
                    storage.updateStorageUnit(actualSlaveStorageUnit);
                    fakeIdToStorageUnit.put(slaveFakeName, actualSlaveStorageUnit);
                }
            }
            initialFragments.sort(Comparator.comparingLong(o -> o.getTimeInterval().getStartTime()));
            for (FragmentMeta fragmentMeta : initialFragments) {
                fragmentMeta.setCreatedBy(id);
                StorageUnitMeta storageUnit = fakeIdToStorageUnit.get(fragmentMeta.getFakeStorageUnitId());
                if (storageUnit.isMaster()) {
                    fragmentMeta.setMasterStorageUnit(storageUnit);
                } else {
                    fragmentMeta.setMasterStorageUnit(getStorageUnit(storageUnit.getMasterId()));
                }
                fragmentMeta.setMasterStorageUnitId(storageUnit.getMasterId());
                storage.addFragment(fragmentMeta);
            }
            cache.initStorageUnit(storage.loadStorageUnit());
            cache.initFragment(storage.loadFragment());
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when initiating fragment: ", e);
        } finally {
            try {
                storage.releaseStorageUnit();
                storage.releaseFragment();
            } catch (MetaStorageException e) {
                logger.error("encounter error when releasing fragment lock: ", e);
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
            logger.error("encounter error when updating schema mapping: ", e);
        }
    }

    @Override
    public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {
        Map<String, Integer> schemaMapping = cache.getSchemaMapping(schema);
        if (schemaMapping == null) {
            schemaMapping = new HashMap<>();
        }
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
            logger.error("encounter error when updating schema mapping: ", e);
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

    @Override
    public void updateActiveTimeSeriesStatistics(Map<String, TimeSeriesStatistics> timeSeriesStatisticsMap) {
        cache.addOrUpdateActiveTimeSeriesStatistics(timeSeriesStatisticsMap);
    }

    @Override
    public Map<String, TimeSeriesStatistics> getActiveTimeSeriesStatistics() {
        return cache.getActiveTimeSeriesStatistics();
    }

    private List<StorageEngineMeta> resolveStorageEngineFromConf() {
        List<StorageEngineMeta> storageEngineMetaList = new ArrayList<>();
        String[] storageEngineStrings = ConfigDescriptor.getInstance().getConfig().getStorageEngineList().split(",");
        for (int i = 0; i < storageEngineStrings.length; i++) {
            if (storageEngineStrings[i].length() == 0) {
                continue;
            }
            String[] storageEngineParts = storageEngineStrings[i].split("#");
            String ip = storageEngineParts[0];
            int port = Integer.parseInt(storageEngineParts[1]);
            String storageEngine = storageEngineParts[2];
            Map<String, String> extraParams = new HashMap<>();
            String[] KAndV;
            for (int j = 3; j < storageEngineParts.length; j++) {
                if (storageEngineParts[j].contains("\"")) {
                    KAndV = storageEngineParts[j].split("\"");
                    extraParams.put(KAndV[0].substring(0, KAndV[0].length() - 1), KAndV[1]);
                } else {
                    KAndV = storageEngineParts[j].split("=");
                    if (KAndV.length != 2) {
                        logger.error("unexpected storage engine meta info: " + storageEngineStrings[i]);
                        continue;
                    }
                    extraParams.put(KAndV[0], KAndV[1]);
                }
            }
            storageEngineMetaList.add(new StorageEngineMeta(i, ip, port, extraParams, storageEngine, id));
        }
        return storageEngineMetaList;
    }

    private boolean needToReshard() {
        logger.info("cache.getActiveStorageEngineStatistics() = {}", cache.getActiveStorageEngineStatistics());
        double totalDensity = 0.0;
        for (StorageEngineStatistics statistics : cache.getActiveStorageEngineStatistics().values()) {
            totalDensity += statistics.getDensity();
        }
        for (StorageEngineStatistics statistics : cache.getActiveStorageEngineStatistics().values()) {
            if (totalDensity != 0.0 && statistics.getDensity() / (totalDensity / cache.getActiveStorageEngineStatistics().size()) >= 1) {
                return true;
            }
        }
        return false;
    }

    private UserMeta resolveUserFromConf() {
        String username = ConfigDescriptor.getInstance().getConfig().getUsername();
        String password = ConfigDescriptor.getInstance().getConfig().getPassword();
        UserType userType = UserType.Administrator;
        Set<AuthType> auths = new HashSet<>();
        auths.add(AuthType.Read);
        auths.add(AuthType.Write);
        auths.add(AuthType.Admin);
        auths.add(AuthType.Cluster);
        return new UserMeta(username, password, userType, auths);
    }

    @Override
    public boolean addUser(UserMeta user) {
        try {
            storage.addUser(user);
            cache.addOrUpdateUser(user);
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when adding user: ", e);
            return false;
        }
    }

    @Override
    public boolean updateUser(String username, String password, Set<AuthType> auths) {
        List<UserMeta> users = cache.getUser(Collections.singletonList(username));
        if (users.size() == 0) { // 待更新的用户不存在
            return false;
        }
        UserMeta user = users.get(0);
        if (password != null) {
            user.setPassword(password);
        }
        if (auths != null) {
            user.setAuths(auths);
        }
        try {
            storage.updateUser(user);
            cache.addOrUpdateUser(user);
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when updating user: ", e);
            return false;
        }
    }

    @Override
    public boolean removeUser(String username) {
        try {
            storage.removeUser(username);
            cache.removeUser(username);
            return true;
        } catch (MetaStorageException e) {
            logger.error("encounter error when removing user: ", e);
            return false;
        }
    }

    @Override
    public UserMeta getUser(String username) {
        List<UserMeta> users = cache.getUser(Collections.singletonList(username));
        if (users.size() == 0) {
            return null;
        }
        return users.get(0);
    }

    @Override
    public List<UserMeta> getUsers() {
        return cache.getUser();
    }

    @Override
    public List<UserMeta> getUsers(List<String> username) {
        return cache.getUser(username);
    }

    public boolean needToParkThreads() {
        return reshardStatus.equals(EXECUTING);
    }

    public void updateMaxActiveEndTime(long endTime) {
        maxActiveEndTime.getAndUpdate(e -> Math.max(e, endTime + ConfigDescriptor.getInstance().getConfig().getReshardFragmentTimeMargin() * 1000));
    }

    public long getMaxActiveEndTime() {
        return maxActiveEndTime.get();
    }

    public long getActiveStartTime() {
        return activeStartTime.get();
    }

    private void createFragment(StorageUnitMeta storageUnit, FragmentMeta fragment, boolean create) {
        fragment.setMasterStorageUnit(storageUnit);
        if (create) {
            cache.addFragment(fragment);
        } else {
            cache.updateFragment(fragment);
        }
        synchronized (terminateResharding) {
            try {
                if (reshardStatus.equals(EXECUTING) && fragment.isLastOfBatch() && !hasCreatedFragments) {
                    hasCreatedFragments = true;
                    if (hasCreatedStorageUnits) {
                        logger.info("iginx node {} increment reshard counter", id);
                        storage.lockReshardCounter();
                        storage.incrementReshardCounter();
                        storage.releaseReshardCounter();
                    }
                }
            } catch (MetaStorageException e) {
                logger.error("encounter error when updating reshard counter: ", e);
            }
        }
    }

    public void addWaitingReshardThread(Thread thread) {
        waitingReshardThreadsQueue.offer(thread);
    }

    private void releaseWaitingReshardThreads() {
        Thread thread;
        while ((thread = waitingReshardThreadsQueue.poll()) != null) {
            logger.info("thread {} is unparked", thread.getId());
            LockSupport.unpark(thread);
        }
    }
}
