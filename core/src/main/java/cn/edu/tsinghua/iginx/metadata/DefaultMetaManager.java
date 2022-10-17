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

import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.EXECUTING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.JUDGING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.NON_RESHARDING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.RECOVER;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngine;
import cn.edu.tsinghua.iginx.engine.physical.PhysicalEngineImpl;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.operator.Delete;
import cn.edu.tsinghua.iginx.engine.shared.source.FragmentSource;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.cache.DefaultMetaCache;
import cn.edu.tsinghua.iginx.metadata.cache.IMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.metadata.hook.StorageUnitHook;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.etcd.ETCDMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.file.FileMetaStorage;
import cn.edu.tsinghua.iginx.metadata.storage.zk.ZooKeeperMetaStorage;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteTask;
import cn.edu.tsinghua.iginx.migration.recover.MigrationExecuteType;
import cn.edu.tsinghua.iginx.migration.recover.MigrationLoggerAnalyzer;
import cn.edu.tsinghua.iginx.monitor.HotSpotMonitor;
import cn.edu.tsinghua.iginx.monitor.RequestsMonitor;
import cn.edu.tsinghua.iginx.policy.simple.TimeSeriesCalDO;
import cn.edu.tsinghua.iginx.sql.statement.InsertStatement;
import cn.edu.tsinghua.iginx.thrift.AuthType;
import cn.edu.tsinghua.iginx.thrift.UserType;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetaManager implements IMetaManager {

  private static final Logger logger = LoggerFactory.getLogger(DefaultMetaManager.class);
  private static DefaultMetaManager INSTANCE;
  private final IMetaCache cache;
  private final static PhysicalEngine physicalEngine = PhysicalEngineImpl.getInstance();

  private final IMetaStorage storage;
  private final List<StorageEngineChangeHook> storageEngineChangeHooks;
  private final List<StorageUnitHook> storageUnitHooks;
  private long id;

  // 当前活跃的最大的结束时间
  private AtomicLong maxActiveEndTime = new AtomicLong(-1L);
  private AtomicInteger maxActiveEndTimeStatisticsCounter = new AtomicInteger(0);

  // 重分片状态
  private ReshardStatus reshardStatus = NON_RESHARDING;

  // 在重分片过程中，是否为提出者
  private boolean isProposer = false;

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
    storageUnitHooks = Collections.synchronizedList(new ArrayList<>());

    try {
      initIginx();
      initStorageEngine();
      initStorageUnit();
      initFragment();
      initSchemaMapping();
      initPolicy();
      initUser();
      initMaxActiveEndTimeStatistics();
      initReshardStatus();
      initReshardCounter();
      recover();
    } catch (MetaStorageException e) {
      logger.error("init meta manager error: ", e);
      System.exit(-1);
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
    storage.registerIginxChangeHook((id, iginx) -> {
      if (iginx == null) {
        cache.removeIginx(id);
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
    storage.registerStorageChangeHook((id, storageEngine) -> {
      if (storageEngine != null) {
        cache.addStorageEngine(storageEngine);
        for (StorageEngineChangeHook hook : storageEngineChangeHooks) {
          hook.onChanged(null, storageEngine);
        }
      }
    });
    for (StorageEngineMeta storageEngine : storage.loadStorageEngine(resolveStorageEngineFromConf())
        .values()) {
      cache.addStorageEngine(storageEngine);
    }

  }

  private void initStorageUnit() throws MetaStorageException {
    storage.registerStorageUnitChangeHook((id, storageUnit) -> {
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
      StorageUnitMeta originStorageUnitMeta = cache.getStorageUnit(id);
      if (originStorageUnitMeta == null) {
        if (!storageUnit.isMaster()) { // 需要加入到主节点的子节点列表中
          StorageUnitMeta masterStorageUnitMeta = cache.getStorageUnit(storageUnit.getMasterId());
          if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
            logger.error("unexpected storage unit " + storageUnit.toString()
                + ", because it does not has a master storage unit");
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
            logger.error("unexpected storage unit " + storageUnit.toString()
                + ", because it does not has a master storage unit");
          } else {
            masterStorageUnitMeta.removeReplica(originStorageUnitMeta);
            masterStorageUnitMeta.addReplica(storageUnit);
          }
        }
      }
      if (originStorageUnitMeta != null) {
        cache.updateStorageUnit(storageUnit);
        cache.getStorageEngine(storageUnit.getStorageEngineId())
            .removeStorageUnit(originStorageUnitMeta.getId());
      } else {
        cache.addStorageUnit(storageUnit);
      }
      cache.getStorageEngine(storageUnit.getStorageEngineId()).addStorageUnit(storageUnit);
      for (StorageUnitHook storageUnitHook : storageUnitHooks) {
        storageUnitHook.onChange(originStorageUnitMeta, storageUnit);
      }
    });
  }

  private void initFragment() throws MetaStorageException {
    storage.registerFragmentChangeHook((create, fragment) -> {
      if (fragment == null) {
        return;
      }
      if (create && fragment.getCreatedBy() == DefaultMetaManager.this.id) {
        return;
      }
      if (!create && fragment.getUpdatedBy() == DefaultMetaManager.this.id) {
        return;
      }
      if (fragment.isInitialFragment()) { // 初始分片不通过异步事件更新
        return;
      }
      if (!cache.hasFragment()) {
        return;
      }
      fragment.setMasterStorageUnit(cache.getStorageUnit(fragment.getMasterStorageUnitId()));
      if (create) {
        cache.addFragment(fragment);
      } else {
        cache.updateFragment(fragment);
      }
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
    for (Map.Entry<String, Map<String, Integer>> schemaEntry : storage.loadSchemaMapping()
        .entrySet()) {
      cache.addOrUpdateSchemaMapping(schemaEntry.getKey(), schemaEntry.getValue());
    }
  }

  private void initPolicy() {
    storage.registerTimeseriesChangeHook(cache::timeSeriesIsUpdated);
    storage.registerVersionChangeHook((version, num) -> {
      double sum = cache.getSumFromTimeSeries();
      Map<String, Double> timeseriesData = cache.getMaxValueFromTimeSeries().stream().
          collect(Collectors.toMap(TimeSeriesCalDO::getTimeSeries, TimeSeriesCalDO::getValue));
      double countSum = timeseriesData.values().stream().mapToDouble(Double::doubleValue).sum();
      if (countSum > 1e-9) {
        timeseriesData.forEach((k, v) -> timeseriesData.put(k, v / countSum * sum));
      }
      try {
        storage.updateTimeseriesData(timeseriesData, getIginxId(), version);
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
    int num = 0;
    try {
      storage.registerPolicy(getIginxId(), num);
      // 从元数据管理器取写入的最大时间戳
      maxActiveEndTime.set(storage.getMaxActiveEndTimeStatistics());
    } catch (Exception e) {
      e.printStackTrace();
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

  @Override
  public boolean scaleInStorageEngines(List<StorageEngineMeta> storageEngineMetas) {
    try {
      for (StorageEngineMeta storageEngineMeta : storageEngineMetas) {
        storage.removeStorageEngine(storageEngineMeta);
        cache.removeStorageEngine(storageEngineMeta);
      }
      return true;
    } catch (MetaStorageException e) {
      logger.error("add storage engines error:", e);
    }
    return false;
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
      logger.error("add storage engines error:", e);
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
  public List<StorageUnitMeta> getStorageUnits() {
    return cache.getStorageUnits();
  }

  @Override
  public List<IginxMeta> getIginxList() {
    return new ArrayList<>(cache.getIginxList());
  }

  @Override
  public long getIginxId() {
    return id;
  }

  public boolean checkFragmentExistenceByTimeInterval(TimeSeriesInterval tsInterval) {
    return !cache.getFragmentMapByExactTimeSeriesInterval(tsInterval).isEmpty();
  }

  @Override
  public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesInterval(
      TimeSeriesInterval tsInterval) {
    return cache.getFragmentMapByTimeSeriesInterval(tsInterval);
  }

  @Override
  public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMapByTimeSeriesInterval(
      TimeSeriesInterval tsInterval) {
    return cache.getLatestFragmentMapByTimeSeriesInterval(tsInterval);
  }

  @Override
  public Map<TimeSeriesInterval, FragmentMeta> getLatestFragmentMap() {
    return cache.getLatestFragmentMap();
  }

  @Override
  public Map<TimeSeriesInterval, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(
      TimeSeriesInterval tsInterval, TimeInterval timeInterval) {
    return cache.getFragmentMapByTimeSeriesIntervalAndTimeInterval(tsInterval, timeInterval);
  }

  @Override
  public List<FragmentMeta> getAllFragments() {
    return cache.getAllFragments();
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
  public List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName,
      TimeInterval timeInterval) {
    return cache.getFragmentListByTimeSeriesNameAndTimeInterval(tsName, timeInterval);
  }

  @Override
  public boolean createFragmentsAndStorageUnits(List<StorageUnitMeta> storageUnits,
      List<FragmentMeta> fragments) {
    try {
      storage.lockFragment();
      storage.lockStorageUnit();

      Map<String, StorageUnitMeta> fakeIdToStorageUnit = new HashMap<>(); // 假名翻译工具
      for (StorageUnitMeta masterStorageUnit : storageUnits) {
        masterStorageUnit.setCreatedBy(id);
        String fakeName = masterStorageUnit.getId();
        String actualName = storage.addStorageUnit();
        StorageUnitMeta actualMasterStorageUnit = masterStorageUnit
            .renameStorageUnitMeta(actualName, actualName);
        cache.updateStorageUnit(actualMasterStorageUnit);
        for (StorageUnitHook hook : storageUnitHooks) {
          hook.onChange(null, actualMasterStorageUnit);
        }
        storage.updateStorageUnit(actualMasterStorageUnit);
        fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
        for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
          slaveStorageUnit.setCreatedBy(id);
          String slaveFakeName = slaveStorageUnit.getId();
          String slaveActualName = storage.addStorageUnit();
          StorageUnitMeta actualSlaveStorageUnit = slaveStorageUnit
              .renameStorageUnitMeta(slaveActualName, actualName);
          actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
          for (StorageUnitHook hook : storageUnitHooks) {
            hook.onChange(null, actualSlaveStorageUnit);
          }
          cache.updateStorageUnit(actualSlaveStorageUnit);
          storage.updateStorageUnit(actualSlaveStorageUnit);
          fakeIdToStorageUnit.put(slaveFakeName, actualSlaveStorageUnit);
        }
      }

      Map<TimeSeriesInterval, FragmentMeta> latestFragments = getLatestFragmentMap();
      for (FragmentMeta originalFragmentMeta : latestFragments.values()) {
        FragmentMeta fragmentMeta = originalFragmentMeta
            .endFragmentMeta(fragments.get(0).getTimeInterval().getStartTime());
        // 在更新分片时，先更新本地
        fragmentMeta.setUpdatedBy(id);
        cache.updateFragment(fragmentMeta);
        storage.updateFragment(fragmentMeta);
      }

      for (FragmentMeta fragmentMeta : fragments) {
        fragmentMeta.setCreatedBy(id);
        fragmentMeta.setInitialFragment(false);
        StorageUnitMeta storageUnit = fakeIdToStorageUnit.get(fragmentMeta.getFakeStorageUnitId());
        if (storageUnit.isMaster()) {
          fragmentMeta.setMasterStorageUnit(storageUnit);
        } else {
          fragmentMeta.setMasterStorageUnit(getStorageUnit(storageUnit.getMasterId()));
        }
        cache.addFragment(fragmentMeta);
        storage.addFragment(fragmentMeta);
      }
      return true;
    } catch (MetaStorageException e) {
      logger.error("create fragment error: ", e);
    } finally {
      try {
        storage.releaseFragment();
        storage.releaseStorageUnit();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }
    return false;
  }

  @Override
  public boolean createFragmentAndStorageUnit(StorageUnitMeta storageUnit, FragmentMeta fragment) {
    List<StorageUnitMeta> storageUnitMetas = new ArrayList<>();
    storageUnitMetas.add(storageUnit);
    List<FragmentMeta> fragmentMetas = new ArrayList<>();
    fragmentMetas.add(fragment);
    return createFragmentsAndStorageUnits(storageUnitMetas, fragmentMetas);
  }

  @Override
  public FragmentMeta splitFragmentAndStorageUnit(StorageUnitMeta toAddStorageUnit,
      FragmentMeta toAddFragment, FragmentMeta fragment) {
    try {
      storage.lockFragment();
      storage.lockStorageUnit();

      // 更新du
      logger.info("update du");
      toAddStorageUnit.setCreatedBy(id);
      String actualName = storage.addStorageUnit();
      StorageUnitMeta actualMasterStorageUnit = toAddStorageUnit
          .renameStorageUnitMeta(actualName, actualName);
      cache.updateStorageUnit(actualMasterStorageUnit);
      for (StorageUnitHook hook : storageUnitHooks) {
        hook.onChange(null, actualMasterStorageUnit);
      }
      storage.updateStorageUnit(actualMasterStorageUnit);
      for (StorageUnitMeta slaveStorageUnit : toAddStorageUnit.getReplicas()) {
        slaveStorageUnit.setCreatedBy(id);
        String slaveActualName = storage.addStorageUnit();
        StorageUnitMeta actualSlaveStorageUnit = slaveStorageUnit
            .renameStorageUnitMeta(slaveActualName, actualName);
        actualMasterStorageUnit.addReplica(actualSlaveStorageUnit);
        for (StorageUnitHook hook : storageUnitHooks) {
          hook.onChange(null, actualSlaveStorageUnit);
        }
        cache.updateStorageUnit(actualSlaveStorageUnit);
        storage.updateStorageUnit(actualSlaveStorageUnit);
      }

      // 结束旧分片
      cache.deleteFragmentByTsInterval(fragment.getTsInterval(), fragment);
      fragment = fragment
          .endFragmentMeta(toAddFragment.getTimeInterval().getStartTime());
      cache.addFragment(fragment);
      fragment.setUpdatedBy(id);
      storage.updateFragment(fragment);

      // 更新新分片
      toAddFragment.setCreatedBy(id);
      toAddFragment.setInitialFragment(false);
      if (toAddStorageUnit.isMaster()) {
        toAddFragment.setMasterStorageUnit(actualMasterStorageUnit);
      } else {
        toAddFragment.setMasterStorageUnit(getStorageUnit(actualMasterStorageUnit.getMasterId()));
      }
      cache.addFragment(toAddFragment);
      storage.addFragment(toAddFragment);
    } catch (MetaStorageException e) {
      logger.error("create fragment error: ", e);
    } finally {
      try {
        storage.releaseFragment();
        storage.releaseStorageUnit();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }

    return fragment;
  }

  @Override
  public void addFragment(FragmentMeta fragmentMeta) {
    try {
      storage.lockFragment();
      cache.addFragment(fragmentMeta);
      storage.addFragment(fragmentMeta);
    } catch (MetaStorageException e) {
      logger.error("add fragment error: ", e);
    } finally {
      try {
        storage.releaseFragment();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }
  }

  @Override
  public void endFragmentByTimeSeriesInterval(FragmentMeta fragmentMeta, String endTimeSeries) {
    try {
      storage.lockFragment();
      TimeSeriesInterval sourceTsInterval = new TimeSeriesInterval(
          fragmentMeta.getTsInterval().getStartTimeSeries(),
          fragmentMeta.getTsInterval().getEndTimeSeries());
      cache.deleteFragmentByTsInterval(fragmentMeta.getTsInterval(), fragmentMeta);
      fragmentMeta.getTsInterval().setEndTimeSeries(endTimeSeries);
      cache.addFragment(fragmentMeta);
      storage.updateFragmentByTsInterval(sourceTsInterval, fragmentMeta);
    } catch (MetaStorageException e) {
      logger.error("end fragment by time series interval error: ", e);
    } finally {
      try {
        storage.releaseFragment();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }
  }

  @Override
  public void updateFragmentByTsInterval(TimeSeriesInterval tsInterval, FragmentMeta fragmentMeta) {
    try {
      storage.lockFragment();
      cache.updateFragmentByTsInterval(tsInterval, fragmentMeta);
      storage.updateFragmentByTsInterval(tsInterval, fragmentMeta);
    } catch (Exception e) {
      logger.error("update fragment error: ", e);
    } finally {
      try {
        storage.releaseFragment();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }
  }

  @Override
  public void deleteFragmentPoints(TimeSeriesInterval tsInterval, TimeInterval timeInterval) {
    try {
      storage.lockFragment();
      storage.deleteFragmentPoints(tsInterval, timeInterval);
    } catch (Exception e) {
      logger.error("delete fragment error: ", e);
    } finally {
      try {
        storage.releaseFragment();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }
  }

  @Override
  public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) {
    try {
      storage.lockFragment();
      storage.updateFragmentPoints(fragmentMeta, points);
    } catch (Exception e) {
      logger.error("update fragment error: ", e);
    } finally {
      try {
        storage.releaseFragment();
      } catch (MetaStorageException e) {
        logger.error("release fragment lock error: ", e);
      }
    }
  }

  @Override
  public boolean hasFragment() {
    return cache.hasFragment();
  }

  @Override
  public boolean createInitialFragmentsAndStorageUnits(List<StorageUnitMeta> storageUnits,
      List<FragmentMeta> initialFragments) { // 必须同时初始化 fragment 和 cache，并且这个方法的主体部分在任意时刻只能由某个 iginx 的某个线程执行
    if (cache.hasFragment() && cache.hasStorageUnit()) {
      return false;
    }
    List<StorageUnitMeta> newStorageUnits = new ArrayList<>();
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
        newStorageUnits.addAll(globalStorageUnits.values());
        newStorageUnits.sort(Comparator.comparing(StorageUnitMeta::getId));
        logger.warn("server has created storage unit, just need to load.");
        logger.warn("notify storage unit listeners.");
        for (StorageUnitHook hook : storageUnitHooks) {
          for (StorageUnitMeta meta : newStorageUnits) {
            hook.onChange(null, meta);
          }
        }
        logger.warn("notify storage unit listeners finished.");
        // 再初始化缓存
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
        StorageUnitMeta actualMasterStorageUnit = masterStorageUnit
            .renameStorageUnitMeta(actualName, actualName);
        storage.updateStorageUnit(actualMasterStorageUnit);
        fakeIdToStorageUnit.put(fakeName, actualMasterStorageUnit);
        for (StorageUnitMeta slaveStorageUnit : masterStorageUnit.getReplicas()) {
          slaveStorageUnit.setCreatedBy(id);
          String slaveFakeName = slaveStorageUnit.getId();
          String slaveActualName = storage.addStorageUnit();
          StorageUnitMeta actualSlaveStorageUnit = slaveStorageUnit
              .renameStorageUnitMeta(slaveActualName, actualName);
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
        storage.addFragment(fragmentMeta);
      }
      Map<String, StorageUnitMeta> loadedStorageUnits = storage.loadStorageUnit();
      newStorageUnits.addAll(loadedStorageUnits.values());
      newStorageUnits.sort(Comparator.comparing(StorageUnitMeta::getId));
      // 先通知
      logger.warn("i have created storage unit.");
      logger.warn("notify storage unit listeners.");
      for (StorageUnitHook hook : storageUnitHooks) {
        for (StorageUnitMeta meta : newStorageUnits) {
          hook.onChange(null, meta);
        }
      }
      logger.warn("notify storage unit listeners finished.");
      // 再初始化缓存
      cache.initStorageUnit(loadedStorageUnits);
      cache.initFragment(storage.loadFragment());
      return true;
    } catch (MetaStorageException e) {
      logger.error("encounter error when init fragment: ", e);
    } finally {
      try {
        storage.releaseStorageUnit();
        storage.releaseFragment();
      } catch (MetaStorageException e) {
        logger.error("encounter error when release fragment lock: ", e);
      }
    }
    return false;
  }

  @Override
  public StorageUnitMeta generateNewStorageUnitMetaByFragment(FragmentMeta fragmentMeta,
      long targetStorageId) throws MetaStorageException {
    String actualName = storage.addStorageUnit();
    StorageUnitMeta storageUnitMeta = new StorageUnitMeta(actualName, targetStorageId, actualName,
        true, false);
    storageUnitMeta.setCreatedBy(getIginxId());

    cache.updateStorageUnit(storageUnitMeta);
    for (StorageUnitHook hook : storageUnitHooks) {
      hook.onChange(null, storageUnitMeta);
    }
    storage.updateStorageUnit(storageUnitMeta);
    return storageUnitMeta;
  }

  @Override
  public List<Long> selectStorageEngineIdList() {
    List<Long> storageEngineIdList = getStorageEngineList().stream().map(StorageEngineMeta::getId)
        .collect(Collectors.toList());
    if (storageEngineIdList.size() <= 1 + ConfigDescriptor.getInstance().getConfig()
        .getReplicaNum()) {
      return storageEngineIdList;
    }
    Random random = new Random();
    for (int i = 0; i < storageEngineIdList.size(); i++) {
      int next = random.nextInt(storageEngineIdList.size());
      Long value = storageEngineIdList.get(next);
      storageEngineIdList.set(next, storageEngineIdList.get(i));
      storageEngineIdList.set(i, value);
    }
    return storageEngineIdList
        .subList(0, 1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum());
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
    String[] storageEngineStrings = ConfigDescriptor.getInstance().getConfig()
        .getStorageEngineList().split(",");
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
      logger.error("add user error: ", e);
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
      logger.error("update user error: ", e);
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
      logger.error("remove user error: ", e);
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

  @Override
  public void registerStorageUnitHook(StorageUnitHook hook) {
    this.storageUnitHooks.add(hook);
  }

  @Override
  public boolean election() {
    return storage.election();
  }

  @Override
  public void saveTimeSeriesData(InsertStatement statement) {
    cache.saveTimeSeriesData(statement);
  }

  @Override
  public List<TimeSeriesCalDO> getMaxValueFromTimeSeries() {
    return cache.getMaxValueFromTimeSeries();
  }

  @Override
  public Map<String, Double> getTimeseriesData() {
    return storage.getTimeseriesData();
  }

  @Override
  public int updateVersion() {
    return storage.updateVersion();
  }

  @Override
  public Map<Integer, Integer> getTimeseriesVersionMap() {
    return cache.getTimeseriesVersionMap();
  }

  @Override
  public void updateFragmentRequests(Map<FragmentMeta, Long> writeRequestsMap,
      Map<FragmentMeta, Long> readRequestsMap) {
    try {
      storage.lockFragmentRequestsCounter();
      storage.updateFragmentRequests(writeRequestsMap, readRequestsMap);
      storage.incrementFragmentRequestsCounter();
      storage.releaseFragmentRequestsCounter();
    } catch (Exception e) {
      logger.error("encounter error when update fragment requests: ", e);
    }
  }

  @Override
  public void updateFragmentHeat(Map<FragmentMeta, Long> writeHotspotMap,
      Map<FragmentMeta, Long> readHotspotMap) {
    try {
      storage.lockFragmentHeatCounter();
      storage.updateFragmentHeat(writeHotspotMap, readHotspotMap);
      storage.incrementFragmentHeatCounter();
      storage.releaseFragmentHeatCounter();
    } catch (Exception e) {
      logger.error("encounter error when update fragment heat: ", e);
    }
  }

  @Override
  public void updateTimeseriesHeat(Map<String, Long> timeseriesHeatMap) throws Exception {
    try {
      storage.lockTimeseriesHeatCounter();
      storage.updateTimeseriesLoad(timeseriesHeatMap);
      storage.incrementTimeseriesHeatCounter();
      storage.releaseTimeseriesHeatCounter();
    } catch (Exception e) {
      logger.error("encounter error when update timeseries heat: ", e);
    }
  }

  @Override
  public boolean isAllMonitorsCompleteCollection() {
    try {
      int fragmentRequestsCount = storage.getFragmentRequestsCounter();
      logger.error("fragmentRequestsCount = {}", fragmentRequestsCount);
      int fragmentHeatCount = storage.getFragmentHeatCounter();
      logger.error("fragmentHeatCount = {}", fragmentHeatCount);
      int count = getIginxList().size();
      logger.error("getIginxList().size() = {}", count);
      return fragmentRequestsCount >= count && fragmentHeatCount >= count;
    } catch (MetaStorageException e) {
      logger.error("encounter error when get monitor counter: ", e);
      return false;
    }
  }

  public boolean isAllTimeseriesMonitorsCompleteCollection() {
    try {
      int timeseriesHeatCount = storage.getTimeseriesHeatCounter();
      int count = getIginxList().size();
      return timeseriesHeatCount >= count;
    } catch (MetaStorageException e) {
      logger.error("encounter error when get monitor counter: ", e);
      return false;
    }
  }

  @Override
  public void clearMonitors() {
    try {
//      incMonitors();
      int time = 0;
//      while (!isAllMonitorsClearing()) {
//        Thread.sleep(100);
//        time++;
//        if (time > 20) {
//          logger.error("wait time more than {} ms", time * 20);
//          return;
//        }
//      }
      Thread.sleep(1000);
      if (getIginxList().get(0).getId() == getIginxId()) {
        storage.lockFragmentRequestsCounter();
        storage.lockFragmentHeatCounter();
        storage.lockTimeseriesHeatCounter();

        storage.resetFragmentRequestsCounter();
        storage.resetFragmentHeatCounter();
        storage.resetTimeseriesHeatCounter();
        storage.removeFragmentRequests();
        storage.removeFragmentHeat();
        storage.removeTimeseriesHeat();

        storage.releaseFragmentRequestsCounter();
        storage.releaseFragmentHeatCounter();
        storage.releaseTimeseriesHeatCounter();
      }
      HotSpotMonitor.getInstance().clear();
      RequestsMonitor.getInstance().clear();
//      storage.resetMonitorClearCounter();
    } catch (Exception e) {
      logger.error("encounter error when clear monitors: ", e);
    }
  }

  private void incMonitors() {
    try {
      storage.incrementMonitorClearCounter();
    } catch (Exception e) {
      logger.error("encounter error when increment monitor clear counter: ", e);
    }
  }

  private boolean isAllMonitorsClearing() {
    try {
      int monitorClearCount = storage.getMonitorClearCounter();
      logger.error("monitorClearCount = {}", monitorClearCount);
      int count = getIginxList().size();
      return monitorClearCount >= count;
    } catch (MetaStorageException e) {
      logger.error("encounter error when get monitor counter: ", e);
      return false;
    }
  }

  @Override
  public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat() {
    try {
      return storage.loadFragmentHeat(cache);
    } catch (Exception e) {
      logger.error("encounter error when remove fragment heat: ", e);
      return new Pair<>(new HashMap<>(), new HashMap<>());
    }
  }

  @Override
  public Map<FragmentMeta, Long> loadFragmentPoints() {
    try {
      return storage.loadFragmentPoints(cache);
    } catch (Exception e) {
      logger.error("encounter error when load fragment points: ", e);
      return new HashMap<>();
    }
  }

  public Map<String, Long> loadTimeseriesHeat() throws Exception {
    try {
      return storage.loadTimeseriesHeat();
    } catch (Exception e) {
      logger.error("encounter error when load fragment points: ", e);
      return new HashMap<>();
    }
  }

  @Override
  public void executeReshardJudging() {
    try {
      if (!reshardStatus.equals(NON_RESHARDING)) {
        return;
      }
      storage.lockReshardStatus();
      reshardStatus = JUDGING;
      isProposer = true;
      logger.info("iginx node {} propose to judge reshard", id);
      storage.releaseReshardStatus();
    } catch (MetaStorageException e) {
      logger.error("encounter error when proposing to reshard: ", e);
    }
  }

  @Override
  public boolean executeReshard() {
    try {
      logger.error("reshardStatus = {}", reshardStatus);
      if (!reshardStatus.equals(JUDGING)) {
        return false;
      }
      storage.lockReshardStatus();
      try {
        // 提议进入重分片流程，返回值为 true 代表提议成功，本节点成为 proposer；为 false 代表提议失败，说明已有其他节点提议成功
        if (storage.proposeToReshard()) {
          reshardStatus = EXECUTING;
          isProposer = true;
          // 生成最终节点状态和整体迁移计划
          // 根据整体迁移计划进行迁移
          logger.info("iginx node {} propose to reshard", id);
          // 在重分片判断阶段，proposer 节点不需要推送本地的存储后端统计信息
          return true;
        } else {
          return false;
        }
      } finally {
        storage.releaseReshardStatus();
      }
    } catch (MetaStorageException e) {
      logger.error("encounter error when proposing to reshard: ", e);
    }
    return false;
  }

  @Override
  public void doneReshard() {
    try {
      storage.lockReshardStatus();
      reshardStatus = NON_RESHARDING;
      storage.updateReshardStatus(NON_RESHARDING);
      storage.releaseReshardStatus();
    } catch (MetaStorageException e) {
      logger.error("encounter error when proposing to reshard: ", e);
    }
  }

  @Override
  public boolean isResharding() {
    return reshardStatus != NON_RESHARDING;
  }

  private void initMaxActiveEndTimeStatistics() throws MetaStorageException {
    storage.registerMaxActiveEndTimeStatisticsChangeHook((endTime) -> {
      if (endTime <= 0L) {
        return;
      }
      updateMaxActiveEndTime(endTime);
      int updatedCounter = maxActiveEndTimeStatisticsCounter.incrementAndGet();
      if (isProposer) {
        logger.info("iginx node {}(proposer) increment max active end time statistics counter {}",
            this.id, updatedCounter);
      } else {
        logger.info("iginx node {} increment max active end time statistics counter {}", this.id,
            updatedCounter);
      }
    });
  }

  private void initReshardStatus() throws MetaStorageException {
    storage.registerReshardStatusHook(status -> {
      try {
        reshardStatus = status;
        if (reshardStatus.equals(EXECUTING)) {
          storage.lockMaxActiveEndTimeStatistics();
          storage.addOrUpdateMaxActiveEndTimeStatistics(maxActiveEndTime.get());
          storage.releaseMaxActiveEndTimeStatistics();

          storage.lockReshardCounter();
          storage.incrementReshardCounter();
          storage.releaseReshardCounter();
        }
        if (reshardStatus.equals(NON_RESHARDING)) {
          if (isProposer) {
            logger.info("iginx node {}(proposer) finish to reshard", id);
          } else {
            logger.info("iginx node {} finish to reshard", id);
          }

          isProposer = false;
          maxActiveEndTimeStatisticsCounter.set(0);
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

          if (reshardStatus == EXECUTING) {
            storage.lockReshardStatus();
            storage.updateReshardStatus(NON_RESHARDING);
            storage.releaseReshardStatus();
          }
        }
      } catch (MetaStorageException e) {
        logger.error("encounter error when updating reshard counter: ", e);
      }
    });
    storage.lockReshardCounter();
    storage.removeReshardCounter();
    storage.releaseReshardCounter();
  }

  private void recover() {
    try {
      storage.lockReshardStatus();
      reshardStatus = RECOVER;
      storage.releaseReshardStatus();

      try {
        MigrationLoggerAnalyzer migrationLoggerAnalyzer = new MigrationLoggerAnalyzer();
        migrationLoggerAnalyzer.analyze();
        if (migrationLoggerAnalyzer.isStartMigration() && !migrationLoggerAnalyzer
            .isMigrationFinished() && !migrationLoggerAnalyzer
            .isLastMigrationExecuteTaskFinished()) {
          MigrationExecuteTask migrationExecuteTask = migrationLoggerAnalyzer
              .getLastMigrationExecuteTask();
          if (migrationExecuteTask.getMigrationExecuteType() == MigrationExecuteType.MIGRATION) {
            FragmentMeta fragmentMeta = migrationExecuteTask.getFragmentMeta();
            // 直接删除整个du
            List<String> paths = new ArrayList<>();
            paths.add(migrationExecuteTask.getMasterStorageUnitId() + "*");
            Delete delete = new Delete(new FragmentSource(fragmentMeta), new ArrayList<>(), paths);
            physicalEngine.execute(delete);
          }
        }
      } catch (IOException | PhysicalException e) {
        e.printStackTrace();
      }

      storage.lockReshardStatus();
      reshardStatus = NON_RESHARDING;
      storage.releaseReshardStatus();
    } catch (Exception e) {
      logger.error("encounter error when proposing to reshard: ", e);
    }
  }

  public void updateMaxActiveEndTime(long endTime) {
    maxActiveEndTime.getAndUpdate(e -> Math.max(e, endTime
        + ConfigDescriptor.getInstance().getConfig().getReshardFragmentTimeMargin() * 1000));
  }

  public long getMaxActiveEndTime() {
    return maxActiveEndTime.get();
  }

  public void submitMaxActiveEndTime() {
    try {
      storage.lockMaxActiveEndTimeStatistics();
      storage.addOrUpdateMaxActiveEndTimeStatistics(maxActiveEndTime.get());
      storage.releaseMaxActiveEndTimeStatistics();
    } catch (MetaStorageException e) {
      logger.error("encounter error when submitting max active time: ", e);
    }
  }
}
