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
import cn.edu.tsinghua.iginx.core.IService;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.utils.JsonUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import com.google.gson.reflect.TypeToken;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public abstract class AbstractMetaManager implements IMetaManager, IService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMetaManager.class);
    protected final Map<Long, StorageEngineMeta> storageEngineMetaMap = new ConcurrentHashMap<>();
    private final CuratorFramework zookeeperClient;
    private final Map<Long, IginxMeta> iginxMetaMap = new ConcurrentHashMap<>();
    private final List<StorageEngineChangeHook> storageEngineChangeHooks = Collections.synchronizedList(new ArrayList<>());
    private final Map<String, Map<String, Integer>> schemaMappings = new ConcurrentHashMap<>();
    protected TreeCache iginxCache;
    protected TreeCache storageEngineCache;
    protected TreeCache fragmentCache;
    protected TreeCache schemaMappingsCache;
    private long iginxId;

    public AbstractMetaManager() {
        zookeeperClient = CuratorFrameworkFactory.builder()
                .connectString(ConfigDescriptor.getInstance().getConfig().getZookeeperConnectionString())
                .connectionTimeoutMs(15000)
                .retryPolicy(new RetryForever(1000))
                .build();
        zookeeperClient.start();

        try {
            registerIginxListener();
            resolveIginxMetaFromZooKeeper();
            registerIginxMeta();

            registerStorageEngineMeta();
            registerStorageEngineListener();
            resolveStorageEngineFromZooKeeper();

            resolveFragmentFromZooKeeper();

            resolveSchemaMappingsFromZooKeeper();
        } catch (Exception e) {
            logger.error("get error when init meta manager: ", e);
            System.exit(1);
        }
    }

    private void resolveSchemaMappingsFromZooKeeper() throws Exception {
        InterProcessMutex mutex = new InterProcessMutex(zookeeperClient, Constants.SCHEMA_MAPPING_LOCK_NODE);
        try {
            mutex.acquire();
            if (this.zookeeperClient.checkExists().forPath(Constants.SCHEMA_MAPPING_PREFIX) == null) {
                // 当前还没有数据，创建父节点，然后不需要解析数据
                this.zookeeperClient.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(Constants.SCHEMA_MAPPING_PREFIX);
            } else {
                // 从 zookeeper 中解析 schema mapping 数据
                List<String> schemas = this.zookeeperClient.getChildren()
                        .forPath(Constants.SCHEMA_MAPPING_PREFIX);
                for (String schema : schemas) {
                    Map<String, Integer> schemaMapping = JsonUtils.getGson().fromJson(new String(this.zookeeperClient.getData()
                            .forPath(Constants.SCHEMA_MAPPING_PREFIX + "/" + schema)), new TypeToken<Map<String, Integer>>() {
                    }.getType());
                    schemaMappings.put(schema, schemaMapping);
                }
            }
            registerSchemaMappingsListener();
        } finally {
            mutex.release();
        }
    }

    private void registerSchemaMappingsListener() throws Exception {
        this.schemaMappingsCache = new TreeCache(this.zookeeperClient, Constants.SCHEMA_MAPPING_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            if (event.getData() == null || event.getData().getPath() == null || event.getData().getPath().equals(Constants.SCHEMA_MAPPING_PREFIX)) {
                return; // 创建根节点，不必理会
            }
            byte[] data;
            Map<String, Integer> schemaMapping;
            String schema = event.getData().getPath().substring(Constants.SCHEMA_MAPPING_PREFIX.length());
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    data = event.getData().getData();
                    schemaMapping = JsonUtils.getGson().fromJson(new String(data), new TypeToken<Map<String, Integer>>() {
                    }.getType());
                    schemaMappings.put(schema, schemaMapping);
                    break;
                case NODE_REMOVED:
                    schemaMappings.remove(schema);
                    break;
                default:
                    break;
            }
        };
        this.schemaMappingsCache.getListenable().addListener(listener);
        this.schemaMappingsCache.start();
    }

    private void registerIginxListener() throws Exception {
        this.iginxCache = new TreeCache(this.zookeeperClient, Constants.IGINX_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            byte[] data;
            IginxMeta iginxMeta;
            switch (event.getType()) {
                case NODE_UPDATED:
                    data = event.getData().getData();
                    iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
                    if (iginxMeta != null) {
                        logger.info("new iginx comes to cluster: id = " + iginxMeta.getId() + " ,ip = " + iginxMeta.getIp() + " , port = " + iginxMeta.getPort());
                        iginxMetaMap.put(iginxMeta.getId(), iginxMeta);
                    } else {
                        logger.error("resolve iginx meta from zookeeper error");
                    }
                    break;
                case NODE_REMOVED:
                    data = event.getData().getData();
                    String path = event.getData().getPath();
                    logger.info("node " + path + " is removed");
                    if (path.equals(Constants.IGINX_NODE_PREFIX)) {
                        // 根节点被删除
                        logger.info("all iginx leave from cluster");
                        iginxMetaMap.clear();
                        break;
                    }
                    iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
                    if (iginxMeta != null) {
                        logger.info("iginx leave from cluster: id = " + iginxMeta.getId() + " ,ip = " + iginxMeta.getIp() + " , port = " + iginxMeta.getPort());
                        iginxMetaMap.remove(iginxMeta.getId());
                    } else {
                        logger.error("resolve iginx meta from zookeeper error");
                    }
                    break;
                default:
                    break;
            }
        };
        this.iginxCache.getListenable().addListener(listener);
        this.iginxCache.start();
    }

    private void resolveIginxMetaFromZooKeeper() throws Exception {
        if (this.zookeeperClient.checkExists().forPath(Constants.IGINX_NODE_PREFIX) == null) {
            return;
        }
        List<String> children = this.zookeeperClient.getChildren()
                .forPath(Constants.IGINX_NODE_PREFIX);
        for (String childName : children) {
            byte[] data = this.zookeeperClient.getData()
                    .forPath(Constants.IGINX_NODE_PREFIX + "/" + childName);
            IginxMeta iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
            if (iginxMeta == null) {
                logger.error("resolve data from " + Constants.IGINX_NODE_PREFIX + "/" + childName + " error");
                continue;
            }
            this.iginxMetaMap.putIfAbsent(iginxMeta.getId(), iginxMeta);
        }
    }

    private void registerIginxMeta() throws Exception {
        String nodeName = this.zookeeperClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                .forPath(Constants.IGINX_NODE, "".getBytes(StandardCharsets.UTF_8));
        long id = Long.parseLong(nodeName.substring(Constants.IGINX_NODE.length()));
        IginxMeta iginxMeta = new IginxMeta(id, ConfigDescriptor.getInstance().getConfig().getIp(),
                ConfigDescriptor.getInstance().getConfig().getPort(), null);
        this.zookeeperClient.setData()
                .forPath(nodeName, JsonUtils.toJson(iginxMeta));
        this.iginxId = id;
        SnowFlakeUtils.init(id);
    }

    private void registerStorageEngineMeta() throws Exception {
        InterProcessMutex lock = new InterProcessMutex(this.zookeeperClient, Constants.STORAGE_ENGINE_LOCK_NODE);
        lock.acquire();
        try {
            if (this.zookeeperClient.checkExists().forPath(Constants.STORAGE_ENGINE_NODE_PREFIX) == null) { // 节点不存在，说明还没有别的 iginx 节点写入过元信息
                for (StorageEngineMeta storageEngineMeta : resolveStorageEngineFromConf()) {
                    String nodeName = this.zookeeperClient.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                            .forPath(Constants.STORAGE_ENGINE_NODE, "".getBytes(StandardCharsets.UTF_8));
                    long id = Long.parseLong(nodeName.substring(Constants.STORAGE_ENGINE_NODE.length()));
                    storageEngineMeta.setId(id);
                    this.zookeeperClient.setData()
                            .forPath(nodeName, JsonUtils.toJson(storageEngineMeta));
                }
            }
        } finally {
            lock.release();
        }
    }

    private void registerStorageEngineListener() throws Exception {
        this.storageEngineCache = new TreeCache(this.zookeeperClient, Constants.STORAGE_ENGINE_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            byte[] data;
            StorageEngineMeta storageEngineMeta = null;
            StorageEngineMeta originalStorageEngineMeta = null;
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    data = event.getData().getData();
                    logger.info("storage engine meta updated " + event.getData().getPath());
                    storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
                    logger.info("storage engine: " + new String(data));
                    if (storageEngineMeta != null) {
                        logger.info("new storage engine comes to cluster: id = " + storageEngineMeta.getId() + " ,ip = " + storageEngineMeta.getIp() + " , port = " + storageEngineMeta.getPort());
                        originalStorageEngineMeta = storageEngineMetaMap.get(storageEngineMeta.getId());
                        storageEngineMetaMap.put(storageEngineMeta.getId(), storageEngineMeta);
                    } else {
                        logger.error("resolve storage engine from zookeeper error");
                    }
                    break;
                case NODE_REMOVED:
                    data = event.getData().getData();
                    String path = event.getData().getPath();
                    logger.info("node " + path + " is removed");
                    if (path.equals(Constants.IGINX_NODE_PREFIX)) {
                        // 根节点被删除
                        logger.info("all iginx leave from cluster");
                        storageEngineMetaMap.clear();
                        break;
                    }
                    storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
                    if (storageEngineMeta != null) {
                        logger.info("storage engine leave from cluster: id = " + storageEngineMeta.getId() + " ,ip = " + storageEngineMeta.getIp() + " , port = " + storageEngineMeta.getPort());
                        storageEngineMetaMap.remove(storageEngineMeta.getId());
                    } else {
                        logger.error("resolve storage engine from zookeeper error");
                    }
                    break;
                default:
                    break;
            }
            if (storageEngineMeta != null || originalStorageEngineMeta != null) {
                for (StorageEngineChangeHook hook : storageEngineChangeHooks) {
                    hook.onChanged(originalStorageEngineMeta, storageEngineMeta);
                }
            }
        };
        this.storageEngineCache.getListenable().addListener(listener);
        this.storageEngineCache.start();

    }

    private void resolveStorageEngineFromZooKeeper() throws Exception {
        if (this.zookeeperClient.checkExists().forPath(Constants.STORAGE_ENGINE_NODE_PREFIX) == null) {
            return;
        }
        List<String> children = this.zookeeperClient.getChildren()
                .forPath(Constants.STORAGE_ENGINE_NODE_PREFIX);
        for (String childName : children) {
            byte[] data = this.zookeeperClient.getData()
                    .forPath(Constants.STORAGE_ENGINE_NODE_PREFIX + "/" + childName);
            StorageEngineMeta storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
            if (storageEngineMeta == null) {
                logger.error("resolve data from " + Constants.STORAGE_ENGINE_NODE_PREFIX + "/" + childName + " error");
                continue;
            }
            this.storageEngineMetaMap.putIfAbsent(storageEngineMeta.getId(), storageEngineMeta);
        }
    }

    private List<StorageEngineMeta> resolveStorageEngineFromConf() {
        List<StorageEngineMeta> storageEngineMetaList = new ArrayList<>();
        logger.info("resolve storage engine meta from config");
        String[] storageEngineStrings = ConfigDescriptor.getInstance().getConfig()
                .getStorageEngineList().split(",");
        for (int i = 0; i < storageEngineStrings.length; i++) {
            // TODO
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
            storageEngineMetaList.add(new StorageEngineMeta(i, ip, port, extraParams, storageEngine, iginxId));
        }
        return storageEngineMetaList;
    }

    private void resolveFragmentFromZooKeeper() throws Exception {
        InterProcessMutex mutex = new InterProcessMutex(zookeeperClient, Constants.FRAGMENT_LOCK_NODE);
        try {
            mutex.acquire();
            if (this.zookeeperClient.checkExists().forPath(Constants.FRAGMENT_NODE_PREFIX) == null) {
                // 当前还没有数据，创建父节点，然后不需要解析数据
                this.zookeeperClient.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(Constants.FRAGMENT_NODE_PREFIX);
            } else {
                List<String> tsIntervalNames = this.zookeeperClient.getChildren().forPath(Constants.FRAGMENT_NODE_PREFIX);
                Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap = new HashMap<>();
                for (String tsIntervalName : tsIntervalNames) {
                    TimeSeriesInterval fragmentTimeSeries = TimeSeriesInterval.fromString(tsIntervalName);
                    List<FragmentMeta> fragmentMetaList = new ArrayList<>();
                    List<String> timeIntervalNames = this.zookeeperClient.getChildren().forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + tsIntervalName);
                    for (String timeIntervalName : timeIntervalNames) {
                        FragmentMeta fragmentMeta = JsonUtils.fromJson(this.zookeeperClient.getData()
                                .forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + tsIntervalName + "/" + timeIntervalName), FragmentMeta.class);
                        fragmentMetaList.add(fragmentMeta);
                    }
                    fragmentListMap.put(fragmentTimeSeries, fragmentMetaList);
                }
                initFragment(fragmentListMap);
            }
            registerFragmentListener();
        } finally {
            mutex.release();
        }
    }

    private void registerFragmentListener() throws Exception {
        this.fragmentCache = new TreeCache(this.zookeeperClient, Constants.FRAGMENT_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            byte[] data;
            FragmentMeta fragmentMeta;
            switch (event.getType()) {
                case NODE_UPDATED:
                    data = event.getData().getData();
                    fragmentMeta = JsonUtils.fromJson(data, FragmentMeta.class);
                    if (fragmentMeta != null) {
                        // 本机更新的分片，此前已经更新过了
                        if (fragmentMeta.getUpdatedBy() == iginxId) {
                            break;
                        }
                        updateFragment(fragmentMeta);

                    } else {
                        logger.error("resolve fragment from zookeeper error");
                    }
                    break;
                case NODE_ADDED:
                    String path = event.getData().getPath();
                    String[] pathParts = path.split("/");
                    if (pathParts.length == 4) {
                        fragmentMeta = JsonUtils.fromJson(event.getData().getData(), FragmentMeta.class);
                        if (fragmentMeta != null) {
                            // 本机创建的分片，此前已经更新过了
                            if (fragmentMeta.getCreatedBy() == iginxId) {
                                break;
                            }
                            addFragment(fragmentMeta);
                            for (FragmentReplicaMeta replicaMeta : fragmentMeta.getReplicaMetas().values()) {
                                long storageEngineId = replicaMeta.getStorageEngineId();
                                StorageEngineMeta storageEngineMeta = storageEngineMetaMap.get(storageEngineId);
                                storageEngineMeta.addLatestFragmentReplicaMetas(replicaMeta);
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        };
        this.fragmentCache.getListenable().addListener(listener);
        this.fragmentCache.start();
    }


    @Override
    public boolean addStorageEngine(StorageEngineMeta storageEngineMeta) {
        InterProcessMutex lock = new InterProcessMutex(this.zookeeperClient, Constants.STORAGE_ENGINE_LOCK_NODE);
        try {
            lock.acquire();
            String nodeName = this.zookeeperClient.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(Constants.STORAGE_ENGINE_NODE, "".getBytes(StandardCharsets.UTF_8));
            logger.info("[addStorageEngine] new node name: " + nodeName);
            long id = Long.parseLong(nodeName.substring(Constants.STORAGE_ENGINE_NODE.length()));
            storageEngineMeta.setId(id);
            this.zookeeperClient.setData()
                    .forPath(nodeName, JsonUtils.toJson(storageEngineMeta));
            return true;
        } catch (Exception e) {
            logger.error("add storage engine error: ", e);
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                logger.error("release mutex error: ", e);
            }
        }

        return false;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return new ArrayList<>(this.storageEngineMetaMap.values());
    }

    @Override
    public List<FragmentReplicaMeta> getFragmentListByStorageEngineId(long StorageEngineId) {
        return null;
    }

    @Override
    public List<IginxMeta> getIginxList() {
        return new ArrayList<>(iginxMetaMap.values());
    }

    @Override
    public long getIginxId() {
        return this.iginxId;
    }

    @Override
    public boolean createFragments(List<FragmentMeta> fragments) {
        InterProcessMutex mutex = new InterProcessMutex(this.zookeeperClient, Constants.FRAGMENT_LOCK_NODE);
        try {
            mutex.acquire();
            Map<TimeSeriesInterval, FragmentMeta> latestFragments = getLatestFragmentMap();
            for (FragmentMeta originalFragmentMeta : latestFragments.values()) { // 终结老分片
                FragmentMeta fragmentMeta = originalFragmentMeta.endFragmentMeta(fragments.get(0).getTimeInterval().getStartTime());
                // 在更新分片时，先更新本地
                fragmentMeta.setUpdatedBy(iginxId);
                updateFragment(fragmentMeta);
                this.zookeeperClient.setData()
                        .forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + fragmentMeta.getTsInterval().toString() + "/" + fragmentMeta.getTimeInterval().toString(), JsonUtils.toJson(fragmentMeta));
            }
            for (FragmentMeta fragmentMeta : fragments) {
                // 针对本机创建的分片，直接将其加入到本地
                fragmentMeta.setCreatedBy(iginxId);
                addFragment(fragmentMeta);
                this.zookeeperClient.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + fragmentMeta.getTsInterval().toString() + "/" + fragmentMeta.getTimeInterval().toString(), JsonUtils.toJson(fragmentMeta));
            }
            return true;
        } catch (Exception e) {
            logger.error("create fragment error: ", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                logger.error("release mutex error: ", e);
            }
        }
        return false;
    }

    @Override
    public void shutdown() {
        this.iginxCache.close();
        this.storageEngineCache.close();
        this.fragmentCache.close();
    }

    protected abstract void initFragment(Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap);

    protected abstract void addFragment(FragmentMeta fragmentMeta);

    protected abstract void updateFragment(FragmentMeta fragmentMeta);

    @Override
    public boolean tryCreateInitialFragments(List<FragmentMeta> initialFragments) {
        InterProcessMutex mutex = new InterProcessMutex(this.zookeeperClient, Constants.FRAGMENT_LOCK_NODE);
        try {
            mutex.acquire();
            if (hasFragment()) {
                return false;
            }
            initialFragments.sort(Comparator.comparingLong(o -> o.getTimeInterval().getStartTime()));
            for (FragmentMeta fragmentMeta : initialFragments) {
                logger.info("create initial fragment: " + new String(JsonUtils.toJson(fragmentMeta)));
                String tsIntervalName = fragmentMeta.getTsInterval().toString();
                String timeIntervalName = fragmentMeta.getTimeInterval().toString();
                // 针对本机创建的分片，直接将其加入到本地
                fragmentMeta.setCreatedBy(iginxId);
                addFragment(fragmentMeta);
                this.zookeeperClient.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + tsIntervalName + "/" + timeIntervalName, JsonUtils.toJson(fragmentMeta));

            }
            return true;
        } catch (Exception e) {
            logger.error("create initial fragment error: ", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                logger.error("release mutex error: ", e);
            }
        }
        return false;
    }

    @Override
    public List<Long> chooseStorageEngineIdListForNewFragment() {
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
    public long chooseStorageEngineIdForDatabasePlan() {
        List<StorageEngineMeta> storageEngineMetaList = getStorageEngineList().stream().
                sorted(Comparator.comparing(StorageEngineMeta::getFragmentReplicaMetaNum)).collect(Collectors.toList());
        return storageEngineMetaList.get(0).getId();
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> generateFragments(String startPath, long startTime) {
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentMap = new HashMap<>();
        List<FragmentMeta> leftFragmentList = new ArrayList<>();
        List<FragmentMeta> rightFragmentList = new ArrayList<>();

        leftFragmentList.add(new FragmentMeta(startPath, null, startTime, Long.MAX_VALUE, chooseStorageEngineIdListForNewFragment()));
        rightFragmentList.add(new FragmentMeta(null, startPath, startTime, Long.MAX_VALUE, chooseStorageEngineIdListForNewFragment()));
        if (startTime != 0) {
            leftFragmentList.add(new FragmentMeta(startPath, null, 0, startTime, chooseStorageEngineIdListForNewFragment()));
            rightFragmentList.add(new FragmentMeta(null, startPath, 0, startTime, chooseStorageEngineIdListForNewFragment()));
        }
        fragmentMap.put(new TimeSeriesInterval(startPath, null), leftFragmentList);
        fragmentMap.put(new TimeSeriesInterval(null, startPath), rightFragmentList);

        return fragmentMap;
    }

    @Override
    public void registerStorageEngineChangeHook(StorageEngineChangeHook hook) {
        if (hook != null) {
            this.storageEngineChangeHooks.add(hook);
        }
    }

    @Override
    public List<FragmentMeta> generateFragments(List<String> prefixList, long startTime) {
        List<FragmentMeta> resultList = new ArrayList<>();
        prefixList = prefixList.stream().filter(Objects::nonNull).sorted(String::compareTo).collect(Collectors.toList());
        String previousPrefix;
        String prefix = null;
        for (String s : prefixList) {
            previousPrefix = prefix;
            prefix = s;
            resultList.add(new FragmentMeta(previousPrefix, prefix, startTime + 5 * 60 * 1000, Long.MAX_VALUE, chooseStorageEngineIdListForNewFragment()));
        }
        resultList.add(new FragmentMeta(prefix, null, startTime + 5 * 60 * 1000, Long.MAX_VALUE, chooseStorageEngineIdListForNewFragment()));
        return resultList;
    }

    @Override
    public void addOrUpdateSchemaMapping(String schema, Map<String, Integer> schemaMapping) {
        InterProcessMutex mutex = new InterProcessMutex(this.zookeeperClient, Constants.SCHEMA_MAPPING_LOCK_NODE);
        try {
            mutex.acquire();
            if (schemaMapping == null) { // 删除数据
                if (schemaMappings.containsKey(schema)) {
                    schemaMappings.remove(schema);
                    this.zookeeperClient.delete()
                            .forPath(Constants.SCHEMA_MAPPING_PREFIX + "/" + schema);
                }
            } else { // 增加/更新数据
                if (schemaMappings.containsKey(schema)) { // 更新数据
                    schemaMappings.put(schema, schemaMapping);
                    this.zookeeperClient.setData()
                            .forPath(Constants.SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
                } else { // 增加数据
                    schemaMappings.put(schema, schemaMapping);
                    this.zookeeperClient.create()
                            .forPath(Constants.SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
                }
            }
        } catch (Exception e) {
            logger.error("add or update schema mapping  error: ", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                logger.error("release mutex error: ", e);
            }
        }
    }

    @Override
    public void addOrUpdateSchemaMappingItem(String schema, String key, int value) {
        InterProcessMutex mutex = new InterProcessMutex(this.zookeeperClient, Constants.SCHEMA_MAPPING_LOCK_NODE);
        try {
            mutex.acquire();
            boolean isNew = false;
            Map<String, Integer> schemaMapping = schemaMappings.get(schema);
            if (schemaMapping == null) {
                isNew = true;
                schemaMapping = new HashMap<>();
                schemaMappings.put(schema, schemaMapping);
            }
            if (value == -1) { // 删除数据
                schemaMapping.remove(key);
            } else { // 增加/更新数据
                schemaMapping.put(key, value);
            }
            if (isNew) {
                this.zookeeperClient.create()
                        .forPath(Constants.SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
            } else {
                this.zookeeperClient.setData()
                        .forPath(Constants.SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
            }
        } catch (Exception e) {
            logger.error("add or update schema mapping  error: ", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                logger.error("release mutex error: ", e);
            }
        }
    }

    @Override
    public Map<String, Integer> getSchemaMapping(String schema) {
        if (this.schemaMappings.get(schema) == null)
            return null;
        return Collections.unmodifiableMap(this.schemaMappings.get(schema));
    }

    @Override
    public int getSchemaMappingItem(String schema, String key) {
        Map<String, Integer> schemaMapping = schemaMappings.get(schema);
        if (schemaMapping == null) {
            return -1;
        }
        return schemaMapping.getOrDefault(key, -1);
    }
}
