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
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.SerializeUtils;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class MetaManager implements IMetaManager, IService {

    private static final Logger logger = LoggerFactory.getLogger(MetaManager.class);

    private static MetaManager INSTANCE = null;

    private final CuratorFramework zookeeperClient;

    // 数据库实例集群的元信息
    private final Map<Long, DatabaseMeta> databaseMetaMap = new ConcurrentHashMap<>();

    // iginx 集群的元信息
    private final Map<Long, IginxMeta> iginxMetaMap = new ConcurrentHashMap<>();

    // iginx 集群的元信息树 Zookeeper Cache
    private TreeCache iginxCache;

    // 分片树
    private final Map<String, Pair<List<FragmentMeta>, ReadWriteLock>> fragmentMap = new ConcurrentHashMap<>();

    // 分片树的 Zookeeper Cache
    private TreeCache fragmentCache;

    // 数据库集群的元信息
    private TreeCache databaseCache;

    private MetaManager() {
        zookeeperClient = CuratorFrameworkFactory.builder()
                .connectString(ConfigDescriptor.getInstance().getConfig().getZookeeperConnectionString())
                .connectionTimeoutMs(15000)
                .retryPolicy(new RetryForever(1000))
                .build();
        zookeeperClient.start();
        try {
            registerIginxListener();
            resolveDatabaseMetaFromConf();
            registerIginxMeta();
            registerDatabaseMeta();
            registerIginxListener();
            registerFragmentListener();
        } catch (Exception e) {
            logger.error("register meta error: ", e);
        }
    }

    private void registerIginxListener() throws Exception {
        this.iginxCache = new TreeCache(this.zookeeperClient, Constants.IGINX_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            byte[] data;
            IginxMeta iginxMeta;
            switch (event.getType()) {
                case NODE_UPDATED:
                    data = event.getData().getData();
                    iginxMeta = SerializeUtils.deserialize(data, IginxMeta.class);
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
                    iginxMeta = SerializeUtils.deserialize(data, IginxMeta.class);
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

    private void resolveIginxMetaFromZookeeper() throws Exception {

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
                .forPath(nodeName, SerializeUtils.serialize(iginxMeta));
        this.iginxMetaMap.put(id, iginxMeta);
        SnowFlakeUtils.init(id);
    }

    private void registerDatabaseMeta() throws Exception {
        InterProcessMutex lock = new InterProcessMutex(this.zookeeperClient, Constants.STORAGE_ENGINE_LOCK_NODE);
        lock.acquire();
        try {
            if (this.zookeeperClient.checkExists().forPath(Constants.STORAGE_ENGINE_NODE_PREFIX) == null) { // 节点不存在，从配置中加载，再写入 zookeeper
                resolveDatabaseMetaFromConf();
                // 将数据库元信息写入到 zookeeper
                for (DatabaseMeta databaseMeta: databaseMetaMap.values()) {
                    long id = databaseMeta.getId();
                    this.zookeeperClient.create()
                            .creatingParentContainersIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(Constants.STORAGE_ENGINE_NODE_PREFIX + "/" + id, SerializeUtils.serialize(databaseMeta));
                }
            } else { // 从 zookeeper 中加载配置信息
                resolveDatabaseMetaFromZooKeeper();
            }
        } catch (Exception e) {
            logger.error("encounter error when write/read database meta from zookeeper: ", e);
            resolveDatabaseMetaFromConf();
        } finally {
            lock.release();
        }
    }

    private void registerDatabaseListener() throws Exception {

    }

    private void registerFragmentListener() throws Exception {
        this.fragmentCache = new TreeCache(this.zookeeperClient, Constants.FRAGMENT_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            logger.info("出现了创建分片事件！");
            FragmentMeta fragmentMeta;
            String path = "";
            String[] pathParts = null;
            switch (event.getType()) {
                case NODE_ADDED:
                    path = event.getData().getPath();
                    pathParts = path.split("/");
                    if (pathParts.length == 3) { // 一个新的分片簇
                        String key = pathParts[2];
                        logger.info("create a new series of fragment: " + key);
                        fragmentMap.put(key, new Pair<>(new ArrayList<>(), new ReentrantReadWriteLock()));
                    } else if (pathParts.length == 4) {
                        String key = pathParts[2];
                        fragmentMeta = SerializeUtils.deserialize(event.getData().getData(), FragmentMeta.class);
                        if (fragmentMeta != null) {
                            Pair<List<FragmentMeta>, ReadWriteLock> pair = this.fragmentMap.get(key);
                            pair.v.writeLock().lock();
                            pair.k.add(fragmentMeta);
                            for (FragmentReplicaMeta replicaMeta: fragmentMeta.getReplicaMetas().values()) {
                                long databaseId = replicaMeta.getDatabaseId();
                                this.databaseMetaMap.get(databaseId).addFragmentReplicaMeta(replicaMeta);
                            }
                            pair.v.writeLock().unlock();
                        }
                    }
                    break;
                case NODE_UPDATED:
                    path = event.getData().getPath();
                    pathParts = path.split("/");
                    if (pathParts.length == 4) {
                        String key = pathParts[1];
                        fragmentMeta = SerializeUtils.deserialize(event.getData().getData(), FragmentMeta.class);
                        if (fragmentMeta != null) {
                            Pair<List<FragmentMeta>, ReadWriteLock> pair = this.fragmentMap.get(key);
                            pair.v.writeLock().lock();
                            FragmentMeta originalFragmentMeta = pair.k.remove(pair.k.size() - 1);
                            pair.k.add(fragmentMeta);
                            for (FragmentReplicaMeta replicaMeta: originalFragmentMeta.getReplicaMetas().values()) {
                                long databaseId = replicaMeta.getDatabaseId();
                                this.databaseMetaMap.get(databaseId).removeFragmentReplicaMeta(replicaMeta);
                            }
                            for (FragmentReplicaMeta replicaMeta: fragmentMeta.getReplicaMetas().values()) {
                                long databaseId = replicaMeta.getDatabaseId();
                                this.databaseMetaMap.get(databaseId).addFragmentReplicaMeta(replicaMeta);
                            }
                            pair.v.writeLock().unlock();
                        }
                    }
                    break;
                case NODE_REMOVED:
                    path = event.getData().getPath();
                    logger.error("unexpected fragment replica node removed: " + path);
                    break;
                default:
                    break;
            }
            logger.info("涉及到的路径：" + path + ", 事件的类型：" + event.getType());
            if (pathParts != null) {
                for (String pathPart: pathParts) {
                    logger.info("拆分的路径片段有：" + pathPart);
                }
            }
        };
        this.fragmentCache.getListenable().addListener(listener);
        this.fragmentCache.start();
    }

    private void resolveDatabaseMetaFromZooKeeper() throws Exception {
        List<String> children = this.zookeeperClient.getChildren()
                .forPath(Constants.STORAGE_ENGINE_NODE_PREFIX);
        for (String childName: children) {
            byte[] data = this.zookeeperClient.getData()
                    .forPath(Constants.STORAGE_ENGINE_NODE_PREFIX + "/" + childName);
            DatabaseMeta databaseMeta = SerializeUtils.deserialize(data, DatabaseMeta.class);
            if (databaseMeta == null) {
                logger.error("resolve data from node " + Constants.STORAGE_ENGINE_NODE_PREFIX + "/" + childName + " failed.");
                continue;
            }
            this.databaseMetaMap.put(databaseMeta.getId(), databaseMeta);
        }
    }

    private void resolveDatabaseMetaFromConf() {
        logger.info("resolve database meta from config");
        String[] databaseMetaStrings = ConfigDescriptor.getInstance().getConfig()
                .getStorageEngineList().split(",");
        logger.info("database connection string: " + ConfigDescriptor.getInstance().getConfig().getStorageEngineList());
        logger.info("database count: " + databaseMetaStrings.length);
        for (int i = 0; i < databaseMetaStrings.length; i++) {
            String[] databaseMetaParts = databaseMetaStrings[i].split(":");
            String ip = databaseMetaParts[0];
            int port = Integer.parseInt(databaseMetaParts[1]);
            StorageEngine storageEngine = StorageEngine.fromString(databaseMetaParts[2]);
            Map<String, String> extraParams = new HashMap<>();
            for (int j = 3; j < databaseMetaParts.length; j++) {
                String[] KAndV = databaseMetaParts[j].split("=");
                if (KAndV.length != 2) {
                    logger.error("unexpected database meta info: " + databaseMetaStrings[i]);
                    continue;
                }
                extraParams.put(KAndV[0], KAndV[1]);
            }
            databaseMetaMap.put((long) i, new DatabaseMeta((long) i, ip, port, extraParams, storageEngine, new ArrayList<>()));
        }
    }

    @Override
    public List<DatabaseMeta> getDatabaseList() {
        return databaseMetaMap.values().stream()
                .map(DatabaseMeta::basicInfo)
                .collect(Collectors.toList());
    }

    @Override
    public List<FragmentReplicaMeta> getFragmentListByDatabase(long databaseId) {
        return new ArrayList<>(databaseMetaMap.get(databaseId)
                .getFragmentReplicaMetaList());
    }

    @Override
    public List<IginxMeta> getIginxList() {
        return new ArrayList<>(iginxMetaMap.values());
    }

    @Override
    public List<FragmentMeta> getFragmentListByKeyAndTimeInterval(String key, long startTime, long endTime) {
        List<FragmentMeta> resultList = new ArrayList<>();
        Pair<List<FragmentMeta>, ReadWriteLock> pair = this.fragmentMap.get(key);
        if (pair == null) {
            return resultList;
        }
        logger.info(key + " 对应的分片有 " + pair.k.size() + "个");
        pair.v.readLock().lock();
        int index = searchFragment(pair.k, startTime);
        if (index != -1) {
            for (int i = index; i < pair.k.size(); i++) {
                FragmentMeta fragmentMeta = pair.k.get(i);
                resultList.add(fragmentMeta);
                if (endTime != 0 && endTime < fragmentMeta.getEndTime()) {
                    break;
                }
            }
        }
        pair.v.readLock().unlock();
        return resultList;
    }

    @Override
    public List<FragmentMeta> getFragmentListByKey(String key) {
        Pair<List<FragmentMeta>, ReadWriteLock> pair = this.fragmentMap.getOrDefault(key, null);
        if (pair == null) {
            return new ArrayList<>();
        }
        pair.v.readLock().lock();
        List<FragmentMeta> resultList = new ArrayList<>(pair.k);
        pair.v.readLock().unlock();
        return resultList;
    }

    private int searchFragment(List<FragmentMeta> fragmentMetaList, long time) {
        if (fragmentMetaList == null || fragmentMetaList.size() == 0) {
            return -1;
        }
        int left = 0, right = fragmentMetaList.size() - 1;
        if (fragmentMetaList.get(right).getStartTime() <= time) {
            return right;
        }
        while (left != right) {
            int mid = (left + right) / 2;
            FragmentMeta fragmentMeta = fragmentMetaList.get(mid);
            if (fragmentMeta.getStartTime() <= time && fragmentMeta.getEndTime() > time) {
                return mid;
            } else if (fragmentMeta.getStartTime() > time) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return -1;
    }

    @Override
    public FragmentMeta getFragmentListByKeyAndTime(String key, long time) {
        FragmentMeta result = null;
        Pair<List<FragmentMeta>, ReadWriteLock> pair = this.fragmentMap.getOrDefault(key, null);
        if (pair == null) {
            return null;
        }
        pair.v.readLock().lock();
        int index = searchFragment(pair.k, time);
        if (index != -1) {
            result = pair.k.get(index);
        }
        pair.v.readLock().unlock();
        return result;
    }

    @Override
    public boolean createFragment(FragmentMeta fragmentMeta) {
        // 创建分片需要加分布式锁
        String key = fragmentMeta.getKey();
        try {
            // 针对创建分片加分布式锁
            this.zookeeperClient.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(Constants.FRAGMENT_LOCK_NODE + "/" + key);
            // 查询该分片是否已经存在
            FragmentMeta prevFragmentMeta = getFragmentListByKeyAndTime(key, fragmentMeta.getStartTime());
            if (prevFragmentMeta == null) { // 全新的分片
                if (fragmentMeta.getStartTime() != 0 || fragmentMeta.getEndTime() != 0) {
                    throw new IllegalArgumentException("unexpected fragment startTime or endTime");
                }
            } else {
                if (fragmentMeta.getEndTime() != 0 || fragmentMeta.getStartTime() <= prevFragmentMeta.getStartTime()) {
                    throw new IllegalArgumentException("unexpected fragment startTime or endTime");
                }
                prevFragmentMeta = prevFragmentMeta.endFragment(fragmentMeta.getStartTime());
                // 更新前一个分片信息
                this.zookeeperClient.setData()
                        .forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + key + "/" + prevFragmentMeta.getStartTime(), SerializeUtils.serialize(prevFragmentMeta));
            }
            this.zookeeperClient.create()
                    .creatingParentContainersIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(Constants.FRAGMENT_NODE_PREFIX + "/" + key + "/" + fragmentMeta.getStartTime(), SerializeUtils.serialize(fragmentMeta));
            return true;
        } catch (Exception e) {
            logger.error("encounter error when create fragment: ", e);
        } finally {
            try {
                this.zookeeperClient.delete().forPath(Constants.FRAGMENT_LOCK_NODE + "/" + key);
            } catch (Exception e) {
                logger.error("remove fragment lock node error: ", e);
            }
        }
        return false;
    }

    @Override
    public List<Long> chooseDatabaseIdsForNewFragment() {
        List<Long> databaseIds = new ArrayList<>();
        List<DatabaseMeta> databaseMetaList = getDatabaseList().stream().
            sorted(Comparator.comparing(DatabaseMeta::getFragmentReplicaMetaNum)).collect(Collectors.toList());
        int replicaNum = Math.min(1 + ConfigDescriptor.getInstance().getConfig().getReplicaNum(), databaseMetaList.size());
        for (int i = 0; i < replicaNum; i++) {
            databaseIds.add(databaseMetaList.get(i).getId());
        }
        return databaseIds;
    }

    @Override
    public long chooseDatabaseIdForDatabasePlan() {
        List<DatabaseMeta> databaseMetaList = getDatabaseList().stream().
            sorted(Comparator.comparing(DatabaseMeta::getFragmentReplicaMetaNum)).collect(Collectors.toList());
        return databaseMetaList.get(0).getId();
    }

    @Override
    public void shutdown() throws Exception {
        this.iginxCache.close();
        this.fragmentCache.close();
        this.zookeeperClient.close();
        synchronized (MetaManager.class) {
            MetaManager.INSTANCE = null;
        }
    }

    public static MetaManager getInstance() {
        if (INSTANCE == null) {
            synchronized (MetaManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MetaManager();
                }
            }
        }
        return INSTANCE;
    }

}
