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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.conf.Constants;
import cn.edu.tsinghua.iginx.core.IService;
import cn.edu.tsinghua.iginx.core.db.StorageEngine;
import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.FragmentReplicaMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadatav2.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadatav2.utils.JsonUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractMetaManager implements IMetaManager, IService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMetaManager.class);

    private final CuratorFramework zookeeperClient;

    private TreeCache iginxCache;

    private long iginxId;

    private final Map<Long, IginxMeta> iginxMetaMap = new ConcurrentHashMap<>();

    private TreeCache storageEngineCache;

    private final Map<Long, StorageEngineMeta> storageEngineMetaMap = new ConcurrentHashMap<>();

    private TreeCache fragmentCache;

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

        } catch (Exception e) {
            logger.error("get error when init meta manager: ", e);
            System.exit(1);
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
        List<String> children = this.zookeeperClient.getChildren()
                .forPath(Constants.IGINX_NODE_PREFIX);
        for (String childName: children) {
            byte[] data = this.zookeeperClient.getData()
                    .forPath(Constants.IGINX_NODE_PREFIX + "/" + childName);
            IginxMeta iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
            if (iginxMeta == null) {
                logger.error("resolve data from " + Constants.IGINX_NODE_PREFIX + "/" + childName + " error");
                continue;
            }
            this.iginxMetaMap.put(iginxMeta.getId(), iginxMeta);
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
        this.iginxMetaMap.put(id, iginxMeta);
        this.iginxId = id;
    }

    private void registerStorageEngineMeta() throws Exception {
        InterProcessMutex lock = new InterProcessMutex(this.zookeeperClient, Constants.STORAGE_ENGINE_LOCK_NODE);
        lock.acquire();
        try {
            if (this.zookeeperClient.checkExists().forPath(Constants.STORAGE_ENGINE_NODE_PREFIX) == null) { // 节点不存在，说明还没有别的 iginx 节点写入过元信息
                for (StorageEngineMeta storageEngineMeta: resolveStorageEngineFromConf()) {
                    long id = storageEngineMeta.getId();
                    this.zookeeperClient.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT)
                            .forPath(Constants.STORAGE_ENGINE_NODE_PREFIX + "/" + id, JsonUtils.toJson(storageEngineMeta));
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
            StorageEngineMeta storageEngineMeta;
            switch (event.getType()) {
                case NODE_UPDATED:
                    data = event.getData().getData();
                    storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
                    if (storageEngineMeta != null) {
                        logger.info("new storage engine comes to cluster: id = " + storageEngineMeta.getId() + " ,ip = " + storageEngineMeta.getIp() + " , port = " + storageEngineMeta.getPort());
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
        };
        this.storageEngineCache.getListenable().addListener(listener);
        this.storageEngineCache.start();

    }

    private void resolveStorageEngineFromZooKeeper() throws Exception {

    }

    private List<StorageEngineMeta> resolveStorageEngineFromConf() {
        List<StorageEngineMeta> storageEngineMetaList = new ArrayList<>();
        logger.info("resolve database meta from config");
        String[] storageEngineStrings = ConfigDescriptor.getInstance().getConfig()
                .getStorageEngineList().split(",");
        for (int i = 0; i < storageEngineStrings.length; i++) {
            String[] storageEngineParts = storageEngineStrings[i].split(":");
            String ip = storageEngineParts[0];
            int port = Integer.parseInt(storageEngineParts[1]);
            StorageEngine storageEngine = StorageEngine.fromString(storageEngineParts[2]);
            Map<String, String> extraParams = new HashMap<>();
            for (int j = 3; j < storageEngineParts.length; j++) {
                String[] KAndV = storageEngineParts[j].split("=");
                if (KAndV.length != 2) {
                    logger.error("unexpected database meta info: " + storageEngineStrings[i]);
                    continue;
                }
                extraParams.put(KAndV[0], KAndV[1]);
            }
            storageEngineMetaList.add(new StorageEngineMeta(i, ip, port, extraParams, storageEngine, new ArrayList<>()));
        }
        return storageEngineMetaList;
    }


    @Override
    public boolean addStorageEngine(StorageEngineMeta storageEngineMeta) {
        return false;
    }

    @Override
    public List<StorageEngineMeta> getStorageEngineList() {
        return null;
    }

    @Override
    public List<FragmentReplicaMeta> getFragmentListByDatabase(long databaseId) {
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
    public Map<String, List<FragmentMeta>> getFragmentListByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        return null;
    }

    @Override
    public Map<String, FragmentMeta> getLatestFragmentListByTimeSeriesInterval(TimeSeriesInterval tsInterval) {
        return null;
    }

    @Override
    public Map<String, List<FragmentMeta>> getFragmentListByTimeSeriesIntervalAndTimeInterval(TimeSeriesInterval tsInterval, TimeInterval timeInterval) {
        return null;
    }

    @Override
    public List<FragmentMeta> getFragmentByTimeSeriesName(String tsName) {
        return null;
    }

    @Override
    public FragmentMeta getLatestFragmentByTimeSeriesName(String tsName) {
        return null;
    }

    @Override
    public List<FragmentMeta> getFragmentByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval) {
        return null;
    }

    @Override
    public boolean createFragment(FragmentMeta fragment) {
        return false;
    }

    @Override
    public void shutdown() throws Exception {
        this.iginxCache.close();
        this.storageEngineCache.close();
        this.fragmentCache.close();
    }
}
