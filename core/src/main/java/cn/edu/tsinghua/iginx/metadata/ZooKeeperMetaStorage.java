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
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.utils.JsonUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ZooKeeperMetaStorage implements IMetaStorage {

    private static final Logger logger = LoggerFactory.getLogger(ZooKeeperMetaStorage.class);

    private static final String IGINX_NODE = "/iginx/node";

    private static final String STORAGE_ENGINE_NODE = "/storage/node";

    private static final String STORAGE_UNIT_NODE = "/unit/unit";

    private static final String IGINX_LOCK_NODE = "/lock/iginx";

    private static final String STORAGE_ENGINE_LOCK_NODE = "/lock/storage";

    private static final String FRAGMENT_LOCK_NODE = "/lock/fragment";

    private static final String STORAGE_UNIT_LOCK_NODE = "/lock/unit";

    private static final String SCHEMA_MAPPING_LOCK_NODE = "/lock/schema";

    private static final String STORAGE_ENGINE_NODE_PREFIX = "/storage";

    private static final String IGINX_NODE_PREFIX = "/iginx";

    private static final String FRAGMENT_NODE_PREFIX = "/fragment";

    private static final String STORAGE_UNIT_NODE_PREFIX = "/unit";

    private static final String SCHEMA_MAPPING_PREFIX = "/schema";

    private static ZooKeeperMetaStorage INSTANCE = null;

    private final CuratorFramework client;

    private SchemaMappingChangeHook schemaMappingChangeHook = null;

    protected TreeCache schemaMappingsCache;

    private IginxChangeHook iginxChangeHook = null;

    protected TreeCache iginxCache;

    private StorageChangeHook storageChangeHook = null;

    protected TreeCache storageEngineCache;

    private StorageUnitChangeHook storageUnitChangeHook = null;

    protected TreeCache storageUnitCache;

    private final Lock storageUnitMutexLock = new ReentrantLock();

    private final InterProcessMutex storageUnitMutex;

    private FragmentChangeHook fragmentChangeHook = null;

    protected TreeCache fragmentCache;

    private final Lock fragmentMutexLock = new ReentrantLock();

    private final InterProcessMutex fragmentMutex;

    public ZooKeeperMetaStorage() {
        client = CuratorFrameworkFactory.builder()
                .connectString(ConfigDescriptor.getInstance().getConfig().getZookeeperConnectionString())
                .connectionTimeoutMs(15000)
                .retryPolicy(new RetryForever(1000))
                .build();
        client.start();

        fragmentMutex = new InterProcessMutex(client, FRAGMENT_LOCK_NODE);
        storageUnitMutex = new InterProcessMutex(client, STORAGE_UNIT_LOCK_NODE);
    }

    public static ZooKeeperMetaStorage getInstance() {
        if (INSTANCE == null) {
            synchronized (ZooKeeperMetaStorage.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ZooKeeperMetaStorage();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(client, SCHEMA_MAPPING_LOCK_NODE);
        try {
            mutex.acquire();
            Map<String, Map<String, Integer>> schemaMappings = new HashMap<>();
            if (client.checkExists().forPath(SCHEMA_MAPPING_PREFIX) == null) {
                // 当前还没有数据，创建父节点，然后不需要解析数据
                client.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(SCHEMA_MAPPING_PREFIX);
            } else {
                List<String> schemas = this.client.getChildren()
                        .forPath(SCHEMA_MAPPING_PREFIX);
                for (String schema : schemas) {
                    Map<String, Integer> schemaMapping = JsonUtils.getGson().fromJson(new String(this.client.getData()
                            .forPath(SCHEMA_MAPPING_PREFIX + "/" + schema)), new TypeToken<Map<String, Integer>>() {
                    }.getType());
                    schemaMappings.put(schema, schemaMapping);
                }
            }
            registerSchemaMappingListener();
            return schemaMappings;
        } catch (Exception e) {
            throw new MetaStorageException("get error when load schema mapping", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
            }
        }
    }

    private void registerSchemaMappingListener() throws Exception {
        this.schemaMappingsCache = new TreeCache(client, SCHEMA_MAPPING_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            if (schemaMappingChangeHook == null) {
                return;
            }
            if (event.getData() == null || event.getData().getPath() == null || event.getData().getPath().equals(SCHEMA_MAPPING_PREFIX)) {
                return; // 创建根节点，不必理会
            }
            byte[] data;
            Map<String, Integer> schemaMapping = null;
            String schema = event.getData().getPath().substring(SCHEMA_MAPPING_PREFIX.length());
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    data = event.getData().getData();
                    schemaMapping = JsonUtils.getGson().fromJson(new String(data), new TypeToken<Map<String, Integer>>() {
                    }.getType());
                    break;
                case NODE_REMOVED:
                default:
                    break;
            }
            schemaMappingChangeHook.onChange(schema, schemaMapping);
        };
        this.schemaMappingsCache.getListenable().addListener(listener);
        this.schemaMappingsCache.start();
    }

    @Override
    public void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook) {
        this.schemaMappingChangeHook = hook;
    }

    @Override
    public void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping) throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(this.client, SCHEMA_MAPPING_LOCK_NODE);
        try {
            mutex.acquire();
            if (this.client.checkExists().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema) == null) {
                if (schemaMapping == null) { // 待删除的数据不存在
                    return;
                }
                // 创建新数据
                this.client.create().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
            } else {
                if (schemaMapping == null) { // 待删除的数据存在
                    this.client.delete().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema);
                }
                // 更新数据
                this.client.setData().forPath(SCHEMA_MAPPING_PREFIX + "/" + schema, JsonUtils.toJson(schemaMapping));
            }
        } catch (Exception e) {
            throw new MetaStorageException("get error when update schemaMapping", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
            }
        }
    }

    @Override
    public Map<Long, IginxMeta> loadIginx() throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(client, IGINX_LOCK_NODE);
        try {
            mutex.acquire();
            Map<Long, IginxMeta> iginxMetaMap = new HashMap<>();
            if (client.checkExists().forPath(IGINX_NODE_PREFIX) == null) {
                // 当前还没有数据，创建父节点，然后不需要解析数据
                client.create()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(IGINX_NODE_PREFIX);
            } else {
                List<String> children = client.getChildren()
                        .forPath(IGINX_NODE_PREFIX);
                for (String childName : children) {
                    byte[] data = client.getData()
                            .forPath(IGINX_NODE_PREFIX + "/" + childName);
                    IginxMeta iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
                    if (iginxMeta == null) {
                        logger.error("resolve data from " + IGINX_NODE_PREFIX + "/" + childName + " error");
                        continue;
                    }
                    iginxMetaMap.putIfAbsent(iginxMeta.getId(), iginxMeta);
                }
            }
            registerIginxListener();
            return iginxMetaMap;
        } catch (Exception e) {
            throw new MetaStorageException("get error when load iginx", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
            }
        }
    }

    @Override
    public long registerIginx(IginxMeta iginx) throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(client, IGINX_LOCK_NODE);
        try {
            mutex.acquire();
            String nodeName = this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(IGINX_NODE, "".getBytes(StandardCharsets.UTF_8));
            long id = Long.parseLong(nodeName.substring(IGINX_NODE.length()));
            IginxMeta iginxMeta = new IginxMeta(id, iginx.getIp(),
                    iginx.getPort(), iginx.getExtraParams());
            this.client.setData()
                    .forPath(nodeName, JsonUtils.toJson(iginxMeta));
            return id;
        } catch (Exception e) {
            throw new MetaStorageException("get error when load iginx", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + IGINX_LOCK_NODE, e);
            }
        }
    }

    private void registerIginxListener() throws Exception {
        this.iginxCache = new TreeCache(this.client, IGINX_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            if (iginxChangeHook == null) {
                return;
            }
            byte[] data;
            IginxMeta iginxMeta;
            switch (event.getType()) {
                case NODE_UPDATED:
                    data = event.getData().getData();
                    iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
                    if (iginxMeta != null) {
                        logger.info("new iginx comes to cluster: id = " + iginxMeta.getId() + " ,ip = " + iginxMeta.getIp() + " , port = " + iginxMeta.getPort());
                        iginxChangeHook.onChange(iginxMeta.getId(), iginxMeta);
                    } else {
                        logger.error("resolve iginx meta from zookeeper error");
                    }
                    break;
                case NODE_REMOVED:
                    data = event.getData().getData();
                    String path = event.getData().getPath();
                    logger.info("node " + path + " is removed");
                    if (path.equals(IGINX_NODE_PREFIX)) {
                        // 根节点被删除
                        logger.info("all iginx leave from cluster, iginx shutdown.");
                        System.exit(1);
                        break;
                    }
                    iginxMeta = JsonUtils.fromJson(data, IginxMeta.class);
                    if (iginxMeta != null) {
                        logger.info("iginx leave from cluster: id = " + iginxMeta.getId() + " ,ip = " + iginxMeta.getIp() + " , port = " + iginxMeta.getPort());
                        iginxChangeHook.onChange(iginxMeta.getId(), null);
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

    @Override
    public void registerIginxChangeHook(IginxChangeHook hook) {
        this.iginxChangeHook = hook;
    }

    @Override
    public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> storageEngines) throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(this.client, STORAGE_ENGINE_LOCK_NODE);
        try {
            mutex.acquire();
            if (this.client.checkExists().forPath(STORAGE_ENGINE_NODE_PREFIX) == null) { // 节点不存在，说明还没有别的 iginx 节点写入过元信息
                for (StorageEngineMeta storageEngineMeta : storageEngines) {
                    String nodeName = this.client.create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                            .forPath(STORAGE_ENGINE_NODE, "".getBytes(StandardCharsets.UTF_8));
                    long id = Long.parseLong(nodeName.substring(STORAGE_ENGINE_NODE.length()));
                    storageEngineMeta.setId(id);
                    this.client.setData().forPath(nodeName, JsonUtils.toJson(storageEngineMeta));
                }
            }
            // 注册监听器
            registerStorageEngineListener();
            // 加载数据
            Map<Long, StorageEngineMeta> storageEngineMetaMap = new HashMap<>();
            List<String> children = this.client.getChildren().forPath(STORAGE_ENGINE_NODE_PREFIX);
            for (String childName : children) {
                byte[] data = this.client.getData()
                        .forPath(STORAGE_ENGINE_NODE_PREFIX + "/" + childName);
                StorageEngineMeta storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
                if (storageEngineMeta == null) {
                    logger.error("resolve data from " + STORAGE_ENGINE_NODE_PREFIX + "/" + childName + " error");
                    continue;
                }
                storageEngineMetaMap.putIfAbsent(storageEngineMeta.getId(), storageEngineMeta);
            }
            return storageEngineMetaMap;
        } catch (Exception e) {
            throw new MetaStorageException("get error when load schema mapping", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + STORAGE_ENGINE_LOCK_NODE, e);
            }
        }
    }

    @Override
    public long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(this.client, STORAGE_ENGINE_LOCK_NODE);
        try {
            mutex.acquire();
            String nodeName = this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(STORAGE_ENGINE_NODE, "".getBytes(StandardCharsets.UTF_8));
            long id = Long.parseLong(nodeName.substring(STORAGE_ENGINE_NODE.length()));
            storageEngine.setId(id);
            this.client.setData()
                    .forPath(nodeName, JsonUtils.toJson(storageEngine));
            return id;
        } catch (Exception e) {
            throw new MetaStorageException("get error when add storage engine", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + SCHEMA_MAPPING_LOCK_NODE, e);
            }
        }
    }

    private void registerStorageEngineListener() throws Exception {
        this.storageEngineCache = new TreeCache(this.client, Constants.STORAGE_ENGINE_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            if (storageChangeHook == null) {
                return;
            }
            byte[] data;
            StorageEngineMeta storageEngineMeta;
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    data = event.getData().getData();
                    logger.info("storage engine meta updated " + event.getData().getPath());
                    logger.info("storage engine: " + new String(data));
                    storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
                    if (storageEngineMeta != null) {
                        logger.info("new storage engine comes to cluster: id = " + storageEngineMeta.getId() + " ,ip = " + storageEngineMeta.getIp() + " , port = " + storageEngineMeta.getPort());
                        storageChangeHook.onChange(storageEngineMeta.getId(), storageEngineMeta);
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
                        logger.info("all iginx leave from cluster, iginx exits");
                        System.exit(2);
                        break;
                    }
                    storageEngineMeta = JsonUtils.fromJson(data, StorageEngineMeta.class);
                    if (storageEngineMeta != null) {
                        logger.info("storage engine leave from cluster: id = " + storageEngineMeta.getId() + " ,ip = " + storageEngineMeta.getIp() + " , port = " + storageEngineMeta.getPort());
                        storageChangeHook.onChange(storageEngineMeta.getId(), null);
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

    @Override
    public void registerStorageChangeHook(StorageChangeHook hook) {
        this.storageChangeHook = hook;
    }

    @Override
    public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(this.client, STORAGE_UNIT_LOCK_NODE);
        try {
            mutex.acquire();
            Map<String, StorageUnitMeta> storageUnitMetaMap = new HashMap<>();
            if (this.client.checkExists().forPath(STORAGE_UNIT_NODE_PREFIX) == null) {
                // 当前还没有数据，创建父节点，然后不需要解析数据
                this.client.create().withMode(CreateMode.PERSISTENT).forPath(STORAGE_UNIT_NODE_PREFIX);
            } else {
                List<String> storageUnitIds = this.client.getChildren().forPath(STORAGE_UNIT_NODE_PREFIX);
                for (String storageUnitId : storageUnitIds) {
                    byte[] data = this.client.getData()
                            .forPath(STORAGE_UNIT_NODE_PREFIX + "/" + storageUnitId);
                    StorageUnitMeta storageUnitMeta = JsonUtils.fromJson(data, StorageUnitMeta.class);
                    if (!storageUnitMeta.isMaster()) { // 需要加入到主节点的子节点列表中
                        StorageUnitMeta masterStorageUnitMeta = storageUnitMetaMap.get(storageUnitMeta.getMasterId());
                        if (masterStorageUnitMeta == null) { // 子节点先于主节点加入系统中，不应该发生，报错
                            logger.error("unexpected storage unit " + new String(data) + ", because it does not has a master storage unit");
                        } else {
                            masterStorageUnitMeta.addReplica(storageUnitMeta);
                        }
                    }
                    storageUnitMetaMap.put(storageUnitMeta.getId(), storageUnitMeta);
                }
            }
            registerStorageUnitListener();
            return storageUnitMetaMap;
        } catch (Exception e) {
            throw new MetaStorageException("get error when load storage unit", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + STORAGE_UNIT_LOCK_NODE, e);
            }
        }
    }

    @Override
    public void lockStorageUnit() throws MetaStorageException {
        try {
            storageUnitMutexLock.lock();
            storageUnitMutex.acquire();
        } catch (Exception e) {
            storageUnitMutexLock.unlock();
            throw new MetaStorageException("acquire storage unit mutex error: ", e);
        }
    }

    @Override
    public String addStorageUnit() throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
        try {
            String nodeName = this.client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(STORAGE_UNIT_NODE, "".getBytes(StandardCharsets.UTF_8));
            return nodeName.substring(STORAGE_UNIT_NODE_PREFIX.length() + 1);
        } catch (Exception e) {
            throw new MetaStorageException("add storage unit error: ", e);
        }
    }

    @Override
    public void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
        try {
            this.client.setData()
                    .forPath(STORAGE_UNIT_NODE_PREFIX + "/" + storageUnitMeta.getId(), JsonUtils.toJson(storageUnitMeta));
        } catch (Exception e) {
            throw new MetaStorageException("add storage unit error: ", e);
        }
    }

    @Override
    public void releaseStorageUnit() throws MetaStorageException {
        try {
            storageUnitMutex.release();
        } catch (Exception e) {
            throw new MetaStorageException("release storage mutex error: ", e);
        } finally {
            storageUnitMutexLock.unlock();
        }
    }

    private void registerStorageUnitListener() throws Exception {
        this.storageUnitCache = new TreeCache(this.client, STORAGE_UNIT_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            if (storageUnitChangeHook == null) {
                return;
            }
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                    byte[] data = event.getData().getData();
                    if (event.getData().getPath().equals(STORAGE_UNIT_NODE_PREFIX)) {
                        break;
                    }
                    StorageUnitMeta storageUnitMeta = JsonUtils.fromJson(data, StorageUnitMeta.class);
                    if (storageUnitMeta != null) {
                        logger.info("new storage unit comes to cluster: id = " + storageUnitMeta.getId());
                        storageUnitChangeHook.onChange(storageUnitMeta.getId(), storageUnitMeta);
                    }
                    break;
            }
        };
        this.storageUnitCache.getListenable().addListener(listener);
        this.storageUnitCache.start();
    }

    @Override
    public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {
        this.storageUnitChangeHook = hook;
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException {
        InterProcessMutex mutex = new InterProcessMutex(client, FRAGMENT_LOCK_NODE);
        try {
            mutex.acquire();
            Map<TimeSeriesInterval, List<FragmentMeta>> fragmentListMap = new HashMap<>();
            if (this.client.checkExists().forPath(FRAGMENT_NODE_PREFIX) == null) {
                // 当前还没有数据，创建父节点，然后不需要解析数据
                this.client.create().withMode(CreateMode.PERSISTENT).forPath(FRAGMENT_NODE_PREFIX);
            } else {
                List<String> tsIntervalNames = this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX);
                for (String tsIntervalName : tsIntervalNames) {
                    TimeSeriesInterval fragmentTimeSeries = TimeSeriesInterval.fromString(tsIntervalName);
                    List<FragmentMeta> fragmentMetaList = new ArrayList<>();
                    List<String> timeIntervalNames = this.client.getChildren().forPath(FRAGMENT_NODE_PREFIX + "/" + tsIntervalName);
                    for (String timeIntervalName : timeIntervalNames) {
                        FragmentMeta fragmentMeta = JsonUtils.fromJson(this.client.getData()
                                .forPath(FRAGMENT_NODE_PREFIX + "/" + tsIntervalName + "/" + timeIntervalName), FragmentMeta.class);
                        fragmentMetaList.add(fragmentMeta);
                    }
                    fragmentListMap.put(fragmentTimeSeries, fragmentMetaList);
                }
            }
            registerFragmentListener();
            return fragmentListMap;
        } catch (Exception e) {
            throw new MetaStorageException("get error when update schema mapping", e);
        } finally {
            try {
                mutex.release();
            } catch (Exception e) {
                throw new MetaStorageException("get error when release interprocess lock for " + FRAGMENT_LOCK_NODE, e);
            }
        }
    }

    private void registerFragmentListener() throws Exception {
        this.fragmentCache = new TreeCache(this.client, FRAGMENT_NODE_PREFIX);
        TreeCacheListener listener = (curatorFramework, event) -> {
            if (fragmentChangeHook == null) {
                return;
            }
            byte[] data;
            FragmentMeta fragmentMeta;
            switch (event.getType()) {
                case NODE_UPDATED:
                    data = event.getData().getData();
                    fragmentMeta = JsonUtils.fromJson(data, FragmentMeta.class);
                    if (fragmentMeta != null) {
                        fragmentChangeHook.onChange(false, fragmentMeta);
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
                            fragmentChangeHook.onChange(true, fragmentMeta);
                        } else {
                            logger.error("resolve fragment from zookeeper error");
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
    public void lockFragment() throws MetaStorageException {
        try {
            fragmentMutexLock.lock();
            fragmentMutex.acquire();
        } catch (Exception e) {
            fragmentMutexLock.unlock();
            throw new MetaStorageException("acquire fragment mutex error: ", e);
        }
    }

    @Override
    public void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
        try {
            this.client.setData()
                    .forPath(FRAGMENT_NODE_PREFIX + "/" + fragmentMeta.getIdealTsInterval().toString() + "/" + fragmentMeta.getIdealTimeInterval().toString(), JsonUtils.toJson(fragmentMeta));
        } catch (Exception e) {
            throw new MetaStorageException("get error when update fragment", e);
        }
    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException { // 只在有锁的情况下调用，内部不需要加锁
        try {
            this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                    .forPath(FRAGMENT_NODE_PREFIX + "/" + fragmentMeta.getIdealTsInterval().toString() + "/" + fragmentMeta.getIdealTimeInterval().toString(), JsonUtils.toJson(fragmentMeta));
        } catch (Exception e) {
            throw new MetaStorageException("get error when add fragment", e);
        }
    }

    @Override
    public void releaseFragment() throws MetaStorageException {
        try {
            fragmentMutex.release();
        } catch (Exception e) {
            throw new MetaStorageException("release fragment mutex error: ", e);
        } finally {
            fragmentMutexLock.unlock();
        }
    }

    @Override
    public void registerFragmentChangeHook(FragmentChangeHook hook) {
        this.fragmentChangeHook = hook;
    }
}
