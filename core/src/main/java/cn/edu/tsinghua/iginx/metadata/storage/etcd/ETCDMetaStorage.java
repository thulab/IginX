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
package cn.edu.tsinghua.iginx.metadata.storage.etcd;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.entity.*;
import cn.edu.tsinghua.iginx.metadata.hook.*;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ETCDMetaStorage implements IMetaStorage {

    private static final Logger logger = LoggerFactory.getLogger(ETCDMetaStorage.class);

    private static final String IGINX_ID = "/id/iginx/";

    private static final String STORAGE_ID = "/id/storage/";

    private static final String STORAGE_UNIT_ID = "/id/storage_unit/";

    private static final String STORAGE_LOCK = "/lock/storage/";

    private static final String STORAGE_UNIT_LOCK = "/lock/storage_unit/";

    private static final String FRAGMENT_LOCK = "/lock/fragment/";

    private static final String USER_LOCK = "/lock/user/";

    private static final String TRANSFORM_LOCK = "/lock/transform/";

    private static final String SCHEMA_MAPPING_PREFIX = "/schema/";

    private static final String IGINX_PREFIX = "/iginx/";

    private static final String STORAGE_PREFIX = "/storage/";

    private static final String STORAGE_UNIT_PREFIX = "/storage_unit/";

    private static final String FRAGMENT_PREFIX = "/fragment/";

    private static final String USER_PREFIX = "/user/";

    private static final String TRANSFORM_PREFIX = "/transform/";

    private static final long MAX_LOCK_TIME = 30; // 最长锁住 30 秒

    private static final long HEART_BEAT_INTERVAL = 5; // 和 etcd 之间的心跳包的时间间隔

    private static ETCDMetaStorage INSTANCE = null;

    private final Lock storageLeaseLock = new ReentrantLock();
    private final Lock storageUnitLeaseLock = new ReentrantLock();
    private final Lock fragmentLeaseLock = new ReentrantLock();
    private final Lock userLeaseLock = new ReentrantLock();
    private final Lock transformLeaseLock = new ReentrantLock();

    private Client client;

    private Watch.Watcher schemaMappingWatcher;
    private SchemaMappingChangeHook schemaMappingChangeHook = null;
    private Watch.Watcher iginxWatcher;
    private IginxChangeHook iginxChangeHook = null;
    private Watch.Watcher storageWatcher;
    private StorageChangeHook storageChangeHook = null;
    private long storageLease = -1L;
    private Watch.Watcher storageUnitWatcher;
    private StorageUnitChangeHook storageUnitChangeHook = null;
    private long storageUnitLease = -1L;
    private Watch.Watcher fragmentWatcher;
    private FragmentChangeHook fragmentChangeHook = null;
    private long fragmentLease = -1L;
    private Watch.Watcher userWatcher;
    private UserChangeHook userChangeHook = null;
    private long userLease = -1L;
    private Watch.Watcher transformWatcher;
    private TransformChangeHook transformChangeHook = null;
    private long transformLease = -1L;

    public ETCDMetaStorage() {
        client = Client.builder()
            .endpoints(ConfigDescriptor.getInstance().getConfig().getEtcdEndpoints()
                .split(","))
            .build();

        // 注册 schema mapping 的监听
        this.schemaMappingWatcher = client.getWatchClient().watch(ByteSequence.from(SCHEMA_MAPPING_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(SCHEMA_MAPPING_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.schemaMappingChangeHook == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        String schema = event.getKeyValue().getKey().toString(StandardCharsets.UTF_8)
                            .substring(SCHEMA_MAPPING_PREFIX.length());
                        Map<String, Integer> schemaMapping = null;
                        switch (event.getEventType()) {
                            case PUT:
                                schemaMapping = JsonUtils.transform(new String(event.getKeyValue().getValue().getBytes()));
                                break;
                            case DELETE:
                                break;
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                        ETCDMetaStorage.this.schemaMappingChangeHook.onChange(schema, schemaMapping);
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });

        // 注册 iginx 的监听
        this.iginxWatcher = client.getWatchClient().watch(ByteSequence.from(IGINX_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(IGINX_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.iginxChangeHook == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        IginxMeta iginx;
                        switch (event.getEventType()) {
                            case PUT:
                                iginx = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), IginxMeta.class);
                                logger.info("new iginx comes to cluster: id = " + iginx.getId() + " ,ip = " + iginx.getIp() + " , port = " + iginx.getPort());
                                ETCDMetaStorage.this.iginxChangeHook.onChange(iginx.getId(), iginx);
                                break;
                            case DELETE:
                                iginx = JsonUtils.fromJson(event.getPrevKV().getValue().getBytes(), IginxMeta.class);
                                logger.info("iginx leave from cluster: id = " + iginx.getId() + " ,ip = " + iginx.getIp() + " , port = " + iginx.getPort());
                                iginxChangeHook.onChange(iginx.getId(), null);
                                break;
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });

        // 注册 storage 的监听
        this.storageWatcher = client.getWatchClient().watch(ByteSequence.from(STORAGE_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(STORAGE_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.storageWatcher == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        StorageEngineMeta storageEngine;
                        switch (event.getEventType()) {
                            case PUT:
                                storageEngine = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), StorageEngineMeta.class);
                                storageChangeHook.onChange(storageEngine.getId(), storageEngine);
                                break;
                            case DELETE:
                                storageEngine = JsonUtils.fromJson(event.getPrevKV().getValue().getBytes(), StorageEngineMeta.class);
                                storageChangeHook.onChange(storageEngine.getId(), null);
                                break;
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });

        // 注册 storage unit 的监听
        this.storageUnitWatcher = client.getWatchClient().watch(ByteSequence.from(STORAGE_UNIT_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(STORAGE_UNIT_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.storageUnitWatcher == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        StorageUnitMeta storageUnit;
                        switch (event.getEventType()) {
                            case PUT:
                                storageUnit = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), StorageUnitMeta.class);
                                storageUnitChangeHook.onChange(storageUnit.getId(), storageUnit);
                                break;
                            case DELETE:
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });

        // 注册 fragment 的监听
        this.fragmentWatcher = client.getWatchClient().watch(ByteSequence.from(FRAGMENT_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(FRAGMENT_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.fragmentChangeHook == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        FragmentMeta fragment;
                        switch (event.getEventType()) {
                            case PUT:
                                fragment = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), FragmentMeta.class);
                                boolean isCreate = event.getPrevKV().getVersion() == 0; // 上一次如果是 0，意味着就是创建
                                fragmentChangeHook.onChange(isCreate, fragment);
                                break;
                            case DELETE:
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });

        // 注册 user 的监听
        this.userWatcher = client.getWatchClient().watch(ByteSequence.from(USER_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(USER_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.userChangeHook == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        UserMeta userMeta;
                        switch (event.getEventType()) {
                            case PUT:
                                userMeta = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), UserMeta.class);
                                userChangeHook.onChange(userMeta.getUsername(), userMeta);
                                break;
                            case DELETE:
                                userMeta = JsonUtils.fromJson(event.getPrevKV().getValue().getBytes(), UserMeta.class);
                                userChangeHook.onChange(userMeta.getUsername(), null);
                                break;
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });

        // 注册 transform 的监听
        this.transformWatcher = client.getWatchClient().watch(ByteSequence.from(TRANSFORM_PREFIX.getBytes()),
            WatchOption.newBuilder().withPrefix(ByteSequence.from(TRANSFORM_PREFIX.getBytes())).withPrevKV(true).build(),
            new Watch.Listener() {
                @Override
                public void onNext(WatchResponse watchResponse) {
                    if (ETCDMetaStorage.this.transformChangeHook == null) {
                        return;
                    }
                    for (WatchEvent event : watchResponse.getEvents()) {
                        TransformTaskMeta taskMeta;
                        switch (event.getEventType()) {
                            case PUT:
                                taskMeta = JsonUtils.fromJson(event.getKeyValue().getValue().getBytes(), TransformTaskMeta.class);
                                transformChangeHook.onChange(taskMeta.getName(), taskMeta);
                                break;
                            case DELETE:
                                taskMeta = JsonUtils.fromJson(event.getPrevKV().getValue().getBytes(), TransformTaskMeta.class);
                                transformChangeHook.onChange(taskMeta.getName(), null);
                                break;
                            default:
                                logger.error("unexpected watchEvent: " + event.getEventType());
                                break;
                        }
                    }
                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });
    }

    public static ETCDMetaStorage getInstance() {
        if (INSTANCE == null) {
            synchronized (ETCDMetaStorage.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ETCDMetaStorage();
                }
            }
        }
        return INSTANCE;
    }

    private long nextId(String category) throws InterruptedException, ExecutionException {
        return client.getKVClient().put(
            ByteSequence.from(category.getBytes()),
            ByteSequence.EMPTY,
            PutOption.newBuilder().withPrevKV().build())
            .get()
            .getPrevKv()
            .getVersion();
    }


    @Override
    public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
        try {
            Map<String, Map<String, Integer>> schemaMappings = new HashMap<>();
            GetResponse response = this.client.getKVClient()
                .get(ByteSequence.from(SCHEMA_MAPPING_PREFIX.getBytes()),
                    GetOption.newBuilder().withPrefix(ByteSequence.from(SCHEMA_MAPPING_PREFIX.getBytes())).build())
                .get();
            response.getKvs().forEach(e -> {
                String schema = e.getKey().toString(StandardCharsets.UTF_8).substring(SCHEMA_MAPPING_PREFIX.length());
                Map<String, Integer> schemaMapping = JsonUtils.transform(e.getValue().toString(StandardCharsets.UTF_8));
                schemaMappings.put(schema, schemaMapping);
            });
            return schemaMappings;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load schema mapping: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook) {
        this.schemaMappingChangeHook = hook;
    }

    @Override
    public void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping) throws MetaStorageException {
        try {
            if (schemaMapping == null) {
                this.client.getKVClient().delete(ByteSequence.from((SCHEMA_MAPPING_PREFIX + schema).getBytes())).get();
            } else {
                this.client.getKVClient().put(ByteSequence.from((SCHEMA_MAPPING_PREFIX + schema).getBytes()), ByteSequence.from(JsonUtils.toJson(schemaMapping))).get();
            }
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when update schema mapping: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public Map<Long, IginxMeta> loadIginx() throws MetaStorageException {
        try {
            Map<Long, IginxMeta> iginxMap = new HashMap<>();
            GetResponse response = this.client.getKVClient()
                .get(ByteSequence.from(IGINX_PREFIX.getBytes()),
                    GetOption.newBuilder().withPrefix(ByteSequence.from(IGINX_PREFIX.getBytes())).build())
                .get();
            response.getKvs().forEach(e -> {
                String info = new String(e.getValue().getBytes());
                System.out.println(info);
                IginxMeta iginx = JsonUtils.fromJson(e.getValue().getBytes(), IginxMeta.class);
                iginxMap.put(iginx.getId(), iginx);
            });
            return iginxMap;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load schema mapping: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public long registerIginx(IginxMeta iginx) throws MetaStorageException {
        try {
            // 申请一个 id
            long id = nextId(IGINX_ID);
            Lease lease = this.client.getLeaseClient();
            long iginxLeaseId = lease.grant(HEART_BEAT_INTERVAL).get().getID();
            lease.keepAlive(iginxLeaseId, new StreamObserver<LeaseKeepAliveResponse>() {
                @Override
                public void onNext(LeaseKeepAliveResponse leaseKeepAliveResponse) {
                    logger.info("send heart beat to etcd succeed.");
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.error("got error when send heart beat to etcd: ", throwable);
                }

                @Override
                public void onCompleted() {

                }
            });
            iginx = new IginxMeta(id, iginx.getIp(),
                iginx.getPort(), iginx.getExtraParams());
            this.client.getKVClient().put(ByteSequence.from((IGINX_PREFIX + String.format("%07d", id)).getBytes()),
                ByteSequence.from(JsonUtils.toJson(iginx))).get();
            return id;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when register iginx meta: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void registerIginxChangeHook(IginxChangeHook hook) {
        this.iginxChangeHook = hook;
    }

    private void lockStorage() throws MetaStorageException {
        try {
            storageLeaseLock.lock();
            storageLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(STORAGE_LOCK.getBytes()), storageLease);
        } catch (Exception e) {
            storageLeaseLock.unlock();
            throw new MetaStorageException("acquire storage mutex error: ", e);
        }
    }

    private void releaseStorage() throws MetaStorageException {
        try {
            client.getLockClient().unlock(ByteSequence.from(STORAGE_LOCK.getBytes())).get();
            client.getLeaseClient().revoke(storageLease).get();
            storageLease = -1L;
        } catch (Exception e) {
            throw new MetaStorageException("release storage error: ", e);
        } finally {
            storageLeaseLock.unlock();
        }
    }

    @Override
    public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> localStorageEngines) throws MetaStorageException {
        try {
            lockStorage();
            Map<Long, StorageEngineMeta> storageEngines = new HashMap<>();
            GetResponse response = this.client.getKVClient()
                .get(ByteSequence.from(STORAGE_PREFIX.getBytes()),
                    GetOption.newBuilder().withPrefix(ByteSequence.from(STORAGE_PREFIX.getBytes())).build())
                .get();
            if (response.getCount() != 0L) { // 服务器上已经有了，本地的不作数
                response.getKvs().forEach(e -> {
                    StorageEngineMeta storageEngine = JsonUtils.fromJson(e.getValue().getBytes(), StorageEngineMeta.class);
                    storageEngines.put(storageEngine.getId(), storageEngine);
                });
            } else { // 服务器上还没有，将本地的注册到服务器上
                for (StorageEngineMeta storageEngine : localStorageEngines) {
                    long id = nextId(STORAGE_ID); // 给每个数据后端分配 id
                    storageEngine.setId(id);
                    storageEngines.put(storageEngine.getId(), storageEngine);
                    this.client.getKVClient()
                        .put(ByteSequence.from((STORAGE_PREFIX + String.format("%06d", id)).getBytes()),
                            ByteSequence.from(JsonUtils.toJson(storageEngine))).get();
                }
            }
            return storageEngines;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load storage: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (storageLease != -1) {
                releaseStorage();
            }
        }
    }

    @Override
    public long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {
        try {
            lockStorage();
            long id = nextId(STORAGE_ID);
            storageEngine.setId(id);
            this.client.getKVClient()
                .put(ByteSequence.from((STORAGE_PREFIX + String.format("%06d", id)).getBytes()),
                    ByteSequence.from(JsonUtils.toJson(storageEngine))).get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when add storage: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (storageLease != -1) {
                releaseStorage();
            }
        }
        return 0L;
    }

    @Override
    public boolean updateStorageEngine(long storageID, StorageEngineMeta storageEngine) throws MetaStorageException {
        try {
            lockStorage();
            this.client.getKVClient()
                    .put(ByteSequence.from((STORAGE_PREFIX + String.format("%06d", storageID)).getBytes()),
                            ByteSequence.from(JsonUtils.toJson(storageEngine))).get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when add storage: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (storageLease != -1) {
                releaseStorage();
            }
        }
        return true;
    }

    @Override
    public void registerStorageChangeHook(StorageChangeHook hook) {
        this.storageChangeHook = hook;
    }

    @Override
    public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
        try {
            Map<String, StorageUnitMeta> storageUnitMap = new HashMap<>();
            GetResponse response = this.client.getKVClient()
                .get(ByteSequence.from(STORAGE_UNIT_PREFIX.getBytes()),
                    GetOption.newBuilder().withPrefix(ByteSequence.from(STORAGE_UNIT_PREFIX.getBytes())).build())
                .get();
            List<KeyValue> kvs = response.getKvs();
            kvs.sort(Comparator.comparing(e -> e.getKey().toString(StandardCharsets.UTF_8)));
            for (KeyValue kv : kvs) {
                StorageUnitMeta storageUnit = JsonUtils.fromJson(kv.getValue().getBytes(), StorageUnitMeta.class);
                if (!storageUnit.isMaster()) { // 需要加入到主节点的子节点列表中
                    StorageUnitMeta masterStorageUnit = storageUnitMap.get(storageUnit.getMasterId());
                    if (masterStorageUnit == null) { // 子节点先于主节点加入系统中，不应该发生，报错
                        logger.error("unexpected storage unit " + new String(kv.getValue().getBytes()) + ", because it does not has a master storage unit");
                    } else {
                        masterStorageUnit.addReplica(storageUnit);
                    }
                }
                storageUnitMap.put(storageUnit.getId(), storageUnit);
            }
            return storageUnitMap;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load storage unit: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void lockStorageUnit() throws MetaStorageException {
        try {
            storageUnitLeaseLock.lock();
            storageUnitLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(STORAGE_UNIT_LOCK.getBytes()), storageUnitLease).get().getKey();
        } catch (Exception e) {
            storageUnitLeaseLock.unlock();
            throw new MetaStorageException("acquire storage unit mutex error: ", e);
        }
    }

    @Override
    public String addStorageUnit() throws MetaStorageException {
        try {
            return String.format("unit%06d", nextId(STORAGE_UNIT_ID));
        } catch (InterruptedException | ExecutionException e) {
            throw new MetaStorageException("add storage unit error: ", e);
        }
    }

    @Override
    public void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException {
        try {
            client.getKVClient().put(ByteSequence.from((STORAGE_UNIT_PREFIX + storageUnitMeta.getId()).getBytes()),
                ByteSequence.from(JsonUtils.toJson(storageUnitMeta))).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new MetaStorageException("update storage unit error: ", e);
        }
    }

    @Override
    public void releaseStorageUnit() throws MetaStorageException {
        try {
            client.getLockClient().unlock(ByteSequence.from(STORAGE_UNIT_LOCK.getBytes())).get();
            client.getLeaseClient().revoke(storageUnitLease).get();
            storageUnitLease = -1L;
        } catch (Exception e) {
            throw new MetaStorageException("release storage mutex error: ", e);
        } finally {
            storageUnitLeaseLock.unlock();
        }
    }

    @Override
    public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {
        this.storageUnitChangeHook = hook;
    }

    @Override
    public Map<TimeSeriesRange, List<FragmentMeta>> loadFragment() throws MetaStorageException {
        try {
            Map<TimeSeriesRange, List<FragmentMeta>> fragmentsMap = new HashMap<>();
            GetResponse response = this.client.getKVClient()
                .get(ByteSequence.from(FRAGMENT_PREFIX.getBytes()),
                    GetOption.newBuilder().withPrefix(ByteSequence.from(FRAGMENT_PREFIX.getBytes())).build())
                .get();
            for (KeyValue kv : response.getKvs()) {
                FragmentMeta fragment = JsonUtils.fromJson(kv.getValue().getBytes(), FragmentMeta.class);
                fragmentsMap.computeIfAbsent(fragment.getTsInterval(), e -> new ArrayList<>())
                    .add(fragment);
            }
            return fragmentsMap;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load fragments: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void lockFragment() throws MetaStorageException {
        try {
            fragmentLeaseLock.lock();
            fragmentLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(FRAGMENT_LOCK.getBytes()), fragmentLease);
        } catch (Exception e) {
            fragmentLeaseLock.unlock();
            throw new MetaStorageException("acquire fragment mutex error: ", e);
        }
    }

    @Override
    public List<FragmentMeta> getFragmentListByTimeSeriesNameAndTimeInterval(String tsName, TimeInterval timeInterval) {
        try {
            List<FragmentMeta> fragments = new ArrayList<>();
            GetResponse response = this.client.getKVClient()
                    .get(ByteSequence.from(FRAGMENT_PREFIX.getBytes()),
                            GetOption.newBuilder().withPrefix(ByteSequence.from(FRAGMENT_PREFIX.getBytes())).build())
                    .get();
            for (KeyValue kv : response.getKvs()) {
                FragmentMeta fragment = JsonUtils.fromJson(kv.getValue().getBytes(), FragmentMeta.class);
                if (fragment.getTimeInterval().isIntersect(timeInterval) && fragment.getTsInterval().isContain(tsName)) {
                    fragments.add(fragment);
                }
            }
            fragments.sort((o1, o2) -> {
                long s1 = o1.getTimeInterval().getStartTime();
                long s2 = o2.getTimeInterval().getStartTime();
                return Long.compare(s2, s1);
            });
            return fragments;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when get fragments by tsName and timeInterval: ", e);
        }
        return new ArrayList<>();
    }

    @Override
    public Map<TimeSeriesRange, List<FragmentMeta>> getFragmentMapByTimeSeriesIntervalAndTimeInterval(TimeSeriesRange tsInterval, TimeInterval timeInterval) {
        try {
            Map<TimeSeriesRange, List<FragmentMeta>> fragmentsMap = new HashMap<>();
            GetResponse response = this.client.getKVClient()
                    .get(ByteSequence.from(FRAGMENT_PREFIX.getBytes()),
                            GetOption.newBuilder().withPrefix(ByteSequence.from(FRAGMENT_PREFIX.getBytes())).build())
                    .get();
            for (KeyValue kv : response.getKvs()) {
                FragmentMeta fragment = JsonUtils.fromJson(kv.getValue().getBytes(), FragmentMeta.class);
                if (fragment.getTimeInterval().isIntersect(timeInterval) && fragment.getTsInterval().isIntersect(tsInterval)) {
                    fragmentsMap.computeIfAbsent(fragment.getTsInterval(), e -> new ArrayList<>()).add(fragment);
                }
            }
            fragmentsMap.values().forEach(e -> e.sort((o1, o2) -> {
                long s1 = o1.getTimeInterval().getStartTime();
                long s2 = o2.getTimeInterval().getStartTime();
                return Long.compare(s1, s2);
            }));
            return fragmentsMap;
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when get fragments by tsName and timeInterval: ", e);
        }
        return new HashMap<>();
    }

    @Override
    public void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
        try {
            client.getKVClient().put(ByteSequence.from((FRAGMENT_PREFIX + fragmentMeta.getTsInterval().toString() + "/" + fragmentMeta.getTimeInterval().toString()).getBytes()),
                ByteSequence.from(JsonUtils.toJson(fragmentMeta))).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new MetaStorageException("update storage unit error: ", e);
        }
    }

    @Override
    public void updateFragmentByTsInterval(TimeSeriesRange tsInterval, FragmentMeta fragmentMeta)
        throws MetaStorageException {

    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
        updateFragment(fragmentMeta);
    }

    @Override
    public void releaseFragment() throws MetaStorageException {
        try {
            client.getLockClient().unlock(ByteSequence.from(FRAGMENT_LOCK.getBytes())).get();
            client.getLeaseClient().revoke(fragmentLease).get();
            fragmentLease = -1L;
        } catch (Exception e) {
            throw new MetaStorageException("release fragment mutex error: ", e);
        } finally {
            fragmentLeaseLock.unlock();
        }
    }

    @Override
    public void registerFragmentChangeHook(FragmentChangeHook hook) {
        this.fragmentChangeHook = hook;
    }

    private void lockUser() throws MetaStorageException {
        try {
            userLeaseLock.lock();
            userLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(USER_LOCK.getBytes()), userLease);
        } catch (Exception e) {
            userLeaseLock.unlock();
            throw new MetaStorageException("acquire user mutex error: ", e);
        }
    }

    private void releaseUser() throws MetaStorageException {
        try {
            client.getLockClient().unlock(ByteSequence.from(USER_LOCK.getBytes())).get();
            client.getLeaseClient().revoke(userLease).get();
            userLease = -1L;
        } catch (Exception e) {
            throw new MetaStorageException("release user mutex error: ", e);
        } finally {
            userLeaseLock.unlock();
        }
    }

    @Override
    public List<UserMeta> loadUser(UserMeta userMeta) throws MetaStorageException {
        try {
            lockUser();
            Map<String, UserMeta> users = new HashMap<>();
            GetResponse response = this.client.getKVClient().get(ByteSequence.from(USER_PREFIX.getBytes()),
                GetOption.newBuilder().withPrefix(ByteSequence.from(USER_PREFIX.getBytes())).build())
                .get();
            if (response.getCount() != 0L) { // 服务器上已经有了，本地的不作数
                response.getKvs().forEach(e -> {
                    UserMeta user = JsonUtils.fromJson(e.getValue().getBytes(), UserMeta.class);
                    users.put(user.getUsername(), user);
                });
            } else {
                addUser(userMeta);
                users.put(userMeta.getUsername(), userMeta);
            }
            return new ArrayList<>(users.values());
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load user: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (userLease != -1) {
                releaseUser();
            }
        }
    }

    @Override
    public void registerUserChangeHook(UserChangeHook hook) {
        userChangeHook = hook;
    }

    @Override
    public void addUser(UserMeta userMeta) throws MetaStorageException {
        updateUser(userMeta);
    }

    @Override
    public void updateUser(UserMeta userMeta) throws MetaStorageException {
        try {
            lockUser();
            this.client.getKVClient()
                .put(ByteSequence.from((USER_PREFIX + userMeta.getUsername()).getBytes()), ByteSequence.from(JsonUtils.toJson(userMeta))).get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when add/update user: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (userLease != -1) {
                releaseUser();
            }
        }
        if (userChangeHook != null) {
            userChangeHook.onChange(userMeta.getUsername(), userMeta);
        }
    }

    @Override
    public void removeUser(String username) throws MetaStorageException {
        try {
            lockUser();
            this.client.getKVClient()
                .delete(ByteSequence.from((USER_PREFIX + username).getBytes())).get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when remove user: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (userLease != -1) {
                releaseUser();
            }
        }
        if (userChangeHook != null) {
            userChangeHook.onChange(username, null);
        }
    }

    @Override
    public void registerTimeseriesChangeHook(TimeSeriesChangeHook hook) {

    }

    @Override
    public void registerVersionChangeHook(VersionChangeHook hook) {

    }

    @Override
    public boolean election() {
        return false;
    }

    @Override
    public void updateTimeseriesData(Map<String, Double> timeseriesData, long iginxid, long version) throws Exception {

    }

    @Override
    public Map<String, Double> getTimeseriesData() {
        return null;
    }

    @Override
    public void registerPolicy(long iginxId, int num) throws Exception {

    }

    @Override
    public int updateVersion() {
        return 0;
    }

    @Override
    public void updateTimeseriesLoad(Map<String, Long> timeseriesLoadMap) throws Exception {

    }

    private void lockTransform() throws MetaStorageException {
        try {
            transformLeaseLock.lock();
            transformLease = client.getLeaseClient().grant(MAX_LOCK_TIME).get().getID();
            client.getLockClient().lock(ByteSequence.from(TRANSFORM_LOCK.getBytes()), transformLease);
        } catch (Exception e) {
            transformLeaseLock.unlock();
            throw new MetaStorageException("acquire transform mutex error: ", e);
        }
    }

    private void releaseTransform() throws MetaStorageException {
        try {
            client.getLockClient().unlock(ByteSequence.from(TRANSFORM_LOCK.getBytes())).get();
            client.getLeaseClient().revoke(transformLease).get();
            transformLease = -1L;
        } catch (Exception e) {
            throw new MetaStorageException("release user mutex error: ", e);
        } finally {
            transformLeaseLock.unlock();
        }
    }

    @Override
    public void registerTransformChangeHook(TransformChangeHook hook) {
        transformChangeHook = hook;
    }

    @Override
    public List<TransformTaskMeta> loadTransformTask() throws MetaStorageException {
        try {
            lockTransform();
            Map<String, TransformTaskMeta> taskMetaMap = new HashMap<>();
            GetResponse response = this.client.getKVClient().get(ByteSequence.from(TRANSFORM_PREFIX.getBytes()),
                GetOption.newBuilder().withPrefix(ByteSequence.from(TRANSFORM_PREFIX.getBytes())).build())
                .get();
            if (response.getCount() != 0L) {
                response.getKvs().forEach(e -> {
                    TransformTaskMeta taskMeta = JsonUtils.fromJson(e.getValue().getBytes(), TransformTaskMeta.class);
                    taskMetaMap.put(taskMeta.getName(), taskMeta);
                });
            }
            return new ArrayList<>(taskMetaMap.values());
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when load transform: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (transformLease != -1) {
                releaseTransform();
            }
        }
    }

    @Override
    public void addTransformTask(TransformTaskMeta transformTask) throws MetaStorageException {
        updateTransformTask(transformTask);
    }

    @Override
    public void updateTransformTask(TransformTaskMeta transformTask) throws MetaStorageException {
        try {
            lockTransform();
            this.client.getKVClient()
                .put(ByteSequence.from((TRANSFORM_PREFIX + transformTask.getName()).getBytes()), ByteSequence.from(JsonUtils.toJson(transformTask))).get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when add/update transform: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (transformLease != -1) {
                releaseTransform();
            }
        }
        if (transformChangeHook != null) {
            transformChangeHook.onChange(transformTask.getName(), transformTask);
        }
    }

    @Override
    public void dropTransformTask(String name) throws MetaStorageException {
        try {
            lockTransform();
            this.client.getKVClient()
                .delete(ByteSequence.from((TRANSFORM_PREFIX + name).getBytes())).get();
        } catch (ExecutionException | InterruptedException e) {
            logger.error("got error when remove transform: ", e);
            throw new MetaStorageException(e);
        } finally {
            if (transformLease != -1) {
                releaseTransform();
            }
        }
        if (transformChangeHook != null) {
            transformChangeHook.onChange(name, null);
        }
    }

    @Override
    public void lockMaxActiveEndTimeStatistics() throws MetaStorageException {

    }

    @Override
    public void addOrUpdateMaxActiveEndTimeStatistics(long endTime) throws MetaStorageException {

    }

    @Override
    public long getMaxActiveEndTimeStatistics() throws MetaStorageException {
        return 0;
    }

    @Override
    public void releaseMaxActiveEndTimeStatistics() throws MetaStorageException {

    }

    @Override
    public void registerMaxActiveEndTimeStatisticsChangeHook(
        MaxActiveEndTimeStatisticsChangeHook hook) throws MetaStorageException {

    }

    public void close() throws MetaStorageException {
        this.schemaMappingWatcher.close();
        this.schemaMappingWatcher = null;

        this.iginxWatcher.close();
        this.iginxWatcher = null;

        this.storageWatcher.close();
        this.storageWatcher = null;

        this.storageUnitWatcher.close();
        this.storageUnitWatcher = null;

        this.fragmentWatcher.close();
        this.fragmentWatcher = null;

        this.userWatcher.close();
        this.userWatcher = null;

        this.transformWatcher.close();
        this.transformWatcher = null;

        this.client.close();
        this.client = null;
    }

}
