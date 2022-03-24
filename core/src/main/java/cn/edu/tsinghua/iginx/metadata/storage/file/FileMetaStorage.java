package cn.edu.tsinghua.iginx.metadata.storage.file;

import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.EXECUTING;
import static cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus.NON_RESHARDING;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.exceptions.MetaStorageException;
import cn.edu.tsinghua.iginx.metadata.cache.IMetaCache;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.IginxMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageEngineMeta;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.entity.UserMeta;
import cn.edu.tsinghua.iginx.metadata.hook.*;
import cn.edu.tsinghua.iginx.metadata.storage.IMetaStorage;
import cn.edu.tsinghua.iginx.metadata.utils.JsonUtils;
import cn.edu.tsinghua.iginx.metadata.utils.ReshardStatus;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FileMetaStorage implements IMetaStorage {

    private static final Logger logger = LoggerFactory.getLogger(FileMetaStorage.class);
    private static final String PATH = ConfigDescriptor.getInstance().getConfig().getFileDataDir();
    private static final String STORAGE_META_FILE = "storage.log";
    private static final String SCHEMA_MAPPING_FILE = "schema.log";
    private static final String FRAGMENT_META_FILE = "fragment.log";
    private static final String STORAGE_UNIT_META_FILE = "storage_unit.log";
    private static final String ID_FILE = "id.log";
    private static final String USER_META_FILE = "user.log";
    private static final String RESHRAD_NOTIFICATION_META_FILE = "reshard_notification.log";
    private static final String RESHARD_COUNTER_META_FILE = "reshard_counter.log";
    private static final long ID_INTERVAL = 100000;
    private static final String UPDATE = "update";
    private static final String REMOVE = "remove";
    private static FileMetaStorage INSTANCE = null;
    private final Lock storageUnitLock = new ReentrantLock();

    private final Lock fragmentUnitLock = new ReentrantLock();

    private final Lock reshardNotificationLock = new ReentrantLock();

    private final Lock reshardCounterLock = new ReentrantLock();

    private IginxChangeHook iginxChangeHook = null;

    private SchemaMappingChangeHook schemaMappingChangeHook = null;

    private StorageChangeHook storageChangeHook = null;

    private StorageUnitChangeHook storageUnitChangeHook = null;

    private FragmentChangeHook fragmentChangeHook = null;

    private UserChangeHook userChangeHook = null;

    private ReshardCounterChangeHook reshardCounterChangeHook = null;

    private ReshardStatusChangeHook reshardStatusChangeHook = null;

    private AtomicLong idGenerator = null; // 加载完数据之后赋值

    public FileMetaStorage() {
        try {
            // 创建目录
            if (Files.notExists(Paths.get(PATH))) {
                Files.createDirectories(Paths.get(PATH));
            }
            // 初始化文件
            if (Files.notExists(Paths.get(PATH, STORAGE_META_FILE))) {
                Files.createFile(Paths.get(PATH, STORAGE_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, SCHEMA_MAPPING_FILE))) {
                Files.createFile(Paths.get(PATH, SCHEMA_MAPPING_FILE));
            }
            if (Files.notExists(Paths.get(PATH, FRAGMENT_META_FILE))) {
                Files.createFile(Paths.get(PATH, FRAGMENT_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, STORAGE_UNIT_META_FILE))) {
                Files.createFile(Paths.get(PATH, STORAGE_UNIT_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, USER_META_FILE))) {
                Files.createFile(Paths.get(PATH, USER_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, RESHRAD_NOTIFICATION_META_FILE))) {
                Files.createFile(Paths.get(PATH, RESHRAD_NOTIFICATION_META_FILE));
            }
            if (Files.notExists(Paths.get(PATH, RESHARD_COUNTER_META_FILE))) {
                Files.createFile(Paths.get(PATH, RESHARD_COUNTER_META_FILE));
            }
        } catch (IOException e) {
            logger.error("encounter error when create log file: ", e);
            System.exit(10);
        }
        // 加载 id
        try {
            if (Files.notExists(Paths.get(PATH, ID_FILE))) {
                Files.createFile(Paths.get(PATH, ID_FILE));
                try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, ID_FILE).toFile(), false))) {
                    writer.write(ID_INTERVAL + "\n");
                }
                idGenerator = new AtomicLong(0L);
            } else {
                long id = ID_INTERVAL;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, ID_FILE).toFile())))) {
                    String line = reader.readLine().trim();
                    id += Long.parseLong(line);
                    idGenerator = new AtomicLong(Long.parseLong(line));
                }
                try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, ID_FILE).toFile(), false))) {
                    writer.write(id + "\n");
                }
            }
        } catch (IOException e) {
            logger.error("encounter error when process id file: ", e);
            System.exit(10);
        }
    }

    public static FileMetaStorage getInstance() {
        if (INSTANCE == null) {
            synchronized (FileMetaStorage.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FileMetaStorage();
                }
            }
        }
        return INSTANCE;
    }

    private long nextId() {
        long id = idGenerator.incrementAndGet();
        if (id % ID_INTERVAL == 0) {
            try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, ID_FILE).toFile(), false))) {
                writer.write((id + ID_INTERVAL) + "\n");
            } catch (IOException e) {
                logger.error("encounter error when rewrite id file: ", e);
            }
        }
        return id;
    }

    @Override
    public Map<String, Map<String, Integer>> loadSchemaMapping() throws MetaStorageException {
        Map<String, Map<String, Integer>> schemaMappings = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, SCHEMA_MAPPING_FILE).toFile())))) {
            String line;
            String[] params;
            while((line = reader.readLine()) != null) {
                params = line.split(" ");
                String schema = params[1];
                if (params[0].equals(UPDATE)) {
                    Map<String, Integer> schemeMapping = JsonUtils.getGson().fromJson(params[2], new TypeToken<Map<String, Integer>>() {
                    }.getType());
                    schemaMappings.put(schema, schemeMapping);
                } else if (params[0].equals(REMOVE)) {
                    schemaMappings.remove(schema);
                } else {
                    logger.error("unknown log content: " + line);
                }
            }
        } catch (IOException e) {
            logger.error("encounter error when read schema mapping log file: ", e);
            throw new MetaStorageException(e);
        }
        return schemaMappings;
    }

    @Override
    public void registerSchemaMappingChangeHook(SchemaMappingChangeHook hook) {
        if (hook != null) {
            schemaMappingChangeHook = hook;
        }
    }

    @Override
    public void updateSchemaMapping(String schema, Map<String, Integer> schemaMapping) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, SCHEMA_MAPPING_FILE).toFile(), true))) {
            if (schemaMapping == null || schemaMapping.isEmpty()) {
                writer.write(String.format("%s %s\n", REMOVE, schema));
            } else {
                writer.write(String.format("%s %s %s\n", UPDATE, schema, JsonUtils.getGson().toJson(schemaMapping)));
            }
        } catch (IOException e) {
            logger.error("write schema mapping file error: ", e);
            throw new MetaStorageException(e);
        }
        if (schemaMappingChangeHook != null) {
            schemaMappingChangeHook.onChange(schema, schemaMapping);
        }

    }

    @Override
    public Map<Long, IginxMeta> loadIginx() throws MetaStorageException { // 实际上不需要有数据，因为本地文件只支持单个文件
        return new HashMap<>();
    }

    @Override
    public long registerIginx(IginxMeta iginx) throws MetaStorageException { // 唯一的一个 iginx 的 id 始终都为 0
        iginxChangeHook.onChange(0L, iginx);
        return 0L;
    }

    @Override
    public void registerIginxChangeHook(IginxChangeHook hook) {
        if (hook != null) {
            iginxChangeHook = hook;
        }
    }

    @Override
    public Map<Long, StorageEngineMeta> loadStorageEngine(List<StorageEngineMeta> storageEngines) throws MetaStorageException {
        Map<Long, StorageEngineMeta> storageEngineMap = new HashMap<>();

        File storageEngineLogFile = Paths.get(PATH, STORAGE_META_FILE).toFile();
        if (storageEngineLogFile.length() == 0L) { // 是第一次启动
            for (StorageEngineMeta storageEngine : storageEngines) {
                storageEngine.setId(addStorageEngine(storageEngine));
                storageEngineMap.put(storageEngine.getId(), storageEngine);
            }
        } else { // 并非第一次启动
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, STORAGE_META_FILE).toFile())))) {
                String line;
                String[] params;
                while((line = reader.readLine()) != null) {
                    params = line.split(" ");
                    if (params[0].equals(UPDATE)) {
                        StorageEngineMeta storageEngine = JsonUtils.fromJson(params[1].getBytes(StandardCharsets.UTF_8), StorageEngineMeta.class);
                        storageEngineMap.put(storageEngine.getId(), storageEngine);
                    } else {
                        logger.error("unknown log content: " + line);
                    }
                }
            } catch (IOException e) {
                logger.error("encounter error when read schema mapping log file: ", e);
                throw new MetaStorageException(e);
            }
        }

        return storageEngineMap;
    }

    @Override
    public void removeStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {

    }

    @Override
    public long addStorageEngine(StorageEngineMeta storageEngine) throws MetaStorageException {
        long id = nextId();
        storageEngine.setId(id);

        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, STORAGE_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", UPDATE, JsonUtils.getGson().toJson(storageEngine)));
        } catch (IOException e) {
            logger.error("write storage engine file error: ", e);
            throw new MetaStorageException(e);
        }

        if (storageChangeHook != null) {
            storageChangeHook.onChange(id, storageEngine);
        }
        return id;
    }

    @Override
    public void registerStorageChangeHook(StorageChangeHook hook) {
        if (hook != null) {
            storageChangeHook = hook;
        }
    }

    @Override
    public Map<String, StorageUnitMeta> loadStorageUnit() throws MetaStorageException {
        Map<String, StorageUnitMeta> storageUnitMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, STORAGE_UNIT_META_FILE).toFile())))) {
            String line;
            String[] params;
            while((line = reader.readLine()) != null) {
                params = line.split(" ");
                if (params[0].equals(UPDATE)) {
                    StorageUnitMeta storageUnit = JsonUtils.getGson().fromJson(params[1], StorageUnitMeta.class);
                    storageUnitMap.put(storageUnit.getId(), storageUnit);
                } else {
                    logger.error("unknown log content: " + line);
                }
            }
        } catch (IOException e) {
            logger.error("encounter error when read storage unit log file: ", e);
            throw new MetaStorageException(e);
        }
        return storageUnitMap;
    }

    @Override
    public void lockStorageUnit() throws MetaStorageException {
        storageUnitLock.lock();
    }

    @Override
    public String addStorageUnit() throws MetaStorageException {
        return "unit" + String.format("%024d", nextId());
    }

    @Override
    public void updateStorageUnit(StorageUnitMeta storageUnitMeta) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, STORAGE_UNIT_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", UPDATE, JsonUtils.getGson().toJson(storageUnitMeta)));
        } catch (IOException e) {
            logger.error("write storage unit file error: ", e);
            throw new MetaStorageException(e);
        }
        if (storageUnitChangeHook != null) {
            storageUnitChangeHook.onChange(storageUnitMeta.getId(), storageUnitMeta);
        }
    }

    @Override
    public void releaseStorageUnit() throws MetaStorageException {
        storageUnitLock.unlock();
    }

    @Override
    public void registerStorageUnitChangeHook(StorageUnitChangeHook hook) {
        if (storageUnitChangeHook != null) {
            storageUnitChangeHook = hook;
        }
    }

    @Override
    public Map<TimeSeriesInterval, List<FragmentMeta>> loadFragment() throws MetaStorageException {
        Map<TimeSeriesInterval, List<FragmentMeta>> fragmentsMap = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, FRAGMENT_META_FILE).toFile())))) {
            String line;
            String[] params;
            while((line = reader.readLine()) != null) {
                params = line.split(" ");
                if (params[0].equals(UPDATE)) {
                    FragmentMeta fragment = JsonUtils.getGson().fromJson(params[1], FragmentMeta.class);
                    List<FragmentMeta> fragmentList = fragmentsMap.computeIfAbsent(fragment.getTsInterval(), e -> new ArrayList<>());
                    fragmentList.remove(fragment);
                    fragmentList.add(fragment);
                } else {
                    logger.error("unknown log content: " + line);
                }
            }
        } catch (IOException e) {
            logger.error("encounter error when read storage unit log file: ", e);
            throw new MetaStorageException(e);
        }
        return fragmentsMap;
    }

    @Override
    public void lockFragment() throws MetaStorageException {
        fragmentUnitLock.lock();
    }

    @Override
    public void updateFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, FRAGMENT_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", UPDATE, JsonUtils.getGson().toJson(fragmentMeta)));
        } catch (IOException e) {
            logger.error("write fragment file error: ", e);
            throw new MetaStorageException(e);
        }
        if (fragmentChangeHook != null) {
            fragmentChangeHook.onChange(false, fragmentMeta);
        }
    }

    @Override
    public void updateFragmentByTsInterval(TimeSeriesInterval tsInterval, FragmentMeta fragmentMeta)
        throws MetaStorageException {

    }

    @Override
    public void addFragment(FragmentMeta fragmentMeta) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, FRAGMENT_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", UPDATE, JsonUtils.getGson().toJson(fragmentMeta)));
        } catch (IOException e) {
            logger.error("write fragment file error: ", e);
            throw new MetaStorageException(e);
        }
        if (fragmentChangeHook != null) {
            fragmentChangeHook.onChange(true, fragmentMeta);
        }
    }

    @Override
    public void releaseFragment() throws MetaStorageException {
        fragmentUnitLock.unlock();
    }

    @Override
    public void registerFragmentChangeHook(FragmentChangeHook hook) {
        if (hook != null) {
            fragmentChangeHook = hook;
        }
    }

    @Override
    public List<UserMeta> loadUser(UserMeta userMeta) throws MetaStorageException {
        Map<String, UserMeta> users = new HashMap<>();

        File userLogFile = Paths.get(PATH, USER_META_FILE).toFile();
        if (userLogFile.length() == 0L) { // 是第一次启动
            addUser(userMeta);
            users.put(userMeta.getUsername(), userMeta);
        } else {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, USER_META_FILE).toFile())))) {
                String line;
                String[] params;
                while((line = reader.readLine()) != null) {
                    params = line.split(" ");
                    if (params[0].equals(UPDATE)) {
                        UserMeta user = JsonUtils.fromJson(params[1].getBytes(StandardCharsets.UTF_8), UserMeta.class);
                        users.put(user.getUsername(), user);
                    } else if (params[0].equals(REMOVE)) {
                        String username = params[1];
                        users.remove(username);
                    } else {
                        logger.error("unknown log content: " + line);
                    }
                }
            } catch (IOException e) {
                logger.error("encounter error when read user log file: ", e);
                throw new MetaStorageException(e);
            }
        }
        return new ArrayList<>(users.values());
    }

    @Override
    public void registerUserChangeHook(UserChangeHook hook) {
        if (hook != null) {
            userChangeHook = hook;
        }
    }

    @Override
    public void addUser(UserMeta userMeta) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, USER_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", UPDATE, JsonUtils.getGson().toJson(userMeta)));
        } catch (IOException e) {
            logger.error("write user file error: ", e);
            throw new MetaStorageException(e);
        }
        if (userChangeHook != null) {
            userChangeHook.onChange(userMeta.getUsername(), userMeta);
        }
    }

    @Override
    public void updateUser(UserMeta userMeta) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, USER_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", UPDATE, JsonUtils.getGson().toJson(userMeta)));
        } catch (IOException e) {
            logger.error("write user file error: ", e);
            throw new MetaStorageException(e);
        }
        if (userChangeHook != null) {
            userChangeHook.onChange(userMeta.getUsername(), userMeta);
        }
    }

    @Override
    public void removeUser(String username) throws MetaStorageException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, USER_META_FILE).toFile(), true))) {
            writer.write(String.format("%s %s\n", REMOVE, username));
        } catch (IOException e) {
            logger.error("write user file error: ", e);
            throw new MetaStorageException(e);
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

    @Override
    public Map<String, Long> loadTimeseriesHeat() throws MetaStorageException, Exception {
        return null;
    }

    @Override
    public void removeTimeseriesHeat() throws MetaStorageException {

    }

    @Override
    public void lockTimeseriesHeatCounter() throws MetaStorageException {

    }

    @Override
    public void incrementTimeseriesHeatCounter() throws MetaStorageException {

    }

    @Override
    public void resetTimeseriesHeatCounter() throws MetaStorageException {

    }

    @Override
    public void releaseTimeseriesHeatCounter() throws MetaStorageException {

    }

    @Override
    public int getTimeseriesHeatCounter() throws MetaStorageException {
        return 0;
    }

    @Override
    public void updateFragmentRequests(Map<FragmentMeta, Long> writeRequestsMap,
        Map<FragmentMeta, Long> readRequestsMap) throws Exception {

    }

  @Override
    public void removeFragmentRequests() throws MetaStorageException {

    }

    @Override
    public void lockFragmentRequestsCounter() throws MetaStorageException {

    }

    @Override
    public void incrementMonitorClearCounter() throws MetaStorageException {

    }

    @Override
    public int getMonitorClearCounter() throws MetaStorageException {
        return 0;
    }

    @Override
    public void resetMonitorClearCounter() throws MetaStorageException {

    }

    @Override
    public void incrementFragmentRequestsCounter() throws MetaStorageException {

    }

    @Override
    public void resetFragmentRequestsCounter() throws MetaStorageException {

    }

    @Override
    public void releaseFragmentRequestsCounter() throws MetaStorageException {

    }

    @Override
    public int getFragmentRequestsCounter() throws MetaStorageException {
        return 0;
    }

    @Override
    public Map<FragmentMeta, Long> loadFragmentPoints(
        IMetaCache cache) throws Exception {
        return null;
    }

    @Override
    public void deleteFragmentPoints(TimeSeriesInterval tsInterval, TimeInterval timeInterval)
        throws Exception {

    }

    @Override
    public void updateFragmentPoints(FragmentMeta fragmentMeta, long points) throws Exception {

    }

    @Override
    public void updateFragmentHeat(Map<FragmentMeta, Long> writeHotspotMap,
        Map<FragmentMeta, Long> readHotspotMap) throws Exception {

    }

    @Override
    public Pair<Map<FragmentMeta, Long>, Map<FragmentMeta, Long>> loadFragmentHeat(
        IMetaCache cache) {
        return null;
    }

    @Override
    public void removeFragmentHeat() throws MetaStorageException {

    }

    @Override
    public void lockFragmentHeatCounter() throws MetaStorageException {

    }

    @Override
    public void incrementFragmentHeatCounter() throws MetaStorageException {

    }

    @Override
    public void resetFragmentHeatCounter() throws MetaStorageException {

    }

    @Override
    public void releaseFragmentHeatCounter() throws MetaStorageException {

    }

    @Override
    public int getFragmentHeatCounter() throws MetaStorageException {
        return 0;
    }

    @Override
    public boolean proposeToReshard() throws MetaStorageException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(Paths.get(PATH, RESHRAD_NOTIFICATION_META_FILE).toFile())))) {
            String line = reader.readLine();
            ReshardStatus status = JsonUtils.getGson().fromJson(line, ReshardStatus.class);
            if (line == null || status.equals(NON_RESHARDING)) {
                if (reshardStatusChangeHook != null) {
                    reshardStatusChangeHook.onChange(EXECUTING);
                }
                updateReshardStatus(EXECUTING);
                return true;
            }
        } catch (IOException e) {
            logger.error("encounter error when proposing to reshard: ", e);
            throw new MetaStorageException(e);
        }
        return false;
    }

    @Override
    public void lockReshardStatus() throws MetaStorageException {
        reshardNotificationLock.lock();
    }

    @Override
    public void updateReshardStatus(ReshardStatus status) throws MetaStorageException {
        removeReshardStatus();
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, RESHRAD_NOTIFICATION_META_FILE).toFile(), true))) {
            writer.write(String.valueOf(status));
        } catch (IOException e) {
            logger.error("encounter error when writing reshard notification log file: ", e);
            throw new MetaStorageException(e);
        }
        if (reshardStatusChangeHook != null) {
            reshardStatusChangeHook.onChange(status);
        }
    }

    @Override
    public void releaseReshardStatus() throws MetaStorageException {
        reshardNotificationLock.unlock();
    }

    @Override
    public void removeReshardStatus() throws MetaStorageException {
        try {
            Files.deleteIfExists(Paths.get(PATH, RESHRAD_NOTIFICATION_META_FILE));
            Files.createFile(Paths.get(PATH, RESHRAD_NOTIFICATION_META_FILE));
        } catch (IOException e) {
            logger.error("encounter error when removing reshard notification file error: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void registerReshardStatusHook(ReshardStatusChangeHook hook) {
        if (hook != null) {
            reshardStatusChangeHook = hook;
        }
    }

    @Override
    public void lockReshardCounter() throws MetaStorageException {
        reshardCounterLock.lock();
    }

    @Override
    public void incrementReshardCounter() throws MetaStorageException {
        updateReshardCounter(1);
        if (reshardCounterChangeHook != null) {
            reshardCounterChangeHook.onChange(1);
        }
    }

    @Override
    public void resetReshardCounter() throws MetaStorageException {
        updateReshardCounter(0);
    }

    private void updateReshardCounter(int counter) throws MetaStorageException {
        removeReshardStatus();
        try (PrintWriter writer = new PrintWriter(new FileWriter(Paths.get(PATH, RESHARD_COUNTER_META_FILE).toFile(), true))) {
            writer.write(counter);
        } catch (IOException e) {
            logger.error("encounter error when writing reshard counter log file error: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void releaseReshardCounter() throws MetaStorageException {
        reshardCounterLock.unlock();
    }

    @Override
    public void removeReshardCounter() throws MetaStorageException {
        try {
            Files.deleteIfExists(Paths.get(PATH, RESHARD_COUNTER_META_FILE));
            Files.createFile(Paths.get(PATH, RESHARD_COUNTER_META_FILE));
        } catch (IOException e) {
            logger.error("encounter error when removing reshard counter file error: ", e);
            throw new MetaStorageException(e);
        }
    }

    @Override
    public void registerReshardCounterChangeHook(ReshardCounterChangeHook hook) {
        if (hook != null) {
            reshardCounterChangeHook = hook;
        }
    }

    @Override
    public void lockMaxActiveEndTimeStatistics() throws MetaStorageException {

    }

    @Override
    public void addOrUpdateMaxActiveEndTimeStatistics(long endTime)
        throws MetaStorageException {

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
}
