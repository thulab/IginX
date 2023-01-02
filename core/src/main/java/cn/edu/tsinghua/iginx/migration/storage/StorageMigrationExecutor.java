package cn.edu.tsinghua.iginx.migration.storage;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.migration.MigrationManager;
import cn.edu.tsinghua.iginx.migration.MigrationPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class StorageMigrationExecutor {

    private final IMetaManager metaManager;

    private final ThreadPoolExecutor executor;

    private static class MigrationTask implements Callable<Boolean>, Runnable {

        private final ThreadPoolExecutor executor;

        private final IMetaManager metaManager;

        private final long storageId;

        public MigrationTask(ThreadPoolExecutor executor, IMetaManager metaManager, long storageId) {
            this.executor = executor;
            this.metaManager = metaManager;
            this.storageId = storageId;
        }

        @Override
        public Boolean call() {
            Map<String, Long> migrationMap = MigrationManager.getInstance().getStorageMigration().generateMigrationPlans(storageId);
            Map<String, String> storageUnitMigrationMap = metaManager.startMigrationStorageUnits(migrationMap);

            List<Callable<Boolean>> tasks = new ArrayList<>();

            for (String sourceStorageUnit: storageUnitMigrationMap.keySet()) {
                String targetStorageUnit = storageUnitMigrationMap.get(sourceStorageUnit);
                MigrationPolicy migrationPolicy = MigrationManager.getInstance().getMigration();
                tasks.add(() -> migrationPolicy.migrationData(sourceStorageUnit, targetStorageUnit));
            }

            try {
                List<Future<Boolean>> futures = executor.invokeAll(tasks);
                for(Future<Boolean> future: futures) {
                    Boolean ret = future.get();
                    if (ret == null || !ret) {
                        return false;
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                return false;
            }
            for (String storageUnitId: migrationMap.keySet()) {
                metaManager.finishMigrationStorageUnit(storageUnitId);
            }
            return true;
        }

        @Override
        public void run() {
            call();
        }
    }

    private StorageMigrationExecutor() {
        metaManager = DefaultMetaManager.getInstance();
        executor = new ThreadPoolExecutor(ConfigDescriptor.getInstance().getConfig().getMigrationThreadPoolSize(),
                Integer.MAX_VALUE,
                60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    }

    public static StorageMigrationExecutor getInstance() {
        return StorageMigrationExecutorHolder.INSTANCE;
    }

    public boolean migration(long storageId, boolean sync) {
        MigrationTask task = new MigrationTask(executor, metaManager, storageId);
        if (sync) {
            return Boolean.TRUE.equals(task.call());
        }
        executor.execute(task);
        return true;
    }

    private static class StorageMigrationExecutorHolder {

        private static final StorageMigrationExecutor INSTANCE = new StorageMigrationExecutor();

        private StorageMigrationExecutorHolder() {
        }

    }

}
