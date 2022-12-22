package cn.edu.tsinghua.iginx.migration.storage;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

public class GreedyStorageMigrationPolicy extends StorageMigrationPolicy {

    private static final Logger logger = LoggerFactory.getLogger(GreedyStorageMigrationPolicy.class);

    public GreedyStorageMigrationPolicy(IMetaManager metaManager) {
        super(metaManager);
    }

    static class StoragePriority implements Comparable<StoragePriority> {

        long storageId;

        int weight;

        StoragePriority(long storageId, int weight) {
            this.storageId = storageId;
            this.weight = weight;
        }

        @Override
        public int compareTo(StoragePriority o) {
            return weight - o.weight;
        }
    }

    @Override
    public Map<String, Long> generateMigrationPlans(long sourceStorageId) {
        logger.info("[storage migration] decide storage migration for " + sourceStorageId);
        Map<Long, List<StorageUnitMeta>> storageUnitsMap = metaManager.getStorageUnits().stream().filter(e -> !e.isDummy()).collect(Collectors.groupingBy(StorageUnitMeta::getStorageEngineId));
        List<StorageUnitMeta> storageUnits = storageUnitsMap.get(sourceStorageId);

        PriorityQueue<StoragePriority> storagePriorities = new PriorityQueue<>();
        for (long storageId: storageUnitsMap.keySet()) {
            if (storageId == sourceStorageId) {
                continue;
            }
            storagePriorities.add(new StoragePriority(storageId, storageUnitsMap.get(storageId).size()));
        }

        Map<String, Long> migrationMap = new HashMap<>();
        for (StorageUnitMeta storageUnit: storageUnits) {
            StoragePriority priority = storagePriorities.remove();
            migrationMap.put(storageUnit.getId(), priority.storageId);
            priority.weight++;
            storagePriorities.add(priority);
        }
        return migrationMap;
    }
}
