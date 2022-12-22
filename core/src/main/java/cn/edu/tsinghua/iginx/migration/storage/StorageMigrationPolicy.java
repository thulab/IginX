package cn.edu.tsinghua.iginx.migration.storage;

import cn.edu.tsinghua.iginx.metadata.IMetaManager;

import java.util.Map;

public abstract class StorageMigrationPolicy {

    protected final IMetaManager metaManager;

    public StorageMigrationPolicy(IMetaManager metaManager) {
        this.metaManager = metaManager;
    }

    public abstract Map<String, Long> generateMigrationPlans(long storageId);
}
