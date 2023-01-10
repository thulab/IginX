package cn.edu.tsinghua.iginx.migration;

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import java.util.HashMap;
import java.util.Map;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import cn.edu.tsinghua.iginx.migration.storage.GreedyStorageMigrationPolicy;
import cn.edu.tsinghua.iginx.migration.storage.StorageMigrationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

  private static final MigrationManager instance = new MigrationManager();

  private final Map<String, MigrationPolicy> policies;

  private MigrationManager() {
    this.policies = new HashMap<>();
  }

  public static MigrationManager getInstance() {
    return instance;
  }

  public MigrationPolicy getMigration() {
    String policyClassName = ConfigDescriptor.getInstance().getConfig()
        .getMigrationPolicyClassName();
    MigrationPolicy policy;
    synchronized (policies) {
      policy = policies.get(policyClassName);
      if (policy == null) {
        try {
          Class<? extends MigrationPolicy> clazz = (Class<? extends MigrationPolicy>) this
              .getClass().getClassLoader().loadClass(policyClassName);
          policy = clazz.getConstructor().newInstance();
          policies.put(policyClassName, policy);
        } catch (Exception e) {
          logger.error(e.getMessage());
        }
      }
    }
    return policy;
  }

  public StorageMigrationPolicy getStorageMigration() {
    return new GreedyStorageMigrationPolicy(DefaultMetaManager.getInstance());
  }
}
