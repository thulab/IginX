package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolicyManager {

  private static final Logger logger = LoggerFactory.getLogger(PolicyManager.class);

  private static final PolicyManager instance = new PolicyManager();

  private final Map<String, IPolicy> policies;

  private PolicyManager() {
    this.policies = new HashMap<>();
  }

  public static PolicyManager getInstance() {
    return instance;
  }

  public IPolicy getPolicy(String policyClassName) {
    IPolicy policy;
    synchronized (policies) {
      policy = policies.get(policyClassName);
      if (policy == null) {
        try {
          Class<? extends IPolicy> clazz = (Class<? extends IPolicy>) this.getClass()
              .getClassLoader().loadClass(policyClassName);
          policy = clazz.getConstructor().newInstance();
          policy.init(DefaultMetaManager.getInstance());
          policies.put(policyClassName, policy);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
          logger.error(e.getMessage());
        }
      }
    }
    return policy;
  }
}
