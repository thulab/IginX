package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.DefaultMetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class PolicyManagerV2 {

    private static final Logger logger = LoggerFactory.getLogger(PolicyManager.class);

    private static final PolicyManagerV2 instance = new PolicyManagerV2();

    private final Map<String, IPolicyV2> policies;

    private PolicyManagerV2() {
        this.policies = new HashMap<>();
    }

    public static PolicyManagerV2 getInstance() {
        return instance;
    }

    public IPolicyV2 getPolicy(String policyClassName) {
        IPolicyV2 policy;
        synchronized (policies) {
            policy = policies.get(policyClassName);
            if (policy == null) {
                try {
                    Class<? extends IPolicyV2> clazz = (Class<? extends IPolicyV2>) this.getClass().getClassLoader().loadClass(policyClassName);
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
