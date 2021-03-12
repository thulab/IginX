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
package cn.edu.tsinghua.iginx.policy;

import cn.edu.tsinghua.iginx.metadata.MetaManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class PolicyManager {

    private static final Logger logger = LoggerFactory.getLogger(PolicyManager.class);

    private static final PolicyManager instance = new PolicyManager();

    private final Map<String, IPolicy> policies;

    private PolicyManager() {
        this.policies = new HashMap<>();
    }

    public IPolicy getPolicy(String policyClassName) {
        IPolicy policy;
        synchronized (policies) {
            policy = policies.get(policyClassName);
            if (policy == null) {
                try {
                    Class<? extends IPolicy> clazz = (Class<? extends IPolicy>) this.getClass().getClassLoader().loadClass(policyClassName);
                    policy = clazz.getConstructor().newInstance();
                    policy.init(MetaManager.getInstance());
                    policies.put(policyClassName, policy);
                } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                    logger.error(e.getMessage());
                }
            }
        }
        return policy;
    }

    public static PolicyManager getInstance() {
        return instance;
    }

}
