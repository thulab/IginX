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
package cn.edu.tsinghua.iginx.policy.simple;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.processor.*;
import cn.edu.tsinghua.iginx.metadata.IMetaManager;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;
import cn.edu.tsinghua.iginx.metadata.entity.TimeInterval;
import cn.edu.tsinghua.iginx.metadata.entity.TimeSeriesInterval;
import cn.edu.tsinghua.iginx.metadata.hook.StorageEngineChangeHook;
import cn.edu.tsinghua.iginx.policy.IFragmentGenerator;
import cn.edu.tsinghua.iginx.policy.IPlanSplitter;
import cn.edu.tsinghua.iginx.policy.IPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SimplePolicy implements IPolicy {

    protected AtomicBoolean needReAllocate = new AtomicBoolean(false);
    private IPlanSplitter iPlanSplitter;
    private IMetaManager iMetaManager;
    private IFragmentGenerator iFragmentGenerator;
    private FragmentCreator fragmentCreator;
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final Logger logger = LoggerFactory.getLogger(SimplePolicy.class);


    @Override
    public PostQueryExecuteProcessor getPostQueryExecuteProcessor() {
        return null;
    }

    @Override
    public PostQueryPlanProcessor getPostQueryPlanProcessor() {
        return null;
    }

    @Override
    public PostQueryProcessor getPostQueryProcessor() {
        return null;
    }

    @Override
    public PostQueryResultCombineProcessor getPostQueryResultCombineProcessor() {
        return null;
    }

    @Override
    public PreQueryExecuteProcessor getPreQueryExecuteProcessor() {
        return null;
    }

    @Override
    public PreQueryPlanProcessor getPreQueryPlanProcessor() {
        return null;
    }

    @Override
    public PreQueryResultCombineProcessor getPreQueryResultCombineProcessor() {
        return null;
    }

    @Override
    public IPlanSplitter getIPlanSplitter() {
        return this.iPlanSplitter;
    }

    @Override
    public IFragmentGenerator getIFragmentGenerator() {
        return this.iFragmentGenerator;
    }

    @Override
    public void init(IMetaManager iMetaManager) {
        this.iMetaManager = iMetaManager;
        this.iPlanSplitter = new SimplePlanSplitter(this, this.iMetaManager);
        this.iFragmentGenerator = new SimpleFragmentGenerator(this.iMetaManager);
        this.fragmentCreator = new FragmentCreator(this, this.iMetaManager);
        StorageEngineChangeHook hook = getStorageEngineChangeHook();
        if (hook != null) {
            iMetaManager.registerStorageEngineChangeHook(hook);
        }
    }

    @Override
    public StorageEngineChangeHook getStorageEngineChangeHook() {
        return (before, after) -> {
            // 哪台机器加了分片，哪台机器初始化，并且在批量添加的时候只有最后一个存储引擎才会导致扩容发生
            if (before == null && after != null && after.getCreatedBy() == iMetaManager.getIginxId() && after.isLastOfBatch()) {
                needReAllocate.set(true);
            }
            // TODO: 针对节点退出的情况缩容
        };
    }

    public boolean isNeedReAllocate() {
        return needReAllocate.getAndSet(false);
    }

    public void setNeedReAllocate(boolean needReAllocate) {
        this.needReAllocate.set(needReAllocate);
    }

    boolean isFirst = true;

    public boolean checkSuccess(Map<String, Double> timeseriesData) {
        Map<TimeSeriesInterval, FragmentMeta> latestFragments = iMetaManager.getLatestFragmentMap();
        Map<TimeSeriesInterval, Double> fragmentValue =  latestFragments.keySet().stream().collect(
                Collectors.toMap(Function.identity(), e1 -> 0.0, (e1, e2) -> e1)
        );
        timeseriesData.forEach((key, value) -> {
            for (TimeSeriesInterval timeSeriesInterval : fragmentValue.keySet()) {
                if (timeSeriesInterval.isContain(key)) {
                    Double tmp = fragmentValue.get(timeSeriesInterval);
                    fragmentValue.put(timeSeriesInterval, value + tmp);
                }
            }
        });
        List<Double> value = fragmentValue.values().stream().sorted().collect(Collectors.toList());
        int num = 0;
        for (Double v: value) {
            logger.info("fragment value num : {}, value : {}", num ++, v);
        }
        if (value.get(new Double(Math.ceil(value.size() - 1) * 0.9).intValue()) > config.getStorageGroupValueLimit() * 3) {
            return false;
        }

        if (isFirst) {
            isFirst = false;
            return false;
        }
        return true;
    }
}
