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
package cn.edu.tsinghua.iginx.core.context;

import cn.edu.tsinghua.iginx.combine.CombineResult;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.query.result.PlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.Status;
import cn.edu.tsinghua.iginx.utils.SnowFlakeUtils;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class RequestContext {

    private long id;

    private long startTime;

    private long endTime;

    private long sessionId;

    private String version;

    private Map<String, Object> extraParams;

    private ContextType type;

    private Status status;

    private List<? extends IginxPlan> iginxPlans;

    private List<PlanExecuteResult> planExecuteResults;

    private CombineResult combineResult;

    public RequestContext() {
        this(0, ContextType.Unknown);
    }

    public RequestContext(long sessionId, ContextType type) {
        this.id = SnowFlakeUtils.getInstance().nextId();
        this.startTime = System.currentTimeMillis();
        this.sessionId = sessionId;
        this.version = null;
        this.type = type;
        this.extraParams = new HashMap<>();
    }

    public Object getExtraParam(String key) {
        return extraParams.getOrDefault(key, null);
    }

    public void setExtraParam(String key, Object value) {
        extraParams.put(key, value);
    }

}
