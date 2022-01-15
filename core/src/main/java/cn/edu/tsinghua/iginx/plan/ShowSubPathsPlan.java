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
package cn.edu.tsinghua.iginx.plan;

public class ShowSubPathsPlan extends IginxPlan {

    private final long storageEngineId;

    private final String path;

    public ShowSubPathsPlan(long storageEngineId) {
        this(storageEngineId, null);
    }

    public ShowSubPathsPlan(long storageEngineId, String path) {
        super(true);
        this.setIginxPlanType(IginxPlanType.SHOW_SUB_PATHS);
        this.setSync(true);
        this.path = path;
        this.storageEngineId = storageEngineId;
    }

    @Override
    public long getStorageEngineId() {
        return storageEngineId;
    }

    public String getPath() {
        return path;
    }
}
