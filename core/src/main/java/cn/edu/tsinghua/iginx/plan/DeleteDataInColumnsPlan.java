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

import cn.edu.tsinghua.iginx.metadata.entity.StorageUnitMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.DELETE_DATA_IN_COLUMNS;

public class DeleteDataInColumnsPlan extends DataPlan {

    private static final Logger logger = LoggerFactory.getLogger(DeleteDataInColumnsPlan.class);

    public DeleteDataInColumnsPlan(List<String> paths, long startTime, long endTime, StorageUnitMeta storageUnit) {
        super(false, paths, startTime, endTime, storageUnit);
        this.setIginxPlanType(DELETE_DATA_IN_COLUMNS);
        this.setSync(true);
    }

    public DeleteDataInColumnsPlan(List<String> paths, long startTime, long endTime) {
        this(paths, startTime, endTime, null);
    }

}
