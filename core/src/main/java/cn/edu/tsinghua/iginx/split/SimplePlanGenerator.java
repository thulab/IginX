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
package cn.edu.tsinghua.iginx.split;

import cn.edu.tsinghua.iginx.core.context.AddColumnsContext;
import cn.edu.tsinghua.iginx.core.context.CreateDatabaseContext;
import cn.edu.tsinghua.iginx.core.context.InsertRecordsContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.CreateDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.InsertRecordsReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesByDataType;

public class SimplePlanGenerator implements IPlanGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SimplePlanGenerator.class);

    private final SimplePlanSplitter planSplitter = new SimplePlanSplitter();

    @Override
    public List<? extends IginxPlan> generateSubPlans(RequestContext requestContext) {
        List<SplitInfo> splitInfoList;
        switch (requestContext.getType()) {
            case InsertRecords:
                InsertRecordsReq insertRecordsReq = ((InsertRecordsContext) requestContext).getReq();
                InsertRecordsPlan insertRecordsPlan = new InsertRecordsPlan(
                        insertRecordsReq.getPaths(),
                        insertRecordsReq.getTimestamps().stream().mapToLong(Long::longValue).toArray(),
                        getValuesByDataType(insertRecordsReq.getValues(), insertRecordsReq.getAttributes()),
                        insertRecordsReq.getAttributes()
                );
                splitInfoList = planSplitter.getSplitResults(insertRecordsPlan);
                return planSplitter.splitInsertRecordsPlan(insertRecordsPlan, splitInfoList);
            case QueryData:
                QueryDataReq queryDataReq = ((QueryDataContext) requestContext).getReq();
                QueryDataPlan queryDataPlan = new QueryDataPlan(
                        queryDataReq.getPaths(),
                        queryDataReq.getStartTime(),
                        queryDataReq.getEndTime()
                );
                splitInfoList = planSplitter.getSplitResults(queryDataPlan);
                return planSplitter.splitQueryDataPlan(queryDataPlan, splitInfoList);
            case AddColumns:
                AddColumnsReq addColumnsReq = ((AddColumnsContext) requestContext).getReq();
                AddColumnsPlan addColumnsPlan = new AddColumnsPlan(
                    addColumnsReq.getPaths(),
                    addColumnsReq.getAttributes()
                );
                splitInfoList = planSplitter.getSplitResults(addColumnsPlan);
                return planSplitter.splitAddColumnsPlan(addColumnsPlan, splitInfoList);
            case CreateDatabase:
                CreateDatabaseReq createDatabaseReq = ((CreateDatabaseContext) requestContext).getReq();
                CreateDatabasePlan createDatabasePlan = new CreateDatabasePlan(
                        createDatabaseReq.getDatabaseName()
                );
                return Collections.singletonList(createDatabasePlan);
            default:
                logger.info("unimplemented method: " + requestContext.getType());
        }
        return null;
    }
}
