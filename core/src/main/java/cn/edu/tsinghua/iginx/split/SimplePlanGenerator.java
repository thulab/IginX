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

import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import cn.edu.tsinghua.iginx.core.context.AddColumnsContext;
import cn.edu.tsinghua.iginx.core.context.CreateDatabaseContext;
import cn.edu.tsinghua.iginx.core.context.DeleteColumnsContext;
import cn.edu.tsinghua.iginx.core.context.DropDatabaseContext;
import cn.edu.tsinghua.iginx.core.context.InsertRecordsContext;
import cn.edu.tsinghua.iginx.core.context.QueryDataContext;
import cn.edu.tsinghua.iginx.core.context.RequestContext;
import cn.edu.tsinghua.iginx.metadata.MetaManager;
import cn.edu.tsinghua.iginx.plan.AddColumnsPlan;
import cn.edu.tsinghua.iginx.plan.CreateDatabasePlan;
import cn.edu.tsinghua.iginx.plan.DeleteColumnsPlan;
import cn.edu.tsinghua.iginx.plan.DropDatabasePlan;
import cn.edu.tsinghua.iginx.plan.IginxPlan;
import cn.edu.tsinghua.iginx.plan.InsertRecordsPlan;
import cn.edu.tsinghua.iginx.plan.QueryDataPlan;
import cn.edu.tsinghua.iginx.policy.IPlanSplitter;
import cn.edu.tsinghua.iginx.policy.PolicyManager;
import cn.edu.tsinghua.iginx.thrift.AddColumnsReq;
import cn.edu.tsinghua.iginx.thrift.CreateDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.DeleteColumnsReq;
import cn.edu.tsinghua.iginx.thrift.DropDatabaseReq;
import cn.edu.tsinghua.iginx.thrift.InsertRecordsReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesListByDataType;

public class SimplePlanGenerator implements IPlanGenerator {

    private static final Logger logger = LoggerFactory.getLogger(SimplePlanGenerator.class);

    private final IPlanSplitter planSplitter = PolicyManager.getInstance()
            .getPolicy(ConfigDescriptor.getInstance().getConfig().getPolicyClassName()).getIPlanSplitter();

    @Override
    public List<? extends IginxPlan> generateSubPlans(RequestContext requestContext) {
        List<SplitInfo> splitInfoList;
        switch (requestContext.getType()) {
            case CreateDatabase:
                CreateDatabaseReq createDatabaseReq = ((CreateDatabaseContext) requestContext).getReq();
                CreateDatabasePlan createDatabasePlan = new CreateDatabasePlan(createDatabaseReq.getDatabaseName());
                createDatabasePlan.setDatabaseId(MetaManager.getInstance().chooseDatabaseIdForDatabasePlan());
                return Collections.singletonList(createDatabasePlan);
            case DropDatabase:
                DropDatabaseReq dropDatabaseReq = ((DropDatabaseContext) requestContext).getReq();
                DropDatabasePlan dropDatabasePlan = new DropDatabasePlan(dropDatabaseReq.getDatabaseName());
                dropDatabasePlan.setDatabaseId(MetaManager.getInstance().chooseDatabaseIdForDatabasePlan());
                return Collections.singletonList(dropDatabasePlan);
            case AddColumns:
                AddColumnsReq addColumnsReq = ((AddColumnsContext) requestContext).getReq();
                AddColumnsPlan addColumnsPlan = new AddColumnsPlan(addColumnsReq.getPaths(), addColumnsReq.getAttributesList());
                splitInfoList = planSplitter.getSplitResults(addColumnsPlan);
                return splitAddColumnsPlan(addColumnsPlan, splitInfoList);
            case DeleteColumns:
                DeleteColumnsReq deleteColumnsReq = ((DeleteColumnsContext) requestContext).getReq();
                DeleteColumnsPlan deleteColumnsPlan = new DeleteColumnsPlan(deleteColumnsReq.getPaths());
                splitInfoList = planSplitter.getSplitResults(deleteColumnsPlan);
                return splitDeleteColumnsPlan(deleteColumnsPlan, splitInfoList);
            case InsertRecords:
                InsertRecordsReq insertRecordsReq = ((InsertRecordsContext) requestContext).getReq();
                InsertRecordsPlan insertRecordsPlan = new InsertRecordsPlan(
                        insertRecordsReq.getPaths(),
                        ByteBuffer.wrap(insertRecordsReq.getTimestamps()).asLongBuffer().array(),
                        getValuesListByDataType(insertRecordsReq.getValuesList(), insertRecordsReq.getDataTypeList()),
                        insertRecordsReq.dataTypeList,
                        insertRecordsReq.getAttributesList()
                );
                splitInfoList = planSplitter.getSplitResults(insertRecordsPlan);
                return splitInsertRecordsPlan(insertRecordsPlan, splitInfoList);
            case DeleteDataInColumns:
                // TODO
                break;
            case QueryData:
                QueryDataReq queryDataReq = ((QueryDataContext) requestContext).getReq();
                QueryDataPlan queryDataPlan = new QueryDataPlan(
                        queryDataReq.getPaths(),
                        queryDataReq.getStartTime(),
                        queryDataReq.getEndTime()
                );
                splitInfoList = planSplitter.getSplitResults(queryDataPlan);
                return splitQueryDataPlan(queryDataPlan, splitInfoList);
            default:
                logger.info("unimplemented method: " + requestContext.getType());
        }
        return null;
    }

    public List<AddColumnsPlan> splitAddColumnsPlan(AddColumnsPlan plan, List<SplitInfo> infoList) {
        List<AddColumnsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            AddColumnsPlan subPlan = new AddColumnsPlan(plan.getPathsByIndexes(info.getPathsIndexes()),
                    plan.getAttributesByIndexes(info.getPathsIndexes()));
            subPlan.setSync(info.getReplica().getReplicaIndex() == 0);
            plans.add(subPlan);
        }

        return plans;
    }

    public List<DeleteColumnsPlan> splitDeleteColumnsPlan(DeleteColumnsPlan plan, List<SplitInfo> infoList) {
        List<DeleteColumnsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            DeleteColumnsPlan subPlan = new DeleteColumnsPlan(plan.getPathsByIndexes(info.getPathsIndexes()));
            subPlan.setSync(info.getReplica().getReplicaIndex() == 0);
            plans.add(subPlan);
        }

        return plans;
    }

    public List<InsertRecordsPlan> splitInsertRecordsPlan(InsertRecordsPlan plan, List<SplitInfo> infoList) {
        List<InsertRecordsPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            Pair<long[], List<Integer>> timestampsAndIndexes = plan.getTimestampsAndIndexesByRange(
                    info.getReplica().getStartTime(), info.getReplica().getEndTime());
            Object[] values = plan.getValuesByIndexes(timestampsAndIndexes.v, info.getPathsIndexes());
            InsertRecordsPlan subPlan = new InsertRecordsPlan(
                    plan.getPathsByIndexes(info.getPathsIndexes()),
                    timestampsAndIndexes.k,
                    values,
                    plan.getDataTypeListByIndexes(info.getPathsIndexes()),
                    plan.getAttributesByIndexes(info.getPathsIndexes()),
                    info.getReplica().getDatabaseId()
            );
            subPlan.setSync(info.getReplica().getReplicaIndex() == 0);
            plans.add(subPlan);
        }

        return plans;
    }

    public List<QueryDataPlan> splitQueryDataPlan(QueryDataPlan plan, List<SplitInfo> infoList) {
        List<QueryDataPlan> plans = new ArrayList<>();

        for (SplitInfo info : infoList) {
            QueryDataPlan subPlan = new QueryDataPlan(plan.getPathsByIndexes(info.getPathsIndexes()),
                    Math.max(plan.getStartTime(), info.getReplica().getStartTime()),
                    Math.min(plan.getEndTime(), info.getReplica().getEndTime()), info.getReplica().getDatabaseId());
            plans.add(subPlan);
        }

        return plans;
    }

}
