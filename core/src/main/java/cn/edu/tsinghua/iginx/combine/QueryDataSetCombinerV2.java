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
package cn.edu.tsinghua.iginx.combine;

import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.result.QueryDataPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.QueryDataReq;
import cn.edu.tsinghua.iginx.thrift.QueryDataSet;
import cn.edu.tsinghua.iginx.thrift.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class QueryDataSetCombinerV2 {

    private static final Logger logger = LoggerFactory.getLogger(QueryDataSetCombinerV2.class);

    private static final QueryDataSetCombinerV2 instance = new QueryDataSetCombinerV2();

    private QueryDataSetCombinerV2() {}

    public QueryDataReq combineResult(QueryDataReq queryDataReq, List<QueryDataPlanExecuteResult> executeResults, Status status) {
        QueryExecuteDataSet queryDataSet = executeResults.get(0).getQueryExecuteDataSet();
        return null;
    }

    public static QueryDataSetCombinerV2 getInstance() {
        return instance;
    }

}
