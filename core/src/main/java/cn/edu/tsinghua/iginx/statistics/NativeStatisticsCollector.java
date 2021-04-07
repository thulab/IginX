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
 */package cn.edu.tsinghua.iginx.statistics;

import cn.edu.tsinghua.iginx.core.processor.PostQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PostQueryResultCombineProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryExecuteProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryPlanProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryProcessor;
import cn.edu.tsinghua.iginx.core.processor.PreQueryResultCombineProcessor;

public class NativeStatisticsCollector implements IStatisticsCollector {

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
    public PreQueryProcessor getPreQueryProcessor() {
        return null;
    }

    @Override
    public PreQueryResultCombineProcessor getPreQueryResultCombineProcessor() {
        return null;
    }
}
