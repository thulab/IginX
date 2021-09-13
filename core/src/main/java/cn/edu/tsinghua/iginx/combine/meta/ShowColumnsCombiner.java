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
package cn.edu.tsinghua.iginx.combine.meta;

import cn.edu.tsinghua.iginx.query.result.ShowColumnsPlanExecuteResult;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ShowColumnsCombiner {

    private static final Logger logger = LoggerFactory.getLogger(ShowColumnsCombiner.class);

    private static final ShowColumnsCombiner instance = new ShowColumnsCombiner();

    private ShowColumnsCombiner() {
    }

    public static ShowColumnsCombiner getInstance() {
        return instance;
    }

    public void combineResult(ShowColumnsResp resp, List<ShowColumnsPlanExecuteResult> planExecuteResults) {
        Map<String, DataType> columnMap = new HashMap<>();
        for (ShowColumnsPlanExecuteResult planExecuteResult : planExecuteResults) {
            if (planExecuteResult == null || planExecuteResult.getPaths() == null || planExecuteResult.getDataTypes() == null) {
                continue;
            }
            List<String> paths = planExecuteResult.getPaths();
            List<DataType> dataTypes = planExecuteResult.getDataTypes();
            for (int i = 0; i < paths.size(); i++) {
                String path = paths.get(i);
                DataType dataType = dataTypes.get(i);
                // 判断是否已经加进去了
                if (columnMap.containsKey(path)) {
                    if (!columnMap.get(path).equals(dataType)) {
                        logger.error("unexpected dataType " + dataType + " of column " + path);
                    }
                    continue;
                }
                columnMap.put(path, dataType);
            }
        }
        List<String> paths = new ArrayList<>();
        List<DataType> dataTypes = new ArrayList<>();
        for (Map.Entry<String, DataType> entry : columnMap.entrySet()) {
            paths.add(entry.getKey());
            dataTypes.add(entry.getValue());
        }
        resp.setDataTypeList(dataTypes);
        resp.setPaths(paths);
    }

}
