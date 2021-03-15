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
package cn.edu.tsinghua.iginx.combine.querydata;

import cn.edu.tsinghua.iginx.exceptions.ExecutionException;
import cn.edu.tsinghua.iginx.query.entity.QueryExecuteDataSet;
import cn.edu.tsinghua.iginx.query.entity.RowRecord;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.List;

class QueryExecuteDataSetWrapper {

    private final QueryExecuteDataSet dataSet;

    private RowRecord rowRecord;

    public QueryExecuteDataSetWrapper(QueryExecuteDataSet dataSet) {
        this.dataSet = dataSet;
    }

    public List<String> getColumnNames() throws ExecutionException {
        return dataSet.getColumnNames();
    }

    public List<DataType> getColumnTypes() throws ExecutionException {
        return dataSet.getColumnTypes();
    }

    public boolean next() throws ExecutionException {
        rowRecord = null;
        if (dataSet.hasNext()) {
            rowRecord = dataSet.next();
        }
        return rowRecord != null;
    }

    public void close() throws ExecutionException {
        dataSet.close();
    }

    public Object getValue(String columnName) {
        return new Object();
    }

    public long getTimestamp() throws ExecutionException {
        return 0;
    }

    public boolean hasNext() throws ExecutionException {
        return dataSet.hasNext();
    }

}
