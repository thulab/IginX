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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.AggregateType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.ExecuteSqlResp;
import cn.edu.tsinghua.iginx.thrift.SqlType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
import cn.edu.tsinghua.iginx.utils.ByteUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValueFromByteBufferByDataType;

public class SessionExecuteSqlResult {

    private SqlType sqlType;
    private AggregateType aggregateType;
    private long[] timestamps;
    private List<String> paths;
    private List<List<Object>> values;
    private List<DataType> dataTypeList;
    private int replicaNum;

    // Only for mock test
    public SessionExecuteSqlResult(){}

    public SessionExecuteSqlResult(ExecuteSqlResp resp) {
        this.sqlType = resp.getType();
        switch (resp.getType()) {
            case Insert:
            case Delete:
            case AddStorageEngines:
                break;
            case GetReplicaNum:
                this.replicaNum = resp.getReplicaNum();
                break;
            case AggregateQuery:
            case SimpleQuery:
            case DownsampleQuery:
            case ValueFilterQuery:
                constructQueryResult(resp);
        }
    }

    private void constructQueryResult(ExecuteSqlResp resp) {
        this.paths = resp.getPaths();
        this.dataTypeList = resp.getDataTypeList();

        if (resp.timestamps != null) {
            this.timestamps = getLongArrayFromByteBuffer(resp.timestamps);
        }
        if (resp.queryDataSet != null && resp.queryDataSet.timestamps != null) {
            this.timestamps = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
        }

        if (resp.getType() == SqlType.AggregateQuery ||
                resp.getType() == SqlType.DownsampleQuery) {
            this.aggregateType = resp.aggregateType;
        }

        // parse values
        if (resp.getType() == SqlType.AggregateQuery) {
            Object[] aggregateValues = ByteUtils.getValuesByDataType(resp.valuesList, resp.dataTypeList);
            List<Object> aggregateValueList = new ArrayList<>(Arrays.asList(aggregateValues));
            this.values = new ArrayList<>();
            this.values.add(aggregateValueList);
        } else {
            this.values = parseValues(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
        }
    }

    private List<List<Object>> parseValues(List<DataType> dataTypeList, List<ByteBuffer> valuesList, List<ByteBuffer> bitmapList) {
        List<List<Object>> res = new ArrayList<>();
        for (int i = 0; i < valuesList.size(); i++) {
            List<Object> tempValues = new ArrayList<>();
            ByteBuffer valuesBuffer = valuesList.get(i);
            ByteBuffer bitmapBuffer = bitmapList.get(i);
            Bitmap bitmap = new Bitmap(dataTypeList.size(), bitmapBuffer.array());
            for (int j = 0; j < dataTypeList.size(); j++) {
                if (bitmap.get(j)) {
                    tempValues.add(getValueFromByteBufferByDataType(valuesBuffer, dataTypeList.get(j)));
                } else {
                    tempValues.add(null);
                }
            }
            res.add(tempValues);
        }
        return res;
    }

    public void print() {
        System.out.printf("Start to Print %s ResultSets:%n", sqlType.toString());
        System.out.println("--------------------------------");

        if(timestamps != null)
            System.out.print("Time\t");
        for (String path : paths) {
            if (aggregateType == null)
                System.out.print(path + "\t");
            else
                System.out.print(aggregateType.toString() + "(" + path + ")\t");
        }

        System.out.println();

        for (int i = 0; i < values.size(); i++) {
            if(timestamps != null)
                System.out.print(timestamps[i] + "\t");
            List<Object> rowData = values.get(i);
            for (Object rowDatum : rowData) {
                if (rowDatum instanceof byte[]) {
                    System.out.print(new String((byte[]) rowDatum) + "\t");
                } else {
                    System.out.print(rowDatum + "\t");
                }
            }
            System.out.println();
        }

        System.out.println("--------------------------------");
        System.out.println("Printing ResultSets Finished.");
    }

    public boolean needPrint() {
        return sqlType == SqlType.SimpleQuery ||
                sqlType == SqlType.AggregateQuery ||
                sqlType == SqlType.DownsampleQuery ||
                sqlType == SqlType.ValueFilterQuery;
    }

    public SqlType getSqlType() {
        return sqlType;
    }

    public void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    public AggregateType getAggregateType() {
        return aggregateType;
    }

    public void setAggregateType(AggregateType aggregateType) {
        this.aggregateType = aggregateType;
    }

    public long[] getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(long[] timestamps) {
        this.timestamps = timestamps;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public void setValues(List<List<Object>> values) {
        this.values = values;
    }

    public List<DataType> getDataTypeList() {
        return dataTypeList;
    }

    public void setDataTypeList(List<DataType> dataTypeList) {
        this.dataTypeList = dataTypeList;
    }

    public int getReplicaNum() {
        return replicaNum;
    }

    public void setReplicaNum(int replicaNum) {
        this.replicaNum = replicaNum;
    }
}
