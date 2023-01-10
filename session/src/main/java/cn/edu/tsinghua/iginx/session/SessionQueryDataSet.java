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

import cn.edu.tsinghua.iginx.thrift.DownsampleQueryResp;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;
import cn.edu.tsinghua.iginx.thrift.QueryDataResp;
import cn.edu.tsinghua.iginx.thrift.ShowColumnsResp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.*;

public class SessionQueryDataSet {

    private final long[] keys;
    private List<String> paths;
    private List<Map<String, String>> tagsList;
    private List<List<Object>> values;

    public SessionQueryDataSet(LastQueryResp resp) {
        this.paths = resp.getPaths();
        this.tagsList = resp.getTagsList();
        this.keys = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
        this.values = getValuesFromBufferAndBitmaps(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
    }

    public SessionQueryDataSet(ShowColumnsResp resp) {
        this.paths = resp.getPaths();
        this.keys = null;
    }

    public SessionQueryDataSet(QueryDataResp resp) {
        this.paths = resp.getPaths();
        this.tagsList = resp.getTagsList();
        this.keys = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
        this.values = getValuesFromBufferAndBitmaps(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
    }

    public SessionQueryDataSet(DownsampleQueryResp resp) {
        this.paths = resp.getPaths();
        this.tagsList = resp.getTagsList();
        if (resp.queryDataSet != null) {
            this.keys = getLongArrayFromByteBuffer(resp.queryDataSet.timestamps);
            this.values = getValuesFromBufferAndBitmaps(resp.dataTypeList, resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList);
        } else {
            this.keys = new long[0];
            values = new ArrayList<>();
        }
        if (this.paths == null) {
            this.paths = new ArrayList<>();
        }
    }

    public List<String> getPaths() {
        return paths;
    }

    public long[] getKeys() {
        return keys;
    }

    public List<List<Object>> getValues() {
        return values;
    }

    public void print() {
        System.out.println("Start to Print ResultSets:");
        System.out.print("Time\t");
        for (int i = 0; i < paths.size(); i++) {
            System.out.print(paths.get(i) + "\t");
        }
        System.out.println();

        for (int i = 0; i < keys.length; i++) {
            System.out.print(keys[i] + "\t");
            for (int j = 0; j < paths.size(); j++) {
                if (values.get(i).get(j) instanceof byte[]) {
                    System.out.print(new String((byte[]) values.get(i).get(j)) + "\t");
                } else {
                    System.out.print(values.get(i).get(j) + "\t");
                }
            }
            System.out.println();
        }
        System.out.println("Printing ResultSets Finished.");
    }
}
