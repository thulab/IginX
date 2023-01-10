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
package cn.edu.tsinghua.iginx.opentsdb.tools;

import cn.edu.tsinghua.iginx.engine.shared.data.write.BitmapView;
import cn.edu.tsinghua.iginx.engine.shared.data.write.DataView;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class DataViewWrapper {

    private final DataView dataView;

    private final Map<Integer, String> pathCache;

    public DataViewWrapper(DataView dataView) {
        this.dataView = dataView;
        this.pathCache = new HashMap<>();
    }

    public int getPathNum() {
        return dataView.getPathNum();
    }

    public int getTimeSize() {
        return dataView.getTimeSize();
    }

    public String getPath(int index) {
        if (pathCache.containsKey(index)) {
            return pathCache.get(index);
        }
        String path = dataView.getPath(index);
        Map<String, String> tags = dataView.getTags(index);
        if (tags != null && !tags.isEmpty()) {
            TreeMap<String, String> sortedTags = new TreeMap<>(tags);
            StringBuilder pathBuilder = new StringBuilder();
            sortedTags.forEach((tagKey, tagValue) -> {
                pathBuilder.append('.').append(tagKey).append('.').append(tagValue);
            });
            path += pathBuilder.toString();
        }
        pathCache.put(index, path);
        return path;
    }

    public DataType getDataType(int index) {
        return dataView.getDataType(index);
    }

    public Long getTimestamp(int index) {
        return dataView.getKey(index);
    }

    public Object getValue(int index1, int index2) {
        return dataView.getValue(index1, index2);
    }

    public BitmapView getBitmapView(int index) {
        return dataView.getBitmapView(index);
    }

}
