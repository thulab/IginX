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
package cn.edu.tsinghua.iginx.engine.physical.storage.domain;

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.TagKVUtils;

import java.util.*;
import java.util.stream.Collectors;

public final class Timeseries {

    private final String path;

    private final Map<String, String> tags;

    private final DataType dataType;

    private String physicalPath = null;

    public Timeseries(String path, DataType dataType) {
        this(path, dataType, null);
    }

    public Timeseries(String path, DataType dataType, Map<String, String> tags) {
        this.path = path;
        this.dataType = dataType;
        this.tags = tags;
    }

    public static RowStream toRowStream(Collection<Timeseries> timeseries) {
        Header header = new Header(Arrays.asList(new Field("path", DataType.BINARY), new Field("type", DataType.BINARY)));
        List<Row> rows = timeseries.stream().map(e -> new Row(header, new Object[]{Field.toFullName(e.path, e.tags).getBytes(), e.dataType.toString().getBytes()})).collect(Collectors.toList());
        return new Table(header, rows);
    }

    public String getPath() {
        return path;
    }

    public String getPhysicalPath() {
        if (physicalPath == null) {
            physicalPath = TagKVUtils.toPhysicalPath(path, tags);
        }
        return physicalPath;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Timeseries that = (Timeseries) o;
        return Objects.equals(path, that.path) && dataType == that.dataType && tags.equals(that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, dataType, tags);
    }

}
