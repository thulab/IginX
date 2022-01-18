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

import cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.thrift.DataType;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class Timeseries {

    private final String path;

    private final DataType dataType;

    public Timeseries(String path, DataType dataType) {
        this.path = path;
        this.dataType = dataType;
    }

    public String getPath() {
        return path;
    }

    public DataType getDataType() {
        return dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Timeseries that = (Timeseries) o;
        return Objects.equals(path, that.path) && dataType == that.dataType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, dataType);
    }

    public static RowStream toRowStream(Collection<Timeseries> timeseries) {
        Header header = new Header(Arrays.asList(new Field("path", DataType.BINARY), new Field("type", DataType.BINARY)));
        List<Row> rows = timeseries.stream().map(e -> new Row(header, new Object[] { e.path.getBytes(), e.dataType.toString().getBytes() })).collect(Collectors.toList());
        return new Table(header, rows);
    }

}
