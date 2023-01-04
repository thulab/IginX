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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.write.Point;
import cn.edu.tsinghua.iginx.session_v2.write.Record;
import cn.edu.tsinghua.iginx.session_v2.write.Table;
import cn.edu.tsinghua.iginx.thrift.TimePrecision;

import java.util.List;

public interface AsyncWriteClient extends AutoCloseable {

    void writePoint(final Point point);

    void writePoint(final Point pointm, final TimePrecision timePrecision);

    void writePoints(final List<Point> points);

    void writePoints(final List<Point> points, final TimePrecision timePrecision);

    void writeRecord(final Record record);

    void writeRecord(final Record record, final TimePrecision timePrecision);

    void writeRecords(final List<Record> records);

    void writeRecords(final List<Record> records, final TimePrecision timePrecision);

    <M> void writeMeasurement(final M measurement);

    <M> void writeMeasurement(final M measurement, final TimePrecision timePrecision);

    <M> void writeMeasurements(final List<M> measurements);

    <M> void writeMeasurements(final List<M> measurements, final TimePrecision timePrecision);

    void writeTable(final Table table);

    void writeTable(final Table table, final TimePrecision timePrecision);

    @Override
    void close() throws Exception;
}
