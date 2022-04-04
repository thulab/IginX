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
package cn.edu.tsinghua.iginx.timescaledb.entity;

import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.RowFetchException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class TimescaleDBQueryRowStream implements RowStream {

  private final List<ResultSet> resultSets;

  private final long[] currTimestamps;

  private final Object[] currValues;

  private final Header header;

  public TimescaleDBQueryRowStream(List<ResultSet> resultSets, List<Field> fields) {
    this.resultSets = resultSets;
    this.header = new Header(Field.TIME, fields);
    this.currTimestamps = new long[resultSets.size()];
    this.currValues = new Object[resultSets.size()];
    // 默认填充一下timestamp列表
    try {
      for (int i = 0; i < this.currTimestamps.length; i++) {
        ResultSet resultSet = this.resultSets.get(i);
        if (resultSet.next()) {
          this.currTimestamps[i] = resultSet.getTimestamp(1).getTime();
          this.currValues[i] = resultSet.getObject(2);
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      // pass
    }
  }

  @Override
  public Header getHeader() {
    return this.header;
  }

  @Override
  public void close() {
    try {
      for (ResultSet resultSet : resultSets) {
        resultSet.close();
      }
    } catch (SQLException e) {
      // pass
    }
  }

  @Override
  public boolean hasNext() throws PhysicalException {
    for (long currTimestamp : this.currTimestamps) {
      if (currTimestamp != Long.MIN_VALUE) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Row next() throws PhysicalException {
    try {
      long timestamp = Long.MAX_VALUE;
      Object[] values = new Object[this.resultSets.size()];
      for (long currTimestamp : this.currTimestamps) {
        if (currTimestamp != Long.MIN_VALUE) {
          timestamp = Math.min(timestamp, currTimestamp);
        }
      }

      for (int i = 0; i < this.currTimestamps.length; i++) {
        if (this.currTimestamps[i] == timestamp) {
          values[i] = this.currValues[i];
          ResultSet resultSet = this.resultSets.get(i);
          if (resultSet.next()) {
            this.currTimestamps[i] = resultSet.getTimestamp(1).getTime();
            this.currValues[i] = resultSet.getObject(2);
          } else {
            // 值已经取完
            this.currTimestamps[i] = Long.MIN_VALUE;
            this.currValues[i] = null;
          }
        }
      }
      return new Row(header, timestamp, values);
    } catch (SQLException e) {
      throw new RowFetchException(e);
    }
  }
}
