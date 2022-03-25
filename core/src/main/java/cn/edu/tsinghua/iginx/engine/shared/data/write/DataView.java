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
package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.thrift.DataType;

public abstract class DataView {

  protected final RawData data;

  protected final int startPathIndex;

  protected final int endPathIndex;

  protected final int startTimeIndex;

  protected final int endTimeIndex;

  public DataView(RawData data, int startPathIndex, int endPathIndex, int startTimeIndex,
      int endTimeIndex) {
    this.data = data;
    this.startPathIndex = startPathIndex;
    this.endPathIndex = endPathIndex;
    this.startTimeIndex = startTimeIndex;
    this.endTimeIndex = endTimeIndex;
  }

  protected void checkPathIndexRange(int index) {
    if (index < 0 || index >= endPathIndex - startPathIndex) {
      throw new IllegalArgumentException(
          String.format("path index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
    }
  }

  protected void checkTypeIndexRange(int index) {
    if (index < 0 || index >= endPathIndex - startPathIndex) {
      throw new IllegalArgumentException(
          String.format("type index out of range [%d, %d)", 0, endPathIndex - startPathIndex));
    }
  }

  protected void checkTimeIndexRange(int index) {
    if (index < 0 || index >= endTimeIndex - startTimeIndex) {
      throw new IllegalArgumentException(
          String.format("time index out of range [%d, %d)", 0, endTimeIndex - startTimeIndex));
    }
  }

  public int getPathNum() {
    return endPathIndex - startPathIndex;
  }

  public int getTimeSize() {
    return endTimeIndex - startTimeIndex;
  }

  public boolean isRowData() {
    return data.isRowData();
  }

  public RawDataType getRawDataType() {
    return data.getType();
  }

  public boolean isColumnData() {
    return data.isColumnData();
  }

  public String getPath(int index) {
    checkPathIndexRange(index);
    return data.getPaths().get(startPathIndex + index);
  }

  public DataType getDataType(int index) {
    checkTypeIndexRange(index);
    return data.getDataTypeList().get(startPathIndex + index);
  }

  public Long getTimestamp(int index) {
    checkTimeIndexRange(index);
    return data.getTimestamps().get(startTimeIndex + index);
  }

  public abstract Object getValue(int index1, int index2);

  public abstract BitmapView getBitmapView(int index);

}
