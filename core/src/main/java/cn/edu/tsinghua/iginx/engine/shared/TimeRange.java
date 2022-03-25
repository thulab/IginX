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
package cn.edu.tsinghua.iginx.engine.shared;

import java.util.Objects;

public final class TimeRange {

  private final long beginTime;

  private final boolean includeBeginTime;

  private final long endTime;

  private final boolean includeEndTime;

  public TimeRange(long beginTime, long endTime) {
    this(beginTime, true, endTime, false);
  }

  public TimeRange(long beginTime, boolean includeBeginTime, long endTime, boolean includeEndTime) {
    this.beginTime = beginTime;
    this.includeBeginTime = includeBeginTime;
    this.endTime = endTime;
    this.includeEndTime = includeEndTime;
  }

  public long getBeginTime() {
    return beginTime;
  }

  public boolean isIncludeBeginTime() {
    return includeBeginTime;
  }

  public long getEndTime() {
    return endTime;
  }

  public boolean isIncludeEndTime() {
    return includeEndTime;
  }

  public TimeRange copy() {
    return new TimeRange(beginTime, includeBeginTime, endTime, includeEndTime);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeRange range = (TimeRange) o;
    return beginTime == range.beginTime && includeBeginTime == range.includeBeginTime
        && endTime == range.endTime && includeEndTime == range.includeEndTime;
  }

  @Override
  public int hashCode() {
    return Objects.hash(beginTime, includeBeginTime, endTime, includeEndTime);
  }

  public long getActualBeginTime() {
    if (includeBeginTime) {
      return beginTime;
    }
    return beginTime + 1;
  }

  public long getActualEndTime() {
    if (includeEndTime) {
      return endTime;
    }
    return endTime - 1;
  }

}
