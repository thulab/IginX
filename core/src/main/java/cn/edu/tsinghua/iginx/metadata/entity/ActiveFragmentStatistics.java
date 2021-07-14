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
package cn.edu.tsinghua.iginx.metadata.entity;

public final class ActiveFragmentStatistics {

    private final TimeSeriesInterval tsInterval; // 序列区间

    private final TimeInterval timeInterval; // 时间区间

    private long count;

    public ActiveFragmentStatistics() {
        this.tsInterval = new TimeSeriesInterval(null, null);
        this.timeInterval = new TimeInterval(-1L, -1L);
        this.count = 0;
    }

    public TimeSeriesInterval getTsInterval() {
        return tsInterval;
    }

    public TimeInterval getTimeInterval() {
        return timeInterval;
    }

    public long getCount() {
        return count;
    }

    public synchronized void updateByItem(ActiveFragmentStatisticsItem item) {
        updateTsInterval(item.getTsInterval());
        updateTimeInterval(item.getTimeInterval());
        updateCount(item.getCount());
    }

    private void updateTsInterval(TimeSeriesInterval tsInterval) {
        if (this.tsInterval.getStartTimeSeries() == null || this.tsInterval.getStartTimeSeries().compareTo(tsInterval.getStartTimeSeries()) > 0) {
            this.tsInterval.setStartTimeSeries(tsInterval.getStartTimeSeries());
        }
        if (this.tsInterval.getEndTimeSeries() == null || this.tsInterval.getEndTimeSeries().compareTo(tsInterval.getEndTimeSeries()) < 0) {
            this.tsInterval.setEndTimeSeries(tsInterval.getEndTimeSeries());
        }
    }

    private void updateTimeInterval(TimeInterval timeInterval) {
        if (this.timeInterval.getStartTime() == -1 || this.timeInterval.getStartTime() > timeInterval.getStartTime()) {
            this.timeInterval.setStartTime(timeInterval.getStartTime());
        }
        if (this.timeInterval.getEndTime() == -1 || this.timeInterval.getEndTime() < timeInterval.getEndTime()) {
            this.timeInterval.setEndTime(timeInterval.getEndTime());
        }
    }

    private void updateCount(long count) {
        this.count += count;
    }


}
