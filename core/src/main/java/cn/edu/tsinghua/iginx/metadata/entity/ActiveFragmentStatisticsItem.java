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

public final class ActiveFragmentStatisticsItem {

    private final TimeSeriesInterval tsInterval; // 当前请求落入该分片的部分的实际的时间序列的范围

    private final TimeInterval timeInterval; // 当前请求落入该分片的部分的实际的时间戳范围

    private final long count; // 当前请求落入该分片部分的点的个数

    public ActiveFragmentStatisticsItem(TimeSeriesInterval tsInterval, TimeInterval timeInterval, long count) {
        this.tsInterval = tsInterval;
        this.timeInterval = timeInterval;
        this.count = count;
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
}
