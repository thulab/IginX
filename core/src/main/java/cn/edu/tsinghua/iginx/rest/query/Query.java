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
package cn.edu.tsinghua.iginx.rest.query;

import java.util.ArrayList;
import java.util.List;

public class Query {
    private Long startAbsolute;
    private Long endAbsolute;
    private Long cacheTime;
    private String timeZone;
    private List<QueryMetric> queryMetrics = new ArrayList<>();

    public List<QueryMetric> getQueryMetrics() {
        return queryMetrics;
    }

    public void setQueryMetrics(List<QueryMetric> queryMetrics) {
        this.queryMetrics = queryMetrics;
    }

    public Long getCacheTime() {
        return cacheTime;
    }

    public void setCacheTime(Long cacheTime) {
        this.cacheTime = cacheTime;
    }

    public Long getEndAbsolute() {
        return endAbsolute;
    }

    public void setEndAbsolute(Long endAbsolute) {
        this.endAbsolute = endAbsolute;
    }

    public Long getStartAbsolute() {
        return startAbsolute;
    }

    public void setStartAbsolute(Long startAbsolute) {
        this.startAbsolute = startAbsolute;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public void addQueryMetrics(QueryMetric queryMetric) {
        this.queryMetrics.add(queryMetric);
    }

}
