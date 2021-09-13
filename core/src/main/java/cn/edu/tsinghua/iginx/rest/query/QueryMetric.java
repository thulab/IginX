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

import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class QueryMetric {
    private String name;
    private Long limit;
    private Map<String, List<String>> tags = new TreeMap();
    private List<QueryAggregator> aggregators = new ArrayList<>();
    private Boolean annotation = false;
    private AnnotationLimit annotationLimit;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Map<String, List<String>> getTags() {
        return tags;
    }

    public void setTags(Map<String, List<String>> tags) {
        this.tags = tags;
    }

    public List<QueryAggregator> getAggregators() {
        return aggregators;
    }

    public void setAggregators(
            List<QueryAggregator> aggregators) {
        this.aggregators = aggregators;
    }

    public void addTag(String key, String value) {
        if (tags.get(key) == null)
            tags.put(key, new ArrayList<>());
        tags.get(key).add(value);
    }

    public void addAggregator(QueryAggregator qa) {
        aggregators.add(qa);
    }

    public Boolean getAnnotation() {
        return annotation;
    }

    public void setAnnotation(Boolean annotation) {
        this.annotation = annotation;
    }

    public AnnotationLimit getAnnotationLimit() {
        return annotationLimit;
    }

    public void setAnnotationLimit(AnnotationLimit annotationLimit) {
        this.annotationLimit = annotationLimit;
    }
}
