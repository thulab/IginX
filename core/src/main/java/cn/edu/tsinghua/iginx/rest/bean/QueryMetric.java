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
package cn.edu.tsinghua.iginx.rest.bean;

import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregator;
import cn.edu.tsinghua.iginx.rest.query.aggregator.QueryAggregatorFirst;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static cn.edu.tsinghua.iginx.rest.RestUtils.TOPTIEM;

@Data
public class QueryMetric {
    private String name;
    private String pathName;
    private String queryOriPath;
    private Long limit;
    private Map<String, List<String>> tags = new TreeMap<>();
    private List<QueryAggregator> aggregators = new ArrayList<>();
    private Boolean annotation = false;
    private Boolean newAnnotation = false;
    private AnnotationLimit annotationLimit;
    private AnnotationLimit newAnnotationLimit;

    public void setQueryOriPath(String path) {
        queryOriPath = new String(path);
    }

    public void addTag(String key, String value) {
        tags.computeIfAbsent(key, k -> new ArrayList<>());
        tags.get(key).add(value);
    }

    public void addAggregator(QueryAggregator qa) {
        aggregators.add(qa);
    }

    public void addFirstAggregator() {
        QueryAggregator qa;
        qa = new QueryAggregatorFirst();
        qa.setDur(TOPTIEM);
        addAggregator(qa);
    }

    public void addCetagory(String key) {
        if(annotationLimit == null) annotationLimit = new AnnotationLimit();
        annotationLimit.addTag(key);
    }
}
