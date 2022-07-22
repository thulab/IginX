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
package cn.edu.tsinghua.iginx.resource;

import cn.edu.tsinghua.iginx.engine.shared.RequestContext;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class QueryResourceManager {

    private final ConcurrentMap<Long, RequestContext> queries;

    private QueryResourceManager() {
        this.queries = new ConcurrentHashMap<>();
    }

    public static QueryResourceManager getInstance() {
        return QueryManagerHolder.INSTANCE;
    }

    public void registerQuery(long queryId, RequestContext context) {
        queries.put(queryId, context);
    }

    public RequestContext getQuery(long queryId) {
        return queries.get(queryId);
    }

    public void releaseQuery(long queryId) {
        queries.remove(queryId);
    }

    private static class QueryManagerHolder {

        private static final QueryResourceManager INSTANCE = new QueryResourceManager();

    }

}
