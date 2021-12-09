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
package cn.edu.tsinghua.iginx.session_v2.internal;

import cn.edu.tsinghua.iginx.session_v2.QueryClient;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import org.apache.http.concurrent.Cancellable;

import java.util.List;
import java.util.function.BiConsumer;

public class QueryClientImpl extends AbstractFunctionClient implements QueryClient {

    public QueryClientImpl(IginXClientImpl iginXClient) {
        super(iginXClient);
    }

    @Override
    public <M> List<M> query(Query query, Class<M> measurementType) {
        return null;
    }

    @Override
    public IginXTable query(String query) {
        return null;
    }

    @Override
    public IginXTable query(Query query) {
        return query(query.getQuery());
    }

    @Override
    public void query(String query, BiConsumer<Cancellable, IginXRecord> onNext) {

    }

    @Override
    public void query(Query query, BiConsumer<Cancellable, IginXRecord> onNext) {
        query(query.getQuery(), onNext);
    }

    @Override
    public <M> void query(String query, Class<M> measurementType, BiConsumer<Cancellable, M> onNext) {

    }

    @Override
    public <M> void query(Query query, Class<M> measurementType, BiConsumer<Cancellable, M> onNext) {

    }
}
