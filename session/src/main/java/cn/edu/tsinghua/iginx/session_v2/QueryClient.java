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
package cn.edu.tsinghua.iginx.session_v2;

import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.AggregateQuery;
import cn.edu.tsinghua.iginx.session_v2.query.DownsampleQuery;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;
import cn.edu.tsinghua.iginx.session_v2.query.SimpleQuery;
import org.apache.http.concurrent.Cancellable;

import java.util.List;
import java.util.function.BiConsumer;

public interface QueryClient {

    IginXTable simpleQuery(final SimpleQuery query) throws IginXException;

    IginXTable aggregateQuery(final AggregateQuery query) throws IginXException;

    IginXTable downsampleQuery(final DownsampleQuery query) throws IginXException;

    <M> List<M> query(final Query query, final Class<M> measurementType) throws IginXException;

    IginXTable query(final String query) throws IginXException;

    IginXTable query(final Query query) throws IginXException;

    void query(final String query, final BiConsumer<Cancellable, IginXRecord> onNext) throws IginXException;

    void query(final Query query, final BiConsumer<Cancellable, IginXRecord> onNext) throws IginXException;

    <M> void query(final String query, final Class<M> measurementType, final BiConsumer<Cancellable, M> onNext) throws IginXException;

    <M> void query(final Query query, final Class<M> measurementType, final BiConsumer<Cancellable, M> onNext) throws IginXException;

}
