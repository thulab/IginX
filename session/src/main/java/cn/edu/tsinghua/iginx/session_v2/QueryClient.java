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
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import cn.edu.tsinghua.iginx.session_v2.query.IginXTable;
import cn.edu.tsinghua.iginx.session_v2.query.Query;

import java.util.List;
import java.util.function.Consumer;

public interface QueryClient {

    IginXTable query(final Query query) throws IginXException;

    <M> List<M> query(final Query query, final Class<M> measurementType) throws IginXException;

    IginXTable query(final String query) throws IginXException;

    void query(final Query query, final Consumer<IginXRecord> onNext) throws IginXException;

    void query(final String query, final Consumer<IginXRecord> onNext) throws IginXException;

    <M> List<M> query(final String query, final Class<M> measurementType) throws IginXException;

    <M> void query(final String query, final Class<M> measurementType, final Consumer<M> onNext) throws IginXException;

    <M> void query(final Query query, final Class<M> measurementType, final Consumer<M> onNext) throws IginXException;

}
