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
package cn.edu.tsinghua.iginx.session_v2.query;

import java.util.Collections;
import java.util.List;

public class IginXTable {

    public static final IginXTable EMPTY_TABLE = new IginXTable(IginXHeader.EMPTY_HEADER, Collections.emptyList());

    private final IginXHeader header;

    private final List<IginXRecord> records;

    public IginXTable(IginXHeader header, List<IginXRecord> records) {
        this.header = header;
        this.records = records;
    }

    public IginXHeader getHeader() {
        return header;
    }

    public List<IginXRecord> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        return "IginXTable{" +
                "header=" + header +
                ", records=" + records +
                '}';
    }
}
