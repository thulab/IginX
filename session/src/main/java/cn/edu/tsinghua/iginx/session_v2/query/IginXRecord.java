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

import java.util.Map;

public class IginXRecord {

    private final long key;

    private final IginXHeader header;

    private final Map<String, Object> values;

    public IginXRecord(IginXHeader header, Map<String, Object> values) {
        this.key = 0L;
        this.header = header;
        this.values = values;
    }

    public IginXRecord(long key, IginXHeader header, Map<String, Object> values) {
        this.key = key;
        this.header = header;
        this.values = values;
    }

    public IginXHeader getHeader() {
        return header;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Object getValue(String measurement) {
        return values.get(measurement);
    }

    public boolean hasTimestamp() {
        return header.hasTimestamp();
    }

    public long getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "IginXRecord{" +
                "values=" + values +
                '}';
    }
}
