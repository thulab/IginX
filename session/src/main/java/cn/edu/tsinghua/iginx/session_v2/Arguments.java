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

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.TaskType;

import java.util.List;
import java.util.Objects;

public final class Arguments {

    private Arguments() {
    }

    public static void checkUrl(final String url, final String name) throws IllegalArgumentException {
        checkNonEmpty(url, name);
        // TODO: 检查 url 合法性
    }

    public static void checkNonEmpty(final String string, final String name) throws IllegalArgumentException {
        if (string == null || string.isEmpty()) {
            throw new IllegalArgumentException("Expecting a non-empty string for " + name);
        }
    }

    public static void checkNotNull(final Object obj, final String name) throws NullPointerException {
        Objects.requireNonNull(obj, () -> "Expecting a not null reference for " + name);
    }

    public static void checkDataType(final Object value, final DataType dataType, final String name) throws IllegalStateException {
        switch (dataType) {
            case INTEGER:
                if (!(value instanceof Integer)) {
                    throw new IllegalArgumentException("Expecting a integer for " + name);
                }
                break;
            case LONG:
                if (!(value instanceof Long)) {
                    throw new IllegalArgumentException("Expecting a long for " + name);
                }
                break;
            case BOOLEAN:
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("Expecting a boolean for " + name);
                }
                break;
            case FLOAT:
                if (!(value instanceof Float)) {
                    throw new IllegalArgumentException("Expecting a float for " + name);
                }
                break;
            case DOUBLE:
                if (!(value instanceof Double)) {
                    throw new IllegalArgumentException("Expecting a double for " + name);
                }
                break;
            case BINARY:
                if (!(value instanceof byte[])) {
                    throw new IllegalArgumentException("Expecting a byte array for " + name);
                }
                break;
        }
    }

    public static <T> void checkListNonEmpty(final List<T> list, final String name) {
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("Expecting a non-empty list for " + name);
        }
    }

    public static void checkTaskType(TaskType expected, TaskType actual) {
        if (actual != null && !expected.equals(actual)) {
            throw new IllegalArgumentException("Expecting task type: " + expected + ", actual: " + actual);
        }
    }

}
