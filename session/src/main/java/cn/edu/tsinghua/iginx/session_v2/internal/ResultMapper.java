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

import cn.edu.tsinghua.iginx.session_v2.Arguments;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResultMapper {

    private static final Logger logger = LoggerFactory.getLogger(ResultMapper.class);

    <T> T toPOJO(final IginXRecord record, final Class<T> clazz) {
        Arguments.checkNotNull(record, "record");
        Arguments.checkNotNull(clazz, "clazz");

        try {
            T pojo = clazz.newInstance();


            return pojo;
        } catch (Exception e) {
            throw new IginXException(e);
        }
    }

    private double toDoubleValue(final Object value) {

        if (double.class.isAssignableFrom(value.getClass()) || Double.class.isAssignableFrom(value.getClass())) {
            return (double) value;
        }

        return (Double) value;
    }

    private long toLongValue(final Object value) {

        if (long.class.isAssignableFrom(value.getClass()) || Long.class.isAssignableFrom(value.getClass())) {
            return (long) value;
        }

        return ((Double) value).longValue();
    }

    private int toIntValue(final Object value) {

        if (int.class.isAssignableFrom(value.getClass()) || Integer.class.isAssignableFrom(value.getClass())) {
            return (int) value;
        }

        return ((Double) value).intValue();
    }


}
