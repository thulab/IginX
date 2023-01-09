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
import cn.edu.tsinghua.iginx.session_v2.annotations.Field;
import cn.edu.tsinghua.iginx.session_v2.annotations.Measurement;
import cn.edu.tsinghua.iginx.session_v2.exception.IginXException;
import cn.edu.tsinghua.iginx.session_v2.query.IginXRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ResultMapper {

    private static final Logger logger = LoggerFactory.getLogger(ResultMapper.class);

    <T> T toPOJO(final IginXRecord record, final Class<T> clazz) {
        Arguments.checkNotNull(record, "record");
        Arguments.checkNotNull(clazz, "clazz");

        String measurement = clazz.getName();
        Measurement measurementAnno = clazz.getAnnotation(Measurement.class);
        if (measurementAnno != null) {
            measurement = measurementAnno.name();
        }

        try {
            T pojo = clazz.newInstance();

            Class<?> currentClazz = clazz;

            while (currentClazz != null) {

                java.lang.reflect.Field[] fields = currentClazz.getDeclaredFields();
                for (java.lang.reflect.Field field: fields) {
                    Field anno = field.getAnnotation(Field.class);
                    String fieldName = field.getName();

                    if (anno != null && anno.timestamp()) {
                        setFieldValue(pojo, field, record.getKey());
                        continue;
                    }

                    if (anno != null && !anno.name().isEmpty()) {
                        fieldName = anno.name();
                    }

                    if (!measurement.isEmpty()) {
                        fieldName = measurement + "." + fieldName;
                    }

                    Map<String, Object> recordValues = record.getValues();
                    if (recordValues.containsKey(fieldName)) {
                        Object value = recordValues.get(fieldName);
                        setFieldValue(pojo, field, value);
                    }
                }

                currentClazz = currentClazz.getSuperclass();
            }

            return pojo;
        } catch (Exception e) {
            throw new IginXException(e);
        }
    }

    private void setFieldValue(final Object object,
                               final java.lang.reflect.Field field,
                               final Object value) {
        if (field == null || value == null) {
            return;
        }
        String msg =
                "Class '%s' field '%s' was defined with a different field type and caused a ClassCastException. "
                        + "The correct type is '%s' (current field value: '%s').";

        try {
            if (!field.isAccessible()) {
                field.setAccessible(true);
            }
            Class<?> fieldType = field.getType();

            if (fieldType.equals(value.getClass())) {
                field.set(object, value);
                return;
            }
            if (double.class.isAssignableFrom(fieldType)) {
                field.setDouble(object, (double) value);
                return;
            }
            if (float.class.isAssignableFrom(fieldType)) {
                field.setFloat(object, (float) value);
                return;
            }
            if (long.class.isAssignableFrom(fieldType)) {
                field.setLong(object, (long) value);
                return;
            }
            if (int.class.isAssignableFrom(fieldType)) {
                field.setInt(object, (int) value);
                return;
            }
            if (boolean.class.isAssignableFrom(fieldType)) {
                field.setBoolean(object, Boolean.parseBoolean(String.valueOf(value)));
                return;
            }
            if (byte[].class.isAssignableFrom(fieldType)) {
                field.set(object, value);
            }
            if (String.class.isAssignableFrom(fieldType)) {
                field.set(object, new String((byte[]) value));
                return;
            }
            field.set(object, value);
        } catch (ClassCastException | IllegalAccessException e) {
            throw new IginXException(String.format(msg, object.getClass().getName(), field.getName(),
                    value.getClass().getName(), value));
        }

    }


}
