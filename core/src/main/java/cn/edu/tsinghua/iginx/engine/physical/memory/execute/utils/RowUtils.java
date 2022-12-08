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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

import java.util.List;

public class RowUtils {

    public static Row transform(Row row, Header targetHeader) {
        Object[] values = new Object[targetHeader.getFieldSize()];
        for (int i = 0; i < targetHeader.getFieldSize(); i++) {
            Field field = targetHeader.getField(i);
            values[i] = row.getValue(field);
        }
        Row targetRow;
        if (targetHeader.hasTimestamp()) {
            targetRow = new Row(targetHeader, row.getTimestamp(), values);
        } else {
            targetRow = new Row(targetHeader, values);
        }
        return targetRow;
    }

    /**
     * @return <tt>-1</tt>: not sorted
     *         <tt>0</tt>: all rows equal
     *         <tt>1</tt>: ascending sorted
     *         <tt>2</tt>: descending sorted
     */
    public static int checkRowsSortedByColumns(List<Row> rows, String prefix, List<String> columns) {
        int res = 0;
        int index = 0;
        while (index < rows.size() - 1) {
            int mark = compareRowsSortedByColumns(rows.get(index), rows.get(index + 1), prefix, prefix, columns);
            if (mark == -1) {
                if (res == 0) {
                    res = 1;
                } else if (res == 2) {
                    return -1;
                }
            } else if (mark == 1) {
                if (res == 0) {
                    res = 2;
                } else if (res == 1) {
                    return -1;
                }
            }
            index++;
        }
        return res;
    }

    /**
     * @return <tt>-1</tt>: row1 < row2
     *         <tt>0</tt>: row1 = row2
     *         <tt>1</tt>: row1 > row2
     */
    public static int compareRowsSortedByColumns(Row row1, Row row2, String prefix1, String prefix2, List<String> columns) {
        for (String column : columns) {
            Object value1 = row1.getValue(prefix1 + '.' + column);
            Object value2 = row2.getValue(prefix2 + '.' + column);
            if (value1 == null && value2 == null) {
                return 0;
            } else if (value1 == null) {
                return -1;
            } else if (value2 == null) {
                return 1;
            }
            switch (row1.getField(row1.getHeader().indexOf(prefix1 + '.' + column)).getType()) {
                case BOOLEAN:
                    boolean boolean1 = (boolean) value1;
                    boolean boolean2 = (boolean) value2;
                    if (boolean1 == boolean2) {
                        continue;
                    } else if (!boolean1) {
                        return -1;
                    } else {
                        return 1;
                    }
                case INTEGER:
                    int int1 = (int) value1;
                    int int2 = (int) value2;
                    if (int1 == int2) {
                        continue;
                    } else if (int1 < int2) {
                        return -1;
                    } else {
                        return 1;
                    }
                case LONG:
                    long long1 = (long) value1;
                    long long2 = (long) value2;
                    if (long1 == long2) {
                        continue;
                    } else if (long1 < long2) {
                        return -1;
                    } else {
                        return 1;
                    }
                case FLOAT:
                    float float1 = (float) value1;
                    float float2 = (float) value2;
                    if (float1 == float2) {
                        continue;
                    } else if (float1 < float2) {
                        return -1;
                    } else {
                        return 1;
                    }
                case DOUBLE:
                    double double1 = (double) value1;
                    double double2 = (double) value2;
                    if (double1 == double2) {
                        continue;
                    } else if (double1 < double2) {
                        return -1;
                    } else {
                        return 1;
                    }
                case BINARY:
                    String string1 = (String) value1;
                    String string2 = (String) value2;
                    if (string1.compareTo(string2) < 0) {
                        return -1;
                    } else if (string1.compareTo(string2) > 0) {
                        return 1;
                    }
            }
        }
        return 0;
    }
}
