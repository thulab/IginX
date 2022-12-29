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

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
            DataType dataType = row1.getField(row1.getHeader().indexOf(prefix1 + '.' + column)).getType();
            int cmp = compareObjects(dataType, value1, value2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static int compareObjects(DataType dataType, Object value1, Object value2) {
        switch (dataType) {
            case BOOLEAN:
                boolean boolean1 = (boolean) value1;
                boolean boolean2 = (boolean) value2;
                return Boolean.compare(boolean1, boolean2);
            case INTEGER:
                int int1 = (int) value1;
                int int2 = (int) value2;
                return Integer.compare(int1, int2);
            case LONG:
                long long1 = (long) value1;
                long long2 = (long) value2;
                return Long.compare(long1, long2);
            case FLOAT:
                float float1 = (float) value1;
                float float2 = (float) value2;
                return Float.compare(float1, float2);
            case DOUBLE:
                double double1 = (double) value1;
                double double2 = (double) value2;
                return Double.compare(double1, double2);
            case BINARY:
                String string1 = (String) value1;
                String string2 = (String) value2;
                return string1.compareTo(string2);
            default:
                throw new IllegalArgumentException("unknown datatype: " + dataType);
        }
    }

    public static Row constructNewRow(Header header, Row rowA, Row rowB) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();
        Object[] valuesJoin = new Object[valuesA.length + valuesB.length + 2];

        valuesJoin[0] = rowA.getTimestamp();
        valuesJoin[valuesA.length + 1] = rowB.getTimestamp();
        System.arraycopy(valuesA, 0, valuesJoin, 1, valuesA.length);
        System.arraycopy(valuesB, 0, valuesJoin, valuesA.length + 2, valuesB.length);
        return new Row(header, valuesJoin);
    }

    public static Row constructNewRow(Header header, Row rowA, Row rowB, int[] indexOfJoinColumnsInTable, boolean cutRight) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();
        Object[] valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnsInTable.length + 2];

        valuesJoin[0] = rowA.getTimestamp();
        if (cutRight) {
            valuesJoin[valuesA.length + 1] = rowB.getTimestamp();
            System.arraycopy(valuesA, 0, valuesJoin, 1, valuesA.length);

            int k = valuesA.length + 2;
            flag:
            for (int i = 0; i < valuesB.length; i++) {
                for (int index : indexOfJoinColumnsInTable) {
                    if (i == index) {
                        continue flag;
                    }
                }
                valuesJoin[k++] = valuesB[i];
            }
        } else {
            valuesJoin[valuesA.length - indexOfJoinColumnsInTable.length + 1] = rowB.getTimestamp();
            System.arraycopy(valuesB, 0, valuesJoin, valuesA.length - indexOfJoinColumnsInTable.length + 2, valuesB.length);

            int k = 1;
            flag:
            for (int i = 0; i < valuesA.length; i++) {
                for (int index : indexOfJoinColumnsInTable) {
                    if (i == index) {
                        continue flag;
                    }
                }
                valuesJoin[k++] = valuesA[i];
            }
        }

        return new Row(header, valuesJoin);
    }

    public static Header constructNewHead(Header headerA, Header headerB, String prefixA, String prefixB) {
        List<Field> fields = new ArrayList<>();
        fields.add(new Field(prefixA + ".key", DataType.LONG));
        fields.addAll(headerA.getFields());
        fields.add(new Field(prefixB + ".key", DataType.LONG));
        fields.addAll(headerB.getFields());
        return new Header(fields);
    }

    public static Pair<int[], Header> constructNewHead(Header headerA, Header headerB, String prefixA, String prefixB, List<String> joinColumns, boolean cutRight) {
        List<Field> fieldsA = headerA.getFields();
        List<Field> fieldsB = headerB.getFields();
        int[] indexOfJoinColumnsInTable = new int[joinColumns.size()];

        List<Field> fields = new ArrayList<>();
        if (cutRight) {
            fields.add(new Field(prefixA + ".key", DataType.LONG));
            fields.addAll(fieldsA);
            fields.add(new Field(prefixB + ".key", DataType.LONG));
            int i = 0;
            flag:
            for (Field fieldB : fieldsB) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldB.getName(), prefixB + '.' + joinColumn)) {
                        indexOfJoinColumnsInTable[i++] = headerB.indexOf(fieldB);
                        continue flag;
                    }
                }
                fields.add(fieldB);
            }
        } else {
            fields.add(new Field(prefixA + ".key", DataType.LONG));
            int i = 0;
            flag:
            for (Field fieldA : fieldsA) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldA.getName(), prefixA + '.' + joinColumn)) {
                        indexOfJoinColumnsInTable[i++] = headerA.indexOf(fieldA);
                        continue flag;
                    }
                }
                fields.add(fieldA);
            }
            fields.add(new Field(prefixB + ".key", DataType.LONG));
            fields.addAll(fieldsB);
        }
        return new Pair<>(indexOfJoinColumnsInTable, new Header(fields));
    }

    public static Row constructUnmatchedRow(Header header, Row halfRow, int anotherRowSize, boolean putLeft) {
        Object[] valuesJoin = new Object[halfRow.getValues().length + anotherRowSize + 2];
        if (putLeft) {
            valuesJoin[0] = halfRow.getTimestamp();
            System.arraycopy(halfRow.getValues(), 0, valuesJoin, 1, halfRow.getValues().length);
        } else {
            valuesJoin[anotherRowSize + 1] = halfRow.getTimestamp();
            System.arraycopy(halfRow.getValues(), 0, valuesJoin, anotherRowSize + 2, halfRow.getValues().length);
        }
        return new Row(header, valuesJoin);
    }

    public static void fillNaturalJoinColumns(List<String> joinColumns, Header headerA, Header headerB, String prefixA, String prefixB) throws PhysicalException {
        if (!joinColumns.isEmpty()) {
            throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
        }
        for (Field fieldA : headerA.getFields()) {
            for (Field fieldB : headerB.getFields()) {
                String joinColumnA = fieldA.getName().replaceFirst(prefixA + '.', "");
                String joinColumnB = fieldB.getName().replaceFirst(prefixB + '.', "");
                if (joinColumnA.equals(joinColumnB)) {
                    joinColumns.add(joinColumnA);
                }
            }
        }
        if (joinColumns.isEmpty()) {
            throw new PhysicalException("natural join has no matching columns");
        }
    }
}
