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

import cn.edu.tsinghua.iginx.constant.GlobalConstant;
import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.function.system.utils.ValueUtils;
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
        if (targetHeader.hasKey()) {
            targetRow = new Row(targetHeader, row.getKey(), values);
        } else {
            targetRow = new Row(targetHeader, values);
        }
        return targetRow;
    }

    /**
     * @return <tt>-1</tt>: not sorted
     * <tt>0</tt>: all rows equal
     * <tt>1</tt>: ascending sorted
     * <tt>2</tt>: descending sorted
     */
    public static int checkRowsSortedByColumns(List<Row> rows, String prefix,
        List<String> columns) throws PhysicalException {
        int res = 0;
        int index = 0;
        while (index < rows.size() - 1) {
            int mark = compareRowsSortedByColumns(rows.get(index), rows.get(index + 1), prefix,
                prefix, columns, columns);
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
     * <tt>0</tt>: row1 = row2
     * <tt>1</tt>: row1 > row2
     */
    public static int compareRowsSortedByColumns(Row row1, Row row2, String prefix1, String prefix2,
        List<String> columns1, List<String> columns2) throws PhysicalException {
        assert columns1.size() == columns2.size();
        int size = columns1.size();
        for (int index = 0; index < size; index++) {
            Object value1 = row1.getValue(prefix1 + '.' + columns1.get(index));
            Object value2 = row2.getValue(prefix2 + '.' + columns2.get(index));
            if (value1 == null && value2 == null) {
                return 0;
            } else if (value1 == null) {
                return -1;
            } else if (value2 == null) {
                return 1;
            }
            DataType dataType1 = row1.getField(row1.getHeader().indexOf(prefix1 + '.' + columns1.get(index)))
                .getType();
            DataType dataType2 = row2.getField(row2.getHeader().indexOf(prefix2 + '.' + columns2.get(index)))
                .getType();
            int cmp = ValueUtils.compare(value1, value2, dataType1, dataType2);
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }

    public static Header constructNewHead(Header headerA, Header headerB, String prefixA,
        String prefixB) {
        List<Field> fields = new ArrayList<>();
        if (headerA.hasKey()) {
            fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
        }
        fields.addAll(headerA.getFields());
        if (headerB.hasKey()) {
            fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
        }
        fields.addAll(headerB.getFields());
        return new Header(fields);
    }

    public static Pair<int[], Header> constructNewHead(Header headerA, Header headerB,
        String prefixA, String prefixB, List<String> joinColumns, boolean cutRight) {
        List<Field> fieldsA = headerA.getFields();
        List<Field> fieldsB = headerB.getFields();
        int[] indexOfJoinColumnsInTable = new int[joinColumns.size()];

        List<Field> fields = new ArrayList<>();
        if (headerA.hasKey()) {
            fields.add(new Field(prefixA + "." + GlobalConstant.KEY_NAME, DataType.LONG));
        }
        if (cutRight) {
            fields.addAll(fieldsA);
            if (headerB.hasKey()) {
                fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
            }
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
            if (headerB.hasKey()) {
                fields.add(new Field(prefixB + "." + GlobalConstant.KEY_NAME, DataType.LONG));
            }
            fields.addAll(fieldsB);
        }
        return new Pair<>(indexOfJoinColumnsInTable, new Header(fields));
    }

    public static Row constructUnmatchedRow(Header header, Row halfRow, int anotherRowSize,
        boolean putLeft) {

        int size = halfRow.getValues().length + anotherRowSize;
        if (halfRow.getHeader().hasKey()) {
            size++;
        }
        Object[] valuesJoin = new Object[size];

        if (putLeft) {
            if (halfRow.getHeader().hasKey()) {
                valuesJoin[0] = halfRow.getKey();
                System.arraycopy(halfRow.getValues(), 0, valuesJoin, 1, halfRow.getValues().length);
            } else {
                System.arraycopy(halfRow.getValues(), 0, valuesJoin, 0, halfRow.getValues().length);
            }
        } else {
            if (halfRow.getHeader().hasKey()) {
                valuesJoin[anotherRowSize] = halfRow.getKey();
                System.arraycopy(halfRow.getValues(), 0, valuesJoin, anotherRowSize + 1, halfRow.getValues().length);
            } else {
                System.arraycopy(halfRow.getValues(), 0, valuesJoin, anotherRowSize, halfRow.getValues().length);
            }
        }
        return new Row(header, valuesJoin);
    }

    public static Row constructNewRow(Header header, Row rowA, Row rowB) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();

        int size = valuesA.length + valuesB.length;
        int rowAStartIndex = 0, rowBStartIndex = valuesA.length;
        if (rowA.getHeader().hasKey()) {
            size++;
            rowAStartIndex++;
            rowBStartIndex++;
        }
        if (rowB.getHeader().hasKey()) {
            size++;
            rowBStartIndex++;
        }

        Object[] valuesJoin = new Object[size];

        if (rowA.getHeader().hasKey()) {
            valuesJoin[0] = rowA.getKey();
        }
        if (rowB.getHeader().hasKey()) {
            valuesJoin[rowBStartIndex - 1] = rowB.getKey();
        }
        System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
        System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
        return new Row(header, valuesJoin);
    }

    public static Row constructNewRow(Header header, Row rowA, Row rowB,
        int[] indexOfJoinColumnsInTable, boolean cutRight) {
        Object[] valuesA = rowA.getValues();
        Object[] valuesB = rowB.getValues();

        int size = valuesA.length + valuesB.length - indexOfJoinColumnsInTable.length;
        int rowAStartIndex = 0, rowBStartIndex;
        if (cutRight) {
            rowBStartIndex = valuesA.length;
        } else {
            rowBStartIndex = valuesA.length - indexOfJoinColumnsInTable.length;
        }

        if (rowA.getHeader().hasKey()) {
            size++;
            rowAStartIndex++;
            rowBStartIndex++;
        }
        if (rowB.getHeader().hasKey()) {
            size++;
            rowBStartIndex++;
        }

        Object[] valuesJoin = new Object[size];

        if (rowA.getHeader().hasKey()) {
            valuesJoin[0] = rowA.getKey();
        }
        if (rowB.getHeader().hasKey()) {
            valuesJoin[rowBStartIndex - 1] = rowB.getKey();
        }
        if (cutRight) {
            System.arraycopy(valuesA, 0, valuesJoin, rowAStartIndex, valuesA.length);
            int k = rowBStartIndex;
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
            System.arraycopy(valuesB, 0, valuesJoin, rowBStartIndex, valuesB.length);
            int k = rowAStartIndex;
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

    public static void fillNaturalJoinColumns(List<String> joinColumns, Header headerA,
        Header headerB, String prefixA, String prefixB) throws PhysicalException {
        if (!joinColumns.isEmpty()) {
            throw new InvalidOperatorParameterException(
                "natural inner join operator should not have using operator");
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
