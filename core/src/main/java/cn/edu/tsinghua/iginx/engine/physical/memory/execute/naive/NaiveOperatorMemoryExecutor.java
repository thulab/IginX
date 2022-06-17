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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.naive;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalTaskExecuteFailureException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.RowUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.utils.Pair;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.*;
import java.util.regex.Pattern;

public class NaiveOperatorMemoryExecutor implements OperatorMemoryExecutor {

    private NaiveOperatorMemoryExecutor() {
    }

    public static NaiveOperatorMemoryExecutor getInstance() {
        return NaiveOperatorMemoryExecutorHolder.INSTANCE;
    }

    @Override
    public RowStream executeUnaryOperator(UnaryOperator operator, RowStream stream) throws PhysicalException {
        switch (operator.getType()) {
            case Project:
                return executeProject((Project) operator, transformToTable(stream));
            case Select:
                return executeSelect((Select) operator, transformToTable(stream));
            case Sort:
                return executeSort((Sort) operator, transformToTable(stream));
            case Limit:
                return executeLimit((Limit) operator, transformToTable(stream));
            case Downsample:
                return executeDownsample((Downsample) operator, transformToTable(stream));
            case RowTransform:
                return executeRowTransform((RowTransform) operator, transformToTable(stream));
            case SetTransform:
                return executeSetTransform((SetTransform) operator, transformToTable(stream));
            case MappingTransform:
                return executeMappingTransform((MappingTransform) operator, transformToTable(stream));
            default:
                throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
        }
    }

    @Override
    public RowStream executeBinaryOperator(BinaryOperator operator, RowStream streamA, RowStream streamB) throws PhysicalException {
        switch (operator.getType()) {
            case Join:
                return executeJoin((Join) operator, transformToTable(streamA), transformToTable(streamB));
            case Union:
                return executeUnion((Union) operator, transformToTable(streamA), transformToTable(streamB));
            default:
                throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
        }
    }

    private Table transformToTable(RowStream stream) throws PhysicalException {
        if (stream instanceof Table) {
            return (Table) stream;
        }
        Header header = stream.getHeader();
        List<Row> rows = new ArrayList<>();
        while (stream.hasNext()) {
            rows.add(stream.next());
        }
        stream.close();
        return new Table(header, rows);
    }

    private RowStream executeProject(Project project, Table table) throws PhysicalException {
        List<String> patterns = project.getPatterns();
        Header header = table.getHeader();
        List<Field> targetFields = new ArrayList<>();

        for (Field field : header.getFields()) {
            for (String pattern : patterns) {
                if (!StringUtils.isPattern(pattern)) {
                    if (pattern.equals(field.getName()) || field.getName().startsWith(pattern)) {
                        targetFields.add(field);
                    }
                } else {
                    if (Pattern.matches(StringUtils.reformatPath(pattern), field.getName())) {
                        targetFields.add(field);
                    }
                }
            }
        }
        Header targetHeader = new Header(header.getTime(), targetFields);
        List<Row> targetRows = new ArrayList<>();
        while (table.hasNext()) {
            Row row = table.next();
            Object[] objects = new Object[targetFields.size()];
            for (int i = 0; i < targetFields.size(); i++) {
                objects[i] = row.getValue(targetFields.get(i));
            }
            if (header.hasTimestamp()) {
                targetRows.add(new Row(targetHeader, row.getTimestamp(), objects));
            } else {
                targetRows.add(new Row(targetHeader, objects));
            }
        }
        return new Table(targetHeader, targetRows);
    }

    private RowStream executeSelect(Select select, Table table) throws PhysicalException {
        Filter filter = select.getFilter();
        List<Row> targetRows = new ArrayList<>();
        while (table.hasNext()) {
            Row row = table.next();
            if (FilterUtils.validate(filter, row)) {
                targetRows.add(row);
            }
        }
        return new Table(table.getHeader(), targetRows);
    }

    private RowStream executeSort(Sort sort, Table table) throws PhysicalException {
        if (!sort.getSortBy().equals(Constants.TIMESTAMP)) {
            throw new InvalidOperatorParameterException("sort operator is not support for field " + sort.getSortBy() + " except for " + Constants.TIMESTAMP);
        }
        if (sort.getSortType() == Sort.SortType.ASC) {
            // 在默认的实现中，每张表都是根据时间已经升序排好的，因此依据时间升序排列的话，已经不需要做任何额外的操作了
            return table;
        }
        // 降序排列的话，只需要将各行反过来就行
        Header header = table.getHeader();
        List<Row> rows = new ArrayList<>();
        for (int i = table.getRowSize() - 1; i >= 0; i--) {
            rows.add(table.getRow(i));
        }
        return new Table(header, rows);
    }

    private RowStream executeLimit(Limit limit, Table table) throws PhysicalException {
        int rowSize = table.getRowSize();
        Header header = table.getHeader();
        List<Row> rows = new ArrayList<>();
        if (rowSize > limit.getOffset()) { // 没有把所有的行都跳过
            for (int i = limit.getOffset(); i < rowSize && i - limit.getOffset() < limit.getLimit(); i++) {
                rows.add(table.getRow(i));
            }
        }
        return new Table(header, rows);
    }

    private RowStream executeDownsample(Downsample downsample, Table table) throws PhysicalException {
        Header header = table.getHeader();
        if (!header.hasTimestamp()) {
            throw new InvalidOperatorParameterException("downsample operator is not support for row stream without timestamps.");
        }
        List<Row> rows = table.getRows();
        long bias = downsample.getTimeRange().getBeginTime();
        long precision = downsample.getPrecision();
        TreeMap<Long, List<Row>> groups = new TreeMap<>();
        SetMappingFunction function = (SetMappingFunction) downsample.getFunctionCall().getFunction();
        Map<String, Value> params = downsample.getFunctionCall().getParams();
        for (Row row : rows) {
            long timestamp = row.getTimestamp() - (row.getTimestamp() - bias) % precision;
            groups.compute(timestamp, (k, v) -> v == null ? new ArrayList<>() : v).add(row);
        }
        List<Pair<Long, Row>> transformedRawRows = new ArrayList<>();
        try {
            for (Map.Entry<Long, List<Row>> entry : groups.entrySet()) {
                long time = entry.getKey();
                List<Row> group = entry.getValue();
                Row row = function.transform(new Table(header, group), params);
                if (row != null) {
                    transformedRawRows.add(new Pair<>(time, row));
                }
            }
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when execute set mapping function " + function.getIdentifier() + ".", e);
        }
        if (transformedRawRows.size() == 0) {
            return Table.EMPTY_TABLE;
        }
        Header newHeader = new Header(Field.TIME, transformedRawRows.get(0).v.getHeader().getFields());
        List<Row> transformedRows = new ArrayList<>();
        for (Pair<Long, Row> pair : transformedRawRows) {
            transformedRows.add(new Row(newHeader, pair.k, pair.v.getValues()));
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeRowTransform(RowTransform rowTransform, Table table) throws PhysicalException {
        RowMappingFunction function = (RowMappingFunction) rowTransform.getFunctionCall().getFunction();
        Map<String, Value> params = rowTransform.getFunctionCall().getParams();
        List<Row> rows = new ArrayList<>();
        try {
            while (table.hasNext()) {
                Row row = function.transform(table.next(), params);
                if (row != null) {
                    rows.add(row);
                }
            }
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when execute row mapping function " + function.getIdentifier() + ".", e);
        }
        if (rows.size() == 0) {
            return Table.EMPTY_TABLE;
        }
        Header header = rows.get(0).getHeader();
        return new Table(header, rows);
    }

    private RowStream executeSetTransform(SetTransform setTransform, Table table) throws PhysicalException {
        SetMappingFunction function = (SetMappingFunction) setTransform.getFunctionCall().getFunction();
        Map<String, Value> params = setTransform.getFunctionCall().getParams();
        try {
            Row row = function.transform(table, params);
            if (row == null) {
                return Table.EMPTY_TABLE;
            }
            Header header = row.getHeader();
            return new Table(header, Collections.singletonList(row));
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when execute set mapping function " + function.getIdentifier() + ".", e);
        }

    }

    private RowStream executeMappingTransform(MappingTransform mappingTransform, Table table) throws PhysicalException {
        MappingFunction function = (MappingFunction) mappingTransform.getFunctionCall().getFunction();
        Map<String, Value> params = mappingTransform.getFunctionCall().getParams();
        try {
            return function.transform(table, params);
        } catch (Exception e) {
            throw new PhysicalTaskExecuteFailureException("encounter error when execute mapping function " + function.getIdentifier() + ".", e);
        }
    }

    private RowStream executeJoin(Join join, Table tableA, Table tableB) throws PhysicalException {
        // 目前只支持使用时间戳和顺序
        if (join.getJoinBy().equals(Constants.TIMESTAMP)) {
            // 检查时间戳
            Header headerA = tableA.getHeader();
            Header headerB = tableB.getHeader();
            if (!headerA.hasTimestamp() || !headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by time should have timestamp.");
            }
            // 检查 field
            for (Field field : headerA.getFields()) {
                if (headerB.indexOf(field) != -1) { // 二者的 field 存在交集
                    throw new PhysicalTaskExecuteFailureException("two source has shared field");
                }
            }
            List<Field> newFields = new ArrayList<>();
            newFields.addAll(headerA.getFields());
            newFields.addAll(headerB.getFields());
            Header newHeader = new Header(Field.TIME, newFields);
            List<Row> newRows = new ArrayList<>();

            int index1 = 0, index2 = 0;
            while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
                Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                long timestamp;
                if (rowA.getTimestamp() == rowB.getTimestamp()) {
                    timestamp = rowA.getTimestamp();
                    System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
                    System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
                    index1++;
                    index2++;
                } else if (rowA.getTimestamp() < rowB.getTimestamp()) {
                    timestamp = rowA.getTimestamp();
                    System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
                    index1++;
                } else {
                    timestamp = rowB.getTimestamp();
                    System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
                    index2++;
                }
                newRows.add(new Row(newHeader, timestamp, values));
            }

            for (; index1 < tableA.getRowSize(); index1++) {
                Row rowA = tableA.getRow(index1);
                Object[] values = new Object[newHeader.getFieldSize()];
                System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
                newRows.add(new Row(newHeader, rowA.getTimestamp(), values));
            }

            for (; index2 < tableB.getRowSize(); index2++) {
                Row rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
                newRows.add(new Row(newHeader, rowB.getTimestamp(), values));
            }
            return new Table(newHeader, newRows);
        } else if (join.getJoinBy().equals(Constants.ORDINAL)) {
            Header headerA = tableA.getHeader();
            Header headerB = tableB.getHeader();
            if (headerA.hasTimestamp() || headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by ordinal shouldn't have timestamp.");
            }
            for (Field field : headerA.getFields()) {
                if (headerB.indexOf(field) != -1) { // 二者的 field 存在交集
                    throw new PhysicalTaskExecuteFailureException("two source has shared field");
                }
            }
            List<Field> newFields = new ArrayList<>();
            newFields.addAll(headerA.getFields());
            newFields.addAll(headerB.getFields());
            Header newHeader = new Header(newFields);
            List<Row> newRows = new ArrayList<>();

            int index1 = 0, index2 = 0;
            while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
                Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
                System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
                index1++;
                index2++;
                newRows.add(new Row(newHeader, values));
            }
            for (; index1 < tableA.getRowSize(); index1++) {
                Row rowA = tableA.getRow(index1);
                Object[] values = new Object[newHeader.getFieldSize()];
                System.arraycopy(rowA.getValues(), 0, values, 0, headerA.getFieldSize());
                newRows.add(new Row(newHeader, values));
            }

            for (; index2 < tableB.getRowSize(); index2++) {
                Row rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                System.arraycopy(rowB.getValues(), 0, values, headerA.getFieldSize(), headerB.getFieldSize());
                newRows.add(new Row(newHeader, values));
            }
            return new Table(newHeader, newRows);
        } else {
            throw new InvalidOperatorParameterException("join operator is not support for field " + join.getJoinBy() + " except for " + Constants.TIMESTAMP + " and " + Constants.ORDINAL);
        }
    }

    private RowStream executeUnion(Union union, Table tableA, Table tableB) throws PhysicalException {
        // 检查时间是否一致
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        if (headerA.hasTimestamp() ^ headerB.hasTimestamp()) {
            throw new InvalidOperatorParameterException("row stream to be union must have same fields");
        }
        boolean hasTimestamp = headerA.hasTimestamp();
        Set<Field> targetFieldSet = new HashSet<>();
        targetFieldSet.addAll(headerA.getFields());
        targetFieldSet.addAll(headerB.getFields());
        List<Field> targetFields = new ArrayList<>(targetFieldSet);
        Header targetHeader;
        List<Row> rows = new ArrayList<>();
        if (!hasTimestamp) {
            targetHeader = new Header(targetFields);
            for (Row row : tableA.getRows()) {
                rows.add(RowUtils.transform(row, targetHeader));
            }
            for (Row row : tableB.getRows()) {
                rows.add(RowUtils.transform(row, targetHeader));
            }
        } else {
            targetHeader = new Header(Field.TIME, targetFields);
            int index1 = 0, index2 = 0;
            while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
                Row row1 = tableA.getRow(index1);
                Row row2 = tableB.getRow(index2);
                if (row1.getTimestamp() <= row2.getTimestamp()) {
                    rows.add(RowUtils.transform(row1, targetHeader));
                    index1++;
                } else {
                    rows.add(RowUtils.transform(row2, targetHeader));
                    index2++;
                }
            }
            for (; index1 < tableA.getRowSize(); index1++) {
                rows.add(RowUtils.transform(tableA.getRow(index1), targetHeader));
            }
            for (; index2 < tableB.getRowSize(); index2++) {
                rows.add(RowUtils.transform(tableB.getRow(index2), targetHeader));
            }
        }
        return new Table(targetHeader, rows);
    }

    private static class NaiveOperatorMemoryExecutorHolder {

        private static final NaiveOperatorMemoryExecutor INSTANCE = new NaiveOperatorMemoryExecutor();

        private NaiveOperatorMemoryExecutorHolder() {
        }

    }

}
