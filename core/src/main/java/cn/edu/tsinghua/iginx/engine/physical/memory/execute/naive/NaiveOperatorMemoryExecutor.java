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
import cn.edu.tsinghua.iginx.engine.physical.exception.UnimplementedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.FilterUtils;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.RowMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.function.SetMappingFunction;
import cn.edu.tsinghua.iginx.engine.shared.operator.BinaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Downsample;
import cn.edu.tsinghua.iginx.engine.shared.operator.Join;
import cn.edu.tsinghua.iginx.engine.shared.operator.Limit;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.engine.shared.operator.RowTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.Select;
import cn.edu.tsinghua.iginx.engine.shared.operator.SetTransform;
import cn.edu.tsinghua.iginx.engine.shared.operator.Sort;
import cn.edu.tsinghua.iginx.engine.shared.operator.UnaryOperator;
import cn.edu.tsinghua.iginx.engine.shared.operator.Union;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.Filter;
import cn.edu.tsinghua.iginx.utils.Pair;
import com.google.gson.internal.LinkedTreeMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class NaiveOperatorMemoryExecutor implements OperatorMemoryExecutor {

    private NaiveOperatorMemoryExecutor() {}

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

    public static NaiveOperatorMemoryExecutor getInstance() {
        return NaiveOperatorMemoryExecutorHolder.INSTANCE;
    }

    private static class NaiveOperatorMemoryExecutorHolder {

        private static final NaiveOperatorMemoryExecutor INSTANCE = new NaiveOperatorMemoryExecutor();

        private NaiveOperatorMemoryExecutorHolder() {}

    }

    private Table transformToTable(RowStream stream) throws PhysicalException {
        if (stream instanceof Table) {
            return (Table) stream;
        }
        Header header = stream.getHeader();
        List<Row> rows = new ArrayList<>();
        while(stream.hasNext()) {
            rows.add(stream.next());
        }
        stream.close();
        return new Table(header, rows);
    }

    private RowStream executeProject(Project project, Table table) throws PhysicalException {
        List<String> patterns = project.getPatterns();
        Header header = table.getHeader();
        List<Field> targetFields = new ArrayList<>();
        for (Field field: header.getFields()) {
            for (String pattern: patterns) {
                if (Pattern.matches(pattern, field.getName())) {
                    targetFields.add(field);
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
                targetRows.add(new Row(targetHeader, objects));
            } else {
                targetRows.add(new Row(targetHeader, row.getTimestamp(), objects));
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
        Map<Long, List<Row>> groups = new TreeMap<>();
        SetMappingFunction function = (SetMappingFunction) downsample.getFunctionCall().getFunction();
        List<Value> params = downsample.getFunctionCall().getParams();
        for (Row row: rows) {
            long timestamp = row.getTimestamp() - (row.getTimestamp() - bias) % precision;
            groups.getOrDefault(timestamp, new ArrayList<>()).add(row);
        }
        List<Pair<Long, Row>> transformedRawRows = new ArrayList<>();
        groups.forEach((time, group) -> {
            Row row = function.transform(new Table(header, group), params);
            if (row != null) {
                transformedRawRows.add(new Pair<>(time, row));
            }
        });
        if (transformedRawRows.size() == 0) {
            return Table.EMPTY_TABLE;
        }
        Header newHeader = new Header(Field.TIME, transformedRawRows.get(0).v.getHeader().getFields());
        List<Row> transformedRows = new ArrayList<>();
        for (Pair<Long, Row> pair: transformedRawRows) {
            transformedRows.add(new Row(newHeader, pair.k, pair.v.getValues()));
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeRowTransform(RowTransform rowTransform, Table table) throws PhysicalException {
        RowMappingFunction function = (RowMappingFunction) rowTransform.getFunctionCall().getFunction();
        List<Value> params = rowTransform.getFunctionCall().getParams();
        List<Row> rows = new ArrayList<>();
        while (table.hasNext()) {
            Row row = function.transform(table.next(), params);
            if (row != null) {
                rows.add(row);
            }
        }
        if (rows.size() == 0) {
            return Table.EMPTY_TABLE;
        }
        Header header = rows.get(0).getHeader();
        return new Table(header, rows);
    }

    private RowStream executeSetTransform(SetTransform setTransform, Table table) throws PhysicalException {
        SetMappingFunction function = (SetMappingFunction) setTransform.getFunctionCall().getFunction();
        List<Value> params = setTransform.getFunctionCall().getParams();
        Row row = function.transform(table, params);
        if (row == null) {
            return Table.EMPTY_TABLE;
        }
        Header header = row.getHeader();
        return new Table(header, Collections.singletonList(row));
    }

    private RowStream executeJoin(Join join, Table tableA, Table tableB) throws PhysicalException {
        // 目前只支持使用时间戳
        if (!join.getJoinBy().equals(Constants.TIMESTAMP)) {
            throw new InvalidOperatorParameterException("join operator is not support for field " + join.getJoinBy() + " except for " + Constants.TIMESTAMP);
        }
        // 检查时间戳
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        if (!headerA.hasTimestamp() || !headerB.hasTimestamp()) {
            throw new InvalidOperatorParameterException("row streams for join operator should have timestamp.");
        }
        // 检查 field
        for (Field field: headerA.getFields()) {
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

        return new Table(headerA, newRows);
    }

    private RowStream executeUnion(Union union, Table tableA, Table tableB) throws PhysicalException {
        // 检查时间是否一致
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        if (headerA.hasTimestamp() ^ headerB.hasTimestamp()) {
            throw new InvalidOperatorParameterException("row stream to be union must have same fields");
        }
        Set<Field> targetFieldSet = new HashSet<>();
        targetFieldSet.addAll(headerA.getFields());
        targetFieldSet.addAll(headerB.getFields());
        throw new UnimplementedOperatorException("unimplemented operator union");
    }

}