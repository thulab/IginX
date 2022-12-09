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
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils.TableUtils;
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
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.FilterType;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.PathFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.utils.Bitmap;
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
            case Rename:
                return executeRename((Rename) operator, transformToTable(stream));
            case Reorder:
                return executeReorder((Reorder) operator, transformToTable(stream));
            default:
                throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
        }
    }

    @Override
    public RowStream executeBinaryOperator(BinaryOperator operator, RowStream streamA, RowStream streamB) throws PhysicalException {
        switch (operator.getType()) {
            case Join:
                return executeJoin((Join) operator, transformToTable(streamA), transformToTable(streamB));
            case CrossJoin:
                return executeCrossJoin((CrossJoin) operator, transformToTable(streamA), transformToTable(streamB));
            case InnerJoin:
                return executeInnerJoin((InnerJoin) operator, transformToTable(streamA), transformToTable(streamB));
            case OuterJoin:
                return executeOuterJoin((OuterJoin) operator, transformToTable(streamA), transformToTable(streamB));
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
        long bias = downsample.getTimeRange().getActualBeginTime();
        long endTime = downsample.getTimeRange().getActualEndTime();
        long precision = downsample.getPrecision();
        long slideDistance = downsample.getSlideDistance();
        // startTime + (n - 1) * slideDistance + precision - 1 >= endTime
        int n = (int) (Math.ceil((double)(endTime - bias - precision + 1) / slideDistance) + 1);
        TreeMap<Long, List<Row>> groups = new TreeMap<>();
        SetMappingFunction function = (SetMappingFunction) downsample.getFunctionCall().getFunction();
        Map<String, Value> params = downsample.getFunctionCall().getParams();
        if (precision == slideDistance) {
            for (Row row : rows) {
                long timestamp = row.getTimestamp() - (row.getTimestamp() - bias) % precision;
                groups.compute(timestamp, (k, v) -> v == null ? new ArrayList<>() : v).add(row);
            }
        } else {
            long[] timestamps = new long[n];
            for (int i = 0; i < n; i++) {
                timestamps[i] = bias + i * slideDistance;
            }
            for (Row row : rows) {
                long rowTimestamp = row.getTimestamp();
                for (int i = 0; i < n; i++) {
                    if (rowTimestamp - timestamps[i] >= 0 && rowTimestamp - timestamps[i] < precision) {
                        groups.compute(timestamps[i], (k, v) -> v == null ? new ArrayList<>() : v).add(row);
                    }
                }
            }
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

    private RowStream executeRename(Rename rename, Table table) throws PhysicalException {
        Header header = table.getHeader();
        Map<String, String> aliasMap = rename.getAliasMap();

        List<Field> fields = new ArrayList<>();
        header.getFields().forEach(field -> {
            String alias = "";
            for (String oldName : aliasMap.keySet()) {
                Pattern pattern = Pattern.compile(StringUtils.reformatColumnName(oldName) + ".*");
                if (pattern.matcher(field.getFullName()).matches()) {
                    alias = aliasMap.get(oldName);
                    break;
                }
            }
            if (alias.equals("")) {
                fields.add(field);
            } else {
                fields.add(new Field(alias, field.getType(), field.getTags()));
            }
        });

        Header newHeader = new Header(header.getTime(), fields);

        List<Row> rows = new ArrayList<>();
        table.getRows().forEach(row -> {
            if (newHeader.hasTimestamp()) {
                rows.add(new Row(newHeader, row.getTimestamp(), row.getValues()));
            } else {
                rows.add(new Row(newHeader, row.getValues()));
            }
        });

        return new Table(newHeader, rows);
    }

    private RowStream executeReorder(Reorder reorder, Table table) throws PhysicalException {
        List<String> patterns = reorder.getPatterns();
        Header header = table.getHeader();
        List<Field> targetFields = new ArrayList<>();
        Map<Integer, Integer> reorderMap = new HashMap<>();

        for (String pattern : patterns) {
            List<Pair<Field, Integer>> matchedFields = new ArrayList<>();
            if (StringUtils.isPattern(pattern)) {
                for (int i = 0; i < header.getFields().size(); i++) {
                    Field field  = header.getField(i);
                    if (Pattern.matches(StringUtils.reformatColumnName(pattern), field.getName())) {
                        matchedFields.add(new Pair<>(field, i));
                    }
                }
            } else {
                for (int i = 0; i < header.getFields().size(); i++) {
                    Field field  = header.getField(i);
                    if (pattern.equals(field.getName()) || field.getName().startsWith(pattern)) {
                        matchedFields.add(new Pair<>(field, i));
                    }
                }
            }
            if (!matchedFields.isEmpty()) {
                matchedFields.sort(Comparator.comparing(pair -> pair.getK().getFullName()));
                matchedFields.forEach(pair -> {
                    reorderMap.put(targetFields.size(), pair.getV());
                    targetFields.add(pair.getK());
                });
            }
        }

        Header newHeader = new Header(header.getTime(), targetFields);
        List<Row> rows = new ArrayList<>();
        table.getRows().forEach(row -> {
            Object[] values = new Object[targetFields.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = row.getValue(reorderMap.get(i));
            }
            if (newHeader.hasTimestamp()) {
                rows.add(new Row(newHeader, row.getTimestamp(), values));
            } else {
                rows.add(new Row(newHeader, values));
            }
        });
        return new Table(newHeader, rows);
    }

    private RowStream executeJoin(Join join, Table tableA, Table tableB) throws PhysicalException {
        boolean hasIntersect = false;
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        // 检查 field，暂时不需要
        for (Field field : headerA.getFields()) {
            if (headerB.indexOf(field) != -1) { // 二者的 field 存在交集
                hasIntersect = true;
                break;
            }
        }
        if (hasIntersect) {
            return executeIntersectJoin(join, tableA, tableB);
        }
        // 目前只支持使用时间戳和顺序
        if (join.getJoinBy().equals(Constants.TIMESTAMP)) {
            // 检查时间戳
            if (!headerA.hasTimestamp() || !headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by time should have timestamp.");
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
            if (headerA.hasTimestamp() || headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by ordinal shouldn't have timestamp.");
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

    private RowStream executeCrossJoin(CrossJoin crossJoin, Table tableA, Table tableB) throws PhysicalException {
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());

        fieldsA.addAll(fieldsB);
        Header newHeader = new Header(fieldsA);

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();

        List<Row> transformedRows = new ArrayList<>();
        for (Row rowA : rowsA) {
            for (Row rowB : rowsB) {
                Object[] valuesA = rowA.getValues();
                Object[] valuesB = rowB.getValues();
                Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                Row transformedRow = new Row(newHeader, valuesJoin);
                transformedRows.add(transformedRow);
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB) throws PhysicalException {
        switch (innerJoin.getJoinAlgType()) {
            case NestedLoopJoin:
                return executeNestedLoopInnerJoin(innerJoin, tableA, tableB);
            case HashJoin:
                return executeHashInnerJoin(innerJoin, tableA, tableB);
            case SortedMergeJoin:
                return executeSortedMergeInnerJoin(innerJoin, tableA, tableB);
            default:
                throw new PhysicalException("Unknown join algorithm type: " + innerJoin.getJoinAlgType());
        }
    }

    private RowStream executeNestedLoopInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB) throws PhysicalException {
        Filter filter = innerJoin.getFilter();
        List<String> joinColumns = innerJoin.getJoinColumns();
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
        if (innerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
            }
            joinColumns = new ArrayList<>();
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(innerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(innerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            fieldsA.addAll(fieldsB);
            newHeader = new Header(fieldsA);
            for (Row rowA : rowsA) {
                for (Row rowB : rowsB) {
                    Object[] valuesA = rowA.getValues();
                    Object[] valuesB = rowB.getValues();
                    Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                    System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                    System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                    Row transformedRow = new Row(newHeader, valuesJoin);
                    if (FilterUtils.validate(filter, transformedRow)) {
                        transformedRows.add(transformedRow);
                    }
                }
            }
        } else { // Join condition: natural or using
            int[] indexOfJoinColumnInTableB = new int[joinColumns.size()];
            int i = 0;
            flag1:
            for (Field fieldB : fieldsB) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldB.getName(), innerJoin.getPrefixB() + '.' + joinColumn)) {
                        indexOfJoinColumnInTableB[i++] = headerB.indexOf(fieldB);
                        continue flag1;
                    }
                }
                fieldsA.add(fieldB);
            }
            newHeader = new Header(fieldsA);
            for (Row rowA : rowsA) {
                flag2:
                for (Row rowB : rowsB) {
                    for (String joinColumn : joinColumns){
                        if (rowA.getValue(innerJoin.getPrefixA() + '.' + joinColumn) != rowB.getValue(innerJoin.getPrefixB() + '.' + joinColumn)){
                            continue flag2;
                        }
                    }
                    Object[] valuesA = rowA.getValues();
                    Object[] valuesB = rowB.getValues();
                    Object[] valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableB.length];
                    System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                    int k = valuesA.length;
                    flag3:
                    for (int j = 0; j < valuesB.length; j++) {
                        for (int index : indexOfJoinColumnInTableB) {
                            if (j == index) {
                                continue flag3;
                            }
                        }
                        valuesJoin[k++] = valuesB[j];
                    }
                    transformedRows.add(new Row(newHeader, valuesJoin));
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeHashInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB) throws PhysicalException {
        Filter filter = innerJoin.getFilter();
        List<String> joinColumns = innerJoin.getJoinColumns();
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
        if (innerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
            }
            joinColumns = new ArrayList<>();
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(innerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(innerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        
        String joinColumn;
        if (filter != null) {
            if (!filter.getType().equals(FilterType.Path)) {
                throw new InvalidOperatorParameterException("hash join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException("hash join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                joinColumn = p.k.replaceFirst(innerJoin.getPrefixA() + '.', "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                joinColumn = p.v.replaceFirst(innerJoin.getPrefixA() + '.', "");
            } else {
                throw new InvalidOperatorParameterException("invalid hash join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("hash join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(innerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                joinColumn = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid hash join column input.");
            }
        }
        
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
        for (Row rowB: rowsB) {
            int hash = rowB.getValue(innerJoin.getPrefixB() + '.' + joinColumn).hashCode();
            List<Row> l = rowsBHashMap.containsKey(hash) ? rowsBHashMap.get(hash) : new ArrayList<>();
            l.add(rowB);
            rowsBHashMap.put(hash, l);
        }
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            fieldsA.addAll(fieldsB);
            newHeader = new Header(fieldsA);
            for (Row rowA : rowsA) {
                int hash = rowA.getValue(innerJoin.getPrefixA() + '.' + joinColumn).hashCode();
                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    for (Row rowB : hashRowsB) {
                        Object[] valuesA = rowA.getValues();
                        Object[] valuesB = rowB.getValues();
                        Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                        System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                        Row transformedRow = new Row(newHeader, valuesJoin);
                        if (FilterUtils.validate(filter, transformedRow)) {
                            transformedRows.add(transformedRow);
                        }
                    }
                }
            }
        } else { // Join condition: natural or using
            for (Field fieldB : fieldsB) {
                if (!fieldB.getName().equals(innerJoin.getPrefixB() + '.' + joinColumn)) {
                    fieldsA.add(fieldB);
                }
            }
            newHeader = new Header(fieldsA);
            for (Row rowA : rowsA) {
                int hash = rowA.getValue(innerJoin.getPrefixA() + '.' + joinColumn).hashCode();
                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    for (Row rowB : hashRowsB) {
                        Object[] valuesA = rowA.getValues();
                        Object[] valuesB = rowB.getValues();
                        Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                        int k = valuesA.length;
                        int index = headerB.indexOf(innerJoin.getPrefixB() + '.' + joinColumn);
                        for (int j = 0; j < valuesB.length; j++) {
                            if (j != index) {
                                valuesJoin[k++] = valuesB[j];
                            }
                        }
                        transformedRows.add(new Row(newHeader, valuesJoin));
                    }
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeSortedMergeInnerJoin(InnerJoin innerJoin, Table tableA, Table tableB) throws PhysicalException {
        Filter filter = innerJoin.getFilter();
        List<String> joinColumns = innerJoin.getJoinColumns();
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
        if (innerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
            }
            joinColumns = new ArrayList<>();
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(innerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(innerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
    
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
        
        if (filter != null) {
            joinColumns = new ArrayList<>();
            List<Pair<String, String>> pairs = FilterUtils.getJoinColumnsFromFilter(filter);
            if (pairs.isEmpty()) {
                throw new InvalidOperatorParameterException("on condition in join operator has no join columns.");
            }
            for(Pair<String, String> p : pairs) {
                if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                    joinColumns.add(p.k.replaceFirst(innerJoin.getPrefixA() + '.', ""));
                } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                    joinColumns.add(p.v.replaceFirst(innerJoin.getPrefixA() + '.', ""));
                } else {
                    throw new InvalidOperatorParameterException("invalid join path filter input.");
                }
            }
        }
        
        boolean isAscendingSorted;
        int flagA = RowUtils.checkRowsSortedByColumns(rowsA, innerJoin.getPrefixA(), joinColumns);
        int flagB = RowUtils.checkRowsSortedByColumns(rowsB, innerJoin.getPrefixB(), joinColumns);
        if (flagA == -1 || flagB == -1) {
            throw new InvalidOperatorParameterException("input rows in merge join haven't be sorted.");
        } else if (flagA + flagB == 3) {
            throw new InvalidOperatorParameterException("input two rows in merge join shouldn't have different sort order.");
        } else if ((flagA == flagB)) {
            isAscendingSorted = flagA == 0 || flagA == 1;
        } else {
            isAscendingSorted = flagA == 1 || flagB == 1;
        }
        if (!isAscendingSorted) {
            for (int index = 0; index < rowsA.size(); index++) {
                rowsA.set(index, tableA.getRow(rowsA.size() - index - 1));
            }
            for (int index = 0; index < rowsB.size(); index++) {
                rowsB.set(index, tableB.getRow(rowsB.size() - index - 1));
            }
        }
        
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) {
            fieldsA.addAll(fieldsB);
            newHeader = new Header(fieldsA);
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB), innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns);
                if (flagAEqualB == 0) {
                    Object[] valuesA = rowsA.get(indexA).getValues();
                    Object[] valuesB = rowsB.get(indexB).getValues();
                    Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                    System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                    System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                    Row transformedRow = new Row(newHeader, valuesJoin);
                    if (FilterUtils.validate(filter, transformedRow)) {
                        transformedRows.add(transformedRow);
                    }
                    
                    if (indexA + 1 == rowsA.size()) {
                        if (indexB + 1 == rowsB.size()) {
                            break;
                        } else {
                            indexB++;
                        }
                    } else {
                        if (indexB + 1 == rowsB.size()) {
                            indexA++;
                            indexB = startIndexOfContinuousEqualValuesB;
                        } else {
                            int flagAEqualNextA = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsA.get(indexA + 1), innerJoin.getPrefixA(), innerJoin.getPrefixA(), joinColumns);
                            int flagBEqualNextB = RowUtils.compareRowsSortedByColumns(rowsB.get(indexB), rowsB.get(indexB + 1), innerJoin.getPrefixB(), innerJoin.getPrefixB(), joinColumns);
                            if (flagBEqualNextB == 0) {
                                indexB++;
                            } else {
                                indexA++;
                                if (flagAEqualNextA != 0) {
                                    indexB++;
                                    startIndexOfContinuousEqualValuesB = indexB;
                                } else {
                                    indexB = startIndexOfContinuousEqualValuesB;
                                }
                            }
                        }
                    }
                } else if (flagAEqualB == -1) {
                    indexA++;
                } else {
                    indexB++;
                    startIndexOfContinuousEqualValuesB = indexB;
                }
            }
        } else { // Join condition: natural or using
            int[] indexOfJoinColumnInTableB = new int[joinColumns.size()];
            int i = 0;
            flag1:
            for (Field fieldB : fieldsB) {
                for (String joinColumn : joinColumns) {
                    if (Objects.equals(fieldB.getName(), innerJoin.getPrefixB() + '.' + joinColumn)) {
                        indexOfJoinColumnInTableB[i++] = headerB.indexOf(fieldB);
                        continue flag1;
                    }
                }
                fieldsA.add(fieldB);
            }
            newHeader = new Header(fieldsA);
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB), innerJoin.getPrefixA(), innerJoin.getPrefixB(), joinColumns);
                if (flagAEqualB == 0) {
                    Object[] valuesA = rowsA.get(indexA).getValues();
                    Object[] valuesB = rowsB.get(indexB).getValues();
                    Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                    System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                    int k = valuesA.length;
                    flag3:
                    for (int j = 0; j < valuesB.length; j++) {
                        for (int index : indexOfJoinColumnInTableB) {
                            if (j == index) {
                                continue flag3;
                            }
                        }
                        valuesJoin[k++] = valuesB[j];
                    }
                    transformedRows.add(new Row(newHeader, valuesJoin));
        
                    int flagAEqualNextA = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsA.get(indexA + 1), innerJoin.getPrefixA(), innerJoin.getPrefixA(), joinColumns);
                    int flagBEqualNextB = RowUtils.compareRowsSortedByColumns(rowsB.get(indexB), rowsB.get(indexB + 1), innerJoin.getPrefixB(), innerJoin.getPrefixB(), joinColumns);
                    if (flagBEqualNextB == 0) {
                        indexB++;
                    } else {
                        indexA++;
                        if (flagAEqualNextA != 0) {
                            indexB++;
                            startIndexOfContinuousEqualValuesB = indexB;
                        } else {
                            indexB = startIndexOfContinuousEqualValuesB;
                        }
                    }
                } else if (flagAEqualB == -1) {
                    indexA++;
                } else {
                    indexB++;
                    startIndexOfContinuousEqualValuesB = indexB;
                }
            }
        }
        List<Row> reverseRows = new ArrayList<>(transformedRows);
        if (!isAscendingSorted) {
            for (int index = 0; index < reverseRows.size(); index++) {
                transformedRows.set(index, reverseRows.get(reverseRows.size() - index - 1));
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB) throws PhysicalException {
        switch (outerJoin.getJoinAlgType()) {
            case NestedLoopJoin:
                return executeNestedLoopOuterJoin(outerJoin, tableA, tableB);
            case HashJoin:
                return executeHashOuterJoin(outerJoin, tableA, tableB);
            case SortedMergeJoin:
                return executeSortedMergeOuterJoin(outerJoin, tableA, tableB);
            default:
                throw new PhysicalException("Unknown join algorithm type: " + outerJoin.getJoinAlgType());
        }
    }
    
    private RowStream executeNestedLoopOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB) throws PhysicalException {
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        Filter filter = outerJoin.getFilter();
        List<String> joinColumns = outerJoin.getJoinColumns();
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
        if (outerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural outer join operator should not have using operator");
            }
            joinColumns = new ArrayList<>();
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(outerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(outerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }

        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();

        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();

        Bitmap bitmapA = new Bitmap(rowsA.size());
        Bitmap bitmapB = new Bitmap(rowsB.size());

        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            fieldsA.addAll(fieldsB);
            newHeader = new Header(fieldsA);
            for (int indexA = 0; indexA < rowsA.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                Object[] valuesA = rowA.getValues();
                for (int indexB = 0; indexB < rowsB.size(); indexB++) {
                    Row rowB = rowsB.get(indexB);
                    Object[] valuesB = rowB.getValues();
                    Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                    System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                    System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                    Row transformedRow = new Row(newHeader, valuesJoin);
                    if (FilterUtils.validate(filter, transformedRow)) {
                        if (!bitmapA.get(indexA)) {
                            bitmapA.mark(indexA);
                        }
                        if (!bitmapB.get(indexB)) {
                            bitmapB.mark(indexB);
                        }
                        transformedRows.add(transformedRow);
                    }
                }
            }
        } else { // Join condition: natural or using
            List<Field> newFields = new ArrayList<>();
            int[] indexOfJoinColumnInTableB = new int[joinColumns.size()];
            int[] indexOfJoinColumnInTableA = new int[joinColumns.size()];
            int i = 0;
            int j = 0;
            if (outerType == OuterJoinType.RIGHT) {
                flag1:
                for (Field fieldA : fieldsA) {
                    for (String joinColumn : joinColumns) {
                        if (Objects.equals(fieldA.getName(), outerJoin.getPrefixA() + '.' + joinColumn)) {
                            indexOfJoinColumnInTableA[j++] = headerA.indexOf(fieldA);
                            continue flag1;
                        }
                    }
                    newFields.add(fieldA);
                }
                newFields.addAll(fieldsB);
            } else {
                newFields.addAll(fieldsA);
                flag2:
                for (Field fieldB : fieldsB) {
                    for (String joinColumn : joinColumns) {
                        if (Objects.equals(fieldB.getName(), outerJoin.getPrefixB() + '.' + joinColumn)) {
                            indexOfJoinColumnInTableB[i++] = headerB.indexOf(fieldB);
                            continue flag2;
                        }
                    }
                    newFields.add(fieldB);
                }
            }
            newHeader = new Header(newFields);
            for (int indexA = 0; indexA < rowsA.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                Object[] valuesA = rowA.getValues();
                flag2:
                for (int indexB = 0; indexB < rowsB.size(); indexB++) {
                    Row rowB = rowsB.get(indexB);
                    for (String joinColumn : joinColumns){
                        if (rowA.getValue(outerJoin.getPrefixA() + '.' + joinColumn) != rowB.getValue(outerJoin.getPrefixB() + '.' + joinColumn)){
                            continue flag2;
                        }
                    }
                    Object[] valuesB = rowB.getValues();
                    Object[] valuesJoin;
                    if (outerType == OuterJoinType.RIGHT) {
                        valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableA.length];
                        System.arraycopy(valuesB, 0, valuesJoin, valuesA.length - indexOfJoinColumnInTableA.length, valuesB.length);
                        int k = 0;
                        flag3:
                        for (int m = 0; m < valuesA.length; m++) {
                            for (int index : indexOfJoinColumnInTableA) {
                                if (m == index) {
                                    continue flag3;
                                }
                            }
                            valuesJoin[k++] = valuesA[m];
                        }
                    } else {
                        valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableB.length];
                        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                        int k = valuesA.length;
                        flag4:
                        for (int m = 0; m < valuesB.length; m++) {
                            for (int index : indexOfJoinColumnInTableB) {
                                if (m == index) {
                                    continue flag4;
                                }
                            }
                            valuesJoin[k++] = valuesB[m];
                        }
                    }
    
                    if (!bitmapA.get(indexA)) {
                        bitmapA.mark(indexA);
                    }
                    if (!bitmapB.get(indexB)) {
                        bitmapB.mark(indexB);
                    }
                    
                    transformedRows.add(new Row(newHeader, valuesJoin));
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = rowsB.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsA.size(); i++) {
                if (!bitmapA.get(i)) {
                    Row unMatchedRow = new Row(newHeader, buildUnMatchedRows(rowsA.get(i), anotherRowSize, true));
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = rowsA.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsB.size(); i++) {
                if (!bitmapB.get(i)) {
                    Row unMatchedRow = new Row(newHeader, buildUnMatchedRows(rowsB.get(i), anotherRowSize, false));
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private Object[] buildUnMatchedRows(Row halfRow, int anotherRowSize, boolean putLeft) {
        Object[] valuesJoin = new Object[halfRow.getValues().length + anotherRowSize];
        if (putLeft) {
            System.arraycopy(halfRow.getValues(), 0, valuesJoin, 0, halfRow.getValues().length);
        } else {
            System.arraycopy(halfRow.getValues(), 0, valuesJoin, anotherRowSize, halfRow.getValues().length);
        }
        return valuesJoin;
    }

    private RowStream executeHashOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB) throws PhysicalException {
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        Filter filter = outerJoin.getFilter();
        List<String> joinColumns = outerJoin.getJoinColumns();
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
        if (outerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural outer join operator should not have using operator");
            }
            joinColumns = new ArrayList<>();
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(outerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(outerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }
    
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
    
        String joinColumn;
        if (filter != null) {
            if (!filter.getType().equals(FilterType.Path)) {
                throw new InvalidOperatorParameterException("hash join only support one path filter yet.");
            }
            Pair<String, String> p = FilterUtils.getJoinColumnFromPathFilter((PathFilter) filter);
            if (p == null) {
                throw new InvalidOperatorParameterException("hash join only support equal path filter yet.");
            }
            if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                joinColumn = p.k.replaceFirst(outerJoin.getPrefixA() + '.', "");
            } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                joinColumn = p.v.replaceFirst(outerJoin.getPrefixA() + '.', "");
            } else {
                throw new InvalidOperatorParameterException("invalid hash join path filter input.");
            }
        } else {
            if (joinColumns.size() != 1) {
                throw new InvalidOperatorParameterException("hash join only support the number of join column is one yet.");
            }
            if (headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumns.get(0)) != -1 && headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumns.get(0)) != -1) {
                joinColumn = joinColumns.get(0);
            } else {
                throw new InvalidOperatorParameterException("invalid hash join column input.");
            }
        }
    
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
    
        Bitmap bitmapA = new Bitmap(rowsA.size());
        Bitmap bitmapB = new Bitmap(rowsB.size());
        
        HashMap<Integer, List<Row>> rowsBHashMap = new HashMap<>();
        HashMap<Integer, List<Integer>> indexOfRowBHashMap = new HashMap<>();
        for (int indexB = 0; indexB < rowsB.size(); indexB++) {
            int hash = rowsB.get(indexB).getValue(outerJoin.getPrefixB() + '.' + joinColumn).hashCode();
            List<Row> l = rowsBHashMap.containsKey(hash) ? rowsBHashMap.get(hash) : new ArrayList<>();
            List<Integer> il = rowsBHashMap.containsKey(hash) ? indexOfRowBHashMap.get(hash) : new ArrayList<>();
            l.add(rowsB.get(indexB));
            il.add(indexB);
            rowsBHashMap.put(hash, l);
            indexOfRowBHashMap.put(hash, il);
        }
        
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) { // Join condition: on
            fieldsA.addAll(fieldsB);
            newHeader = new Header(fieldsA);
            for (int indexA = 0; indexA < rowsB.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                int hash = rowsA.get(indexA).getValue(outerJoin.getPrefixA() + '.' + joinColumn).hashCode();
                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    List<Integer> hashIndexB = indexOfRowBHashMap.get(hash);
                    for (int i = 0; i < hashRowsB.size(); i++) {
                        Object[] valuesA = rowA.getValues();
                        Object[] valuesB = hashRowsB.get(i).getValues();
                        Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                        System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                        Row transformedRow = new Row(newHeader, valuesJoin);
                        int indexB = hashIndexB.get(i);
                        if (FilterUtils.validate(filter, transformedRow)) {
                            if (!bitmapA.get(indexA)) {
                                bitmapA.mark(indexA);
                            }
                            if (!bitmapB.get(indexB)) {
                                bitmapB.mark(indexB);
                            }
                            transformedRows.add(transformedRow);
                        }
                    }
                }
            }
        } else { // Join condition: natural or using
            List<Field> newFields = new ArrayList<>();
            if (outerType == OuterJoinType.RIGHT) {
                for (Field fieldA : fieldsA) {
                    if (Objects.equals(fieldA.getName(), outerJoin.getPrefixA() + '.' + joinColumn)) {
                        continue;
                    }
                    newFields.add(fieldA);
                }
                newFields.addAll(fieldsB);
            } else {
                newFields.addAll(fieldsA);
                for (Field fieldB : fieldsB) {
                    if (Objects.equals(fieldB.getName(), outerJoin.getPrefixB() + '.' + joinColumn)) {
                        continue;
                    }
                    newFields.add(fieldB);
                }
            }
            newHeader = new Header(newFields);
            for (int indexA = 0; indexA < rowsB.size(); indexA++) {
                Row rowA = rowsA.get(indexA);
                int hash = rowsA.get(indexA).getValue(outerJoin.getPrefixA() + '.' + joinColumn).hashCode();
                if (rowsBHashMap.containsKey(hash)) {
                    List<Row> hashRowsB = rowsBHashMap.get(hash);
                    List<Integer> hashIndexB = indexOfRowBHashMap.get(hash);
                    for (int i = 0; i < hashRowsB.size(); i++) {
                        Object[] valuesA = rowA.getValues();
                        Object[] valuesB = hashRowsB.get(i).getValues();
                        Object[] valuesJoin = new Object[valuesA.length + valuesB.length - 1];
                        if (outerType == OuterJoinType.RIGHT) {
                            System.arraycopy(valuesB, 0, valuesJoin, valuesA.length - 1, valuesB.length);
                            int k = 0;
                            int index = headerA.indexOf(outerJoin.getPrefixA() + '.' + joinColumn);
                            for (int m = 0; m < valuesA.length; m++) {
                                if (m != index) {
                                    valuesJoin[k++] = valuesA[m];
                                }
                            }
                        } else {
                            System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                            int k = valuesA.length;
                            int index = headerB.indexOf(outerJoin.getPrefixB() + '.' + joinColumn);
                            for (int m = 0; m < valuesB.length; m++) {
                                if (m != index) {
                                    valuesJoin[k++] = valuesB[m];
                                }
                            }
                        }
                        
                        int indexB = hashIndexB.get(i);
                        if (!bitmapA.get(indexA)) {
                            bitmapA.mark(indexA);
                        }
                        if (!bitmapB.get(indexB)) {
                            bitmapB.mark(indexB);
                        }
                        
                        transformedRows.add(new Row(newHeader, valuesJoin));
                    }
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = rowsB.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsA.size(); i++) {
                if (!bitmapA.get(i)) {
                    Row unMatchedRow = new Row(newHeader, buildUnMatchedRows(rowsA.get(i), anotherRowSize, true));
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = rowsA.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsB.size(); i++) {
                if (!bitmapB.get(i)) {
                    Row unMatchedRow = new Row(newHeader, buildUnMatchedRows(rowsB.get(i), anotherRowSize, false));
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private RowStream executeSortedMergeOuterJoin(OuterJoin outerJoin, Table tableA, Table tableB) throws PhysicalException {
        OuterJoinType outerType = outerJoin.getOuterJoinType();
        Filter filter = outerJoin.getFilter();
        List<String> joinColumns = outerJoin.getJoinColumns();
        List<Field> fieldsA = new ArrayList<>(tableA.getHeader().getFields());
        List<Field> fieldsB = new ArrayList<>(tableB.getHeader().getFields());
        if (outerJoin.isNaturalJoin()) {
            if (!joinColumns.isEmpty()) {
                throw new InvalidOperatorParameterException("natural inner join operator should not have using operator");
            }
            joinColumns = new ArrayList<>();
            for (Field fieldA : fieldsA) {
                for (Field fieldB : fieldsB) {
                    String joinColumnA = fieldA.getName().replaceFirst(outerJoin.getPrefixA() + '.', "");
                    String joinColumnB = fieldB.getName().replaceFirst(outerJoin.getPrefixB() + '.', "");
                    if (joinColumnA.equals(joinColumnB)) {
                        joinColumns.add(joinColumnA);
                    }
                }
            }
        }
        if ((filter == null && joinColumns.isEmpty()) || (filter != null && !joinColumns.isEmpty())) {
            throw new InvalidOperatorParameterException("using(or natural) and on operator cannot be used at the same time");
        }
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
    
        List<Row> rowsA = tableA.getRows();
        List<Row> rowsB = tableB.getRows();
    
        if (filter != null) {
            joinColumns = new ArrayList<>();
            List<Pair<String, String>> pairs = FilterUtils.getJoinColumnsFromFilter(filter);
            if (pairs.isEmpty()) {
                throw new InvalidOperatorParameterException("on condition in join operator has no join columns.");
            }
            for(Pair<String, String> p : pairs) {
                if (headerA.indexOf(p.k) != -1 && headerB.indexOf(p.v) != -1) {
                    joinColumns.add(p.k.replaceFirst(outerJoin.getPrefixA() + '.', ""));
                } else if (headerA.indexOf(p.v) != -1 && headerB.indexOf(p.k) != -1) {
                    joinColumns.add(p.v.replaceFirst(outerJoin.getPrefixA() + '.', ""));
                } else {
                    throw new InvalidOperatorParameterException("invalid join path filter input.");
                }
            }
        }
    
        boolean isAscendingSorted;
        int flagA = RowUtils.checkRowsSortedByColumns(rowsA, outerJoin.getPrefixA(), joinColumns);
        int flagB = RowUtils.checkRowsSortedByColumns(rowsB, outerJoin.getPrefixB(), joinColumns);
        if (flagA == -1 || flagB == -1) {
            throw new InvalidOperatorParameterException("input rows in merge join haven't be sorted.");
        } else if (flagA + flagB == 3) {
            throw new InvalidOperatorParameterException("input two rows in merge join shouldn't have different sort order.");
        } else if ((flagA == flagB)) {
            isAscendingSorted = flagA == 0 || flagA == 1;
        } else {
            isAscendingSorted = flagA == 1 || flagB == 1;
        }
        if (!isAscendingSorted) {
            for (int index = 0; index < rowsA.size(); index++) {
                rowsA.set(index, tableA.getRow(rowsA.size() - index - 1));
            }
            for (int index = 0; index < rowsB.size(); index++) {
                rowsB.set(index, tableB.getRow(rowsB.size() - index - 1));
            }
        }
    
        Bitmap bitmapA = new Bitmap(rowsA.size());
        Bitmap bitmapB = new Bitmap(rowsB.size());
    
        Header newHeader;
        List<Row> transformedRows = new ArrayList<>();
        if (filter != null) {
            fieldsA.addAll(fieldsB);
            newHeader = new Header(fieldsA);
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB), outerJoin.getPrefixA(), outerJoin.getPrefixB(), joinColumns);
                if (flagAEqualB == 0) {
                    Object[] valuesA = rowsA.get(indexA).getValues();
                    Object[] valuesB = rowsB.get(indexB).getValues();
                    Object[] valuesJoin = new Object[valuesA.length + valuesB.length];
                    System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                    System.arraycopy(valuesB, 0, valuesJoin, valuesA.length, valuesB.length);
                    Row transformedRow = new Row(newHeader, valuesJoin);
                    if (FilterUtils.validate(filter, transformedRow)) {
                        if (!bitmapA.get(indexA)) {
                            bitmapA.mark(indexA);
                        }
                        if (!bitmapB.get(indexB)) {
                            bitmapB.mark(indexB);
                        }
                        transformedRows.add(transformedRow);
                    }
                
                    if (indexA + 1 == rowsA.size()) {
                        if (indexB + 1 == rowsB.size()) {
                            break;
                        } else {
                            indexB++;
                        }
                    } else {
                        if (indexB + 1 == rowsB.size()) {
                            indexA++;
                            indexB = startIndexOfContinuousEqualValuesB;
                        } else {
                            int flagAEqualNextA = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsA.get(indexA + 1), outerJoin.getPrefixA(), outerJoin.getPrefixA(), joinColumns);
                            int flagBEqualNextB = RowUtils.compareRowsSortedByColumns(rowsB.get(indexB), rowsB.get(indexB + 1), outerJoin.getPrefixB(), outerJoin.getPrefixB(), joinColumns);
                            if (flagBEqualNextB == 0) {
                                indexB++;
                            } else {
                                indexA++;
                                if (flagAEqualNextA != 0) {
                                    indexB++;
                                    startIndexOfContinuousEqualValuesB = indexB;
                                } else {
                                    indexB = startIndexOfContinuousEqualValuesB;
                                }
                            }
                        }
                    }
                } else if (flagAEqualB == -1) {
                    indexA++;
                } else {
                    indexB++;
                    startIndexOfContinuousEqualValuesB = indexB;
                }
            }
        } else { // Join condition: natural or using
            List<Field> newFields = new ArrayList<>();
            int[] indexOfJoinColumnInTableB = new int[joinColumns.size()];
            int[] indexOfJoinColumnInTableA = new int[joinColumns.size()];
            int i = 0;
            int j = 0;
            if (outerType == OuterJoinType.RIGHT) {
                flag1:
                for (Field fieldA : fieldsA) {
                    for (String joinColumn : joinColumns) {
                        if (Objects.equals(fieldA.getName(), outerJoin.getPrefixA() + '.' + joinColumn)) {
                            indexOfJoinColumnInTableA[j++] = headerA.indexOf(fieldA);
                            continue flag1;
                        }
                    }
                    newFields.add(fieldA);
                }
                newFields.addAll(fieldsB);
            } else {
                newFields.addAll(fieldsA);
                flag2:
                for (Field fieldB : fieldsB) {
                    for (String joinColumn : joinColumns) {
                        if (Objects.equals(fieldB.getName(), outerJoin.getPrefixB() + '.' + joinColumn)) {
                            indexOfJoinColumnInTableB[i++] = headerB.indexOf(fieldB);
                            continue flag2;
                        }
                    }
                    newFields.add(fieldB);
                }
            }
            newHeader = new Header(newFields);
            int indexA = 0;
            int indexB = 0;
            int startIndexOfContinuousEqualValuesB = 0;
            while (indexA < rowsA.size() && indexB < rowsB.size()) {
                int flagAEqualB = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsB.get(indexB), outerJoin.getPrefixA(), outerJoin.getPrefixB(), joinColumns);
                if (flagAEqualB == 0) {
                    Object[] valuesA = rowsA.get(indexA).getValues();
                    Object[] valuesB = rowsB.get(indexB).getValues();
                    Object[] valuesJoin;
                    if (outerType == OuterJoinType.RIGHT) {
                        valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableA.length];
                        System.arraycopy(valuesB, 0, valuesJoin, valuesA.length - indexOfJoinColumnInTableA.length, valuesB.length);
                        int k = 0;
                        flag3:
                        for (int m = 0; m < valuesA.length; m++) {
                            for (int index : indexOfJoinColumnInTableA) {
                                if (m == index) {
                                    continue flag3;
                                }
                            }
                            valuesJoin[k++] = valuesA[m];
                        }
                    } else {
                        valuesJoin = new Object[valuesA.length + valuesB.length - indexOfJoinColumnInTableB.length];
                        System.arraycopy(valuesA, 0, valuesJoin, 0, valuesA.length);
                        int k = valuesA.length;
                        flag4:
                        for (int m = 0; m < valuesB.length; m++) {
                            for (int index : indexOfJoinColumnInTableB) {
                                if (m == index) {
                                    continue flag4;
                                }
                            }
                            valuesJoin[k++] = valuesB[m];
                        }
                    }
                    
                    if (!bitmapA.get(indexA)) {
                        bitmapA.mark(indexA);
                    }
                    if (!bitmapB.get(indexB)) {
                        bitmapB.mark(indexB);
                    }
                    transformedRows.add(new Row(newHeader, valuesJoin));
                
                    int flagAEqualNextA = RowUtils.compareRowsSortedByColumns(rowsA.get(indexA), rowsA.get(indexA + 1), outerJoin.getPrefixA(), outerJoin.getPrefixA(), joinColumns);
                    int flagBEqualNextB = RowUtils.compareRowsSortedByColumns(rowsB.get(indexB), rowsB.get(indexB + 1), outerJoin.getPrefixB(), outerJoin.getPrefixB(), joinColumns);
                    if (flagBEqualNextB == 0) {
                        indexB++;
                    } else {
                        indexA++;
                        if (flagAEqualNextA != 0) {
                            indexB++;
                            startIndexOfContinuousEqualValuesB = indexB;
                        } else {
                            indexB = startIndexOfContinuousEqualValuesB;
                        }
                    }
                } else if (flagAEqualB == -1) {
                    indexA++;
                } else {
                    indexB++;
                    startIndexOfContinuousEqualValuesB = indexB;
                }
            }
        }
        List<Row> reverseRows = new ArrayList<>(transformedRows);
        if (!isAscendingSorted) {
            for (int index = 0; index < reverseRows.size(); index++) {
                transformedRows.set(index, reverseRows.get(reverseRows.size() - index - 1));
            }
        }
        
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.LEFT) {
            int anotherRowSize = rowsB.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsA.size(); i++) {
                if (!bitmapA.get(i)) {
                    Row unMatchedRow = new Row(newHeader, buildUnMatchedRows(rowsA.get(i), anotherRowSize, true));
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        if (outerType == OuterJoinType.FULL || outerType == OuterJoinType.RIGHT) {
            int anotherRowSize = rowsA.get(0).getValues().length;
            if (filter == null) {
                anotherRowSize -= joinColumns.size();
            }
            for (int i = 0; i < rowsB.size(); i++) {
                if (!bitmapB.get(i)) {
                    Row unMatchedRow = new Row(newHeader, buildUnMatchedRows(rowsB.get(i), anotherRowSize, false));
                    transformedRows.add(unMatchedRow);
                }
            }
        }
        return new Table(newHeader, transformedRows);
    }

    private static void writeToNewRow(Object[] values, Row row, Map<Field, Integer> fieldIndices) {
        List<Field> fields = row.getHeader().getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (row.getValue(i) == null) {
                continue;
            }
            values[fieldIndices.get(fields.get(i))] = row.getValue(i);
        }
    }

    private RowStream executeIntersectJoin(Join join, Table tableA, Table tableB) throws PhysicalException {
        Header headerA = tableA.getHeader();
        Header headerB = tableB.getHeader();
        List<Field> newFields = new ArrayList<>();
        Map<Field, Integer> fieldIndices = new HashMap<>();
        for (Field field: headerA.getFields()) {
            if (fieldIndices.containsKey(field)) {
                continue;
            }
            fieldIndices.put(field, newFields.size());
            newFields.add(field);
        }
        for (Field field: headerB.getFields()) {
            if (fieldIndices.containsKey(field)) {
                continue;
            }
            fieldIndices.put(field, newFields.size());
            newFields.add(field);
        }

        // 目前只支持使用时间戳和顺序
        if (join.getJoinBy().equals(Constants.TIMESTAMP)) {
            // 检查时间戳
            if (!headerA.hasTimestamp() || !headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by time should have timestamp.");
            }
            Header newHeader = new Header(Field.TIME, newFields);
            List<Row> newRows = new ArrayList<>();

            int index1 = 0, index2 = 0;
            while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
                Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                long timestamp;
                if (rowA.getTimestamp() == rowB.getTimestamp()) {
                    timestamp = rowA.getTimestamp();
                    writeToNewRow(values, rowA, fieldIndices);
                    writeToNewRow(values, rowB, fieldIndices);
                    index1++;
                    index2++;
                } else if (rowA.getTimestamp() < rowB.getTimestamp()) {
                    timestamp = rowA.getTimestamp();
                    writeToNewRow(values, rowA, fieldIndices);
                    index1++;
                } else {
                    timestamp = rowB.getTimestamp();
                    writeToNewRow(values, rowB, fieldIndices);
                    index2++;
                }
                newRows.add(new Row(newHeader, timestamp, values));
            }

            for (; index1 < tableA.getRowSize(); index1++) {
                Row rowA = tableA.getRow(index1);
                Object[] values = new Object[newHeader.getFieldSize()];
                writeToNewRow(values, rowA, fieldIndices);
                newRows.add(new Row(newHeader, rowA.getTimestamp(), values));
            }

            for (; index2 < tableB.getRowSize(); index2++) {
                Row rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                writeToNewRow(values, rowB, fieldIndices);
                newRows.add(new Row(newHeader, rowB.getTimestamp(), values));
            }
            return new Table(newHeader, newRows);
        } else if (join.getJoinBy().equals(Constants.ORDINAL)) {
            if (headerA.hasTimestamp() || headerB.hasTimestamp()) {
                throw new InvalidOperatorParameterException("row streams for join operator by ordinal shouldn't have timestamp.");
            }
            Header newHeader = new Header(newFields);
            List<Row> newRows = new ArrayList<>();

            int index1 = 0, index2 = 0;
            while (index1 < tableA.getRowSize() && index2 < tableB.getRowSize()) {
                Row rowA = tableA.getRow(index1), rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                writeToNewRow(values, rowA, fieldIndices);
                writeToNewRow(values, rowB, fieldIndices);
                index1++;
                index2++;
                newRows.add(new Row(newHeader, values));
            }
            for (; index1 < tableA.getRowSize(); index1++) {
                Row rowA = tableA.getRow(index1);
                Object[] values = new Object[newHeader.getFieldSize()];
                writeToNewRow(values, rowA, fieldIndices);
                newRows.add(new Row(newHeader, values));
            }

            for (; index2 < tableB.getRowSize(); index2++) {
                Row rowB = tableB.getRow(index2);
                Object[] values = new Object[newHeader.getFieldSize()];
                writeToNewRow(values, rowB, fieldIndices);
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
