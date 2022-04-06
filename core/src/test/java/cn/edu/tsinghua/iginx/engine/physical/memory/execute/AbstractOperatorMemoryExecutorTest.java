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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.data.Value;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.manager.FunctionManager;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;
import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public abstract class AbstractOperatorMemoryExecutorTest {

    private static class EmptySource implements Source {

        public static final EmptySource EMPTY_SOURCE = new EmptySource();

        @Override
        public SourceType getType() {
            return null;
        }

        @Override
        public Source copy() {
            return null;
        }
    }

    protected abstract OperatorMemoryExecutor getExecutor();

    private Table generateTableForUnaryOperator(boolean hasTimestamp) {
        Header header;
        List<Field> fields = Arrays.asList(
                new Field("a.a.b", DataType.INTEGER),
                new Field("a.b.c", DataType.INTEGER),
                new Field("a.a.c", DataType.INTEGER));
        if (hasTimestamp) {
            header = new Header(Field.TIME, fields);
        } else {
            header = new Header(fields);
        }
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            if (hasTimestamp) {
                rows.add(new Row(header, i, new Object[] {i, i + 1, i + 2}));
            } else {
                rows.add(new Row(header, new Object[] {i, i + 1, i + 2}));
            }
        }
        return new Table(header, rows);
    }

    @Test
    public void testProjectWithPattern() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.*"));
        RowStream stream = getExecutor().executeUnaryOperator(project, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasTimestamp());
        assertEquals(2, targetHeader.getFields().size());
        assertEquals("a.a.b", targetHeader.getFields().get(0).getName());
        assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
        assertEquals("a.a.b", targetHeader.getFields().get(0).getName());
        assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row.getTimestamp(), targetRow.getTimestamp());
            assertEquals(row.getValue(0), targetRow.getValue(0));
            assertEquals(row.getValue(2), targetRow.getValue(1));
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testProjectWithoutPattern() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.b"));
        RowStream stream = getExecutor().executeUnaryOperator(project, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasTimestamp());
        assertEquals(1, targetHeader.getFields().size());
        assertEquals("a.a.b", targetHeader.getFields().get(0).getName());
        assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row.getTimestamp(), targetRow.getTimestamp());
            assertEquals(row.getValue(0), targetRow.getValue(0));
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testProjectWithMixedPattern() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Project project = new Project(EmptySource.EMPTY_SOURCE, Arrays.asList("a.*.c", "a.a.b"));
        RowStream stream = getExecutor().executeUnaryOperator(project, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasTimestamp());
        assertEquals(3, targetHeader.getFields().size());
        for (Field field: table.getHeader().getFields()) {
            assertTrue(targetHeader.getFields().contains(field));
        }

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row.getTimestamp(), targetRow.getTimestamp());
            for (int i = 0; i < 3; i++) {
                assertEquals(row.getValue(i), targetRow.getValue(i));
            }
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testSelectWithTimeFilter() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Filter filter = new TimeFilter(Op.GE, 5);
        Select select = new Select(EmptySource.EMPTY_SOURCE, filter);
        RowStream stream = getExecutor().executeUnaryOperator(select, table);

        assertEquals(table.getHeader(), stream.getHeader());

        int index = 5;
        while (stream.hasNext()) {
            assertEquals(table.getRow(index), stream.next());
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testSelectWithValueFilter() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Filter filter = new ValueFilter("a.a.b", Op.NE, new Value(3));
        Select select = new Select(EmptySource.EMPTY_SOURCE, filter);
        RowStream stream = getExecutor().executeUnaryOperator(select, table);

        assertEquals(table.getHeader(), stream.getHeader());

        int index = 0;
        while (stream.hasNext()) {
            assertEquals(table.getRow(index), stream.next());
            index++;
            if (index == 3) {
                index++;
            }
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testSelectWithCompoundFilter() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Filter filter = new AndFilter(Arrays.asList(new TimeFilter(Op.LE, 5), new ValueFilter("a.a.b", Op.NE, new Value(3))));
        Select select = new Select(EmptySource.EMPTY_SOURCE, filter);
        RowStream stream = getExecutor().executeUnaryOperator(select, table);

        assertEquals(table.getHeader(), stream.getHeader());

        int index = 0;
        while (stream.hasNext()) {
            assertEquals(table.getRow(index), stream.next());
            index++;
            if (index == 3) {
                index++;
            }
        }
        assertEquals(6, index);
    }

    @Test
    public void testSortByTimeAsc() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Sort sort = new Sort(EmptySource.EMPTY_SOURCE, Constants.TIMESTAMP, Sort.SortType.ASC);
        RowStream stream = getExecutor().executeUnaryOperator(sort, table);
        assertEquals(table.getHeader(), stream.getHeader());
        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row, targetRow);
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testSortByTimeDesc() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Sort sort = new Sort(EmptySource.EMPTY_SOURCE, Constants.TIMESTAMP, Sort.SortType.DESC);
        RowStream stream = getExecutor().executeUnaryOperator(sort, table);
        assertEquals(table.getHeader(), stream.getHeader());
        int index = table.getRowSize();
        while (stream.hasNext()) {
            index--;
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row, targetRow);
        }
        assertEquals(0, index);
    }


    @Test(expected = InvalidOperatorParameterException.class)
    public void testSortByOtherField() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Sort sort = new Sort(EmptySource.EMPTY_SOURCE, "a.a.b", Sort.SortType.ASC);
        getExecutor().executeUnaryOperator(sort, table);
        fail();
    }

    @Test
    public void testLimit() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Limit limit = new Limit(EmptySource.EMPTY_SOURCE, 5, 2);
        RowStream stream = getExecutor().executeUnaryOperator(limit, table);
        assertEquals(table.getHeader(), stream.getHeader());
        int index = 2;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row, targetRow);
            index++;
        }
        assertEquals(7, index);
    }

    @Test
    public void testLimitWithOutOfRangeA() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Limit limit = new Limit(EmptySource.EMPTY_SOURCE, 100, 2);
        RowStream stream = getExecutor().executeUnaryOperator(limit, table);
        assertEquals(table.getHeader(), stream.getHeader());
        int index = 2;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row, targetRow);
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testLimitWithOutOfRangeB() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Limit limit = new Limit(EmptySource.EMPTY_SOURCE, 100, 200);
        RowStream stream = getExecutor().executeUnaryOperator(limit, table);
        assertEquals(table.getHeader(), stream.getHeader());
        assertFalse(stream.hasNext());
    }

    @Test
    public void testDownsample() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Downsample downsample = new Downsample(EmptySource.EMPTY_SOURCE, 3,
                new FunctionCall(FunctionManager.getInstance().getFunction("avg"), Collections.singletonList(new Value("a.a.b"))),
                new TimeRange(0, 11));
        RowStream stream = getExecutor().executeUnaryOperator(downsample, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasTimestamp());
        assertEquals(1, targetHeader.getFields().size());
        assertEquals("avg(a.a.b)", targetHeader.getFields().get(0).getName());
        assertEquals(DataType.DOUBLE, targetHeader.getFields().get(0).getType());

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            int sum = 0;
            int cnt = 0;
            while (cnt < 3 && index + cnt < table.getRowSize()) {
                sum += (int) table.getRow(index + cnt).getValue("a.a.b");
                cnt++;
            }
            assertEquals(sum * 1.0 / cnt, (double) targetRow.getValue(0), 0.01);
            index += cnt;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test(expected = InvalidOperatorParameterException.class)
    public void testDownsampleWithoutTimestamp() throws PhysicalException {
        Table table = generateTableForUnaryOperator(false);
        Downsample downsample = new Downsample(EmptySource.EMPTY_SOURCE, 3,
                new FunctionCall(
                        FunctionManager.getInstance().getFunction("max"),
                        Collections.singletonList(new Value("a.a.b"))
                ),
                new TimeRange(0, 11));
        getExecutor().executeUnaryOperator(downsample, table);
        fail();
    }

    @Test
    public void testMappingTransform() throws PhysicalException {
        Table table = generateTableForUnaryOperator(false);
        MappingTransform mappingTransform = new MappingTransform(EmptySource.EMPTY_SOURCE,
                new FunctionCall(
                        FunctionManager.getInstance().getFunction("last"),
                        Collections.singletonList(new Value("a.a.b"))
                )
        );

        RowStream stream = getExecutor().executeUnaryOperator(mappingTransform, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasTimestamp());
        assertEquals(2, targetHeader.getFields().size());
        assertEquals("path", targetHeader.getFields().get(0).getName());
        assertEquals(DataType.BINARY, targetHeader.getFields().get(0).getType());
        assertEquals("value", targetHeader.getFields().get(1).getName());
        assertEquals(DataType.BINARY, targetHeader.getFields().get(1).getType());

        assertTrue(stream.hasNext());

        Row targetRow = stream.next();
        Row row = table.getRow(table.getRowSize() - 1);
        assertEquals(row.getTimestamp(), targetRow.getTimestamp());
        assertEquals("a.a.b", targetRow.getAsValue("path").getBinaryVAsString());
        assertEquals("9", targetRow.getAsValue("value").getBinaryVAsString());
        assertFalse(stream.hasNext());
    }

    @Test
    public void testSetTransform() throws PhysicalException {
        Table table = generateTableForUnaryOperator(false);
        SetTransform setTransform = new SetTransform(EmptySource.EMPTY_SOURCE,
                new FunctionCall(
                        FunctionManager.getInstance().getFunction("avg"),
                        Collections.singletonList(new Value("a.a.b"))
                )
        );

        RowStream stream = getExecutor().executeUnaryOperator(setTransform, table);

        Header targetHeader = stream.getHeader();
        assertFalse(targetHeader.hasTimestamp());
        assertEquals(1, targetHeader.getFields().size());
        assertEquals("avg(a.a.b)", targetHeader.getFields().get(0).getName());
        assertEquals(DataType.DOUBLE, targetHeader.getFields().get(0).getType());

        assertTrue(stream.hasNext());

        Row targetRow = stream.next();

        int sum = 0;
        int cnt = 0;
        while (cnt < table.getRowSize()) {
            sum += (int) table.getRow(cnt).getValue("a.a.b");
            cnt++;
        }
        assertEquals(sum * 1.0 / cnt, (double) targetRow.getValue(0), 0.01);

        assertFalse(stream.hasNext());
    }

    @Test
    public void testJoin() throws PhysicalException {

    }

    @Test
    public void testUnion() throws PhysicalException {

    }

}
