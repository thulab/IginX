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
import cn.edu.tsinghua.iginx.engine.shared.operator.type.JoinAlgType;
import cn.edu.tsinghua.iginx.engine.shared.operator.type.OuterJoinType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;
import cn.edu.tsinghua.iginx.engine.shared.source.SourceType;
import cn.edu.tsinghua.iginx.thrift.DataType;
import org.junit.Test;

import java.util.*;

import static cn.edu.tsinghua.iginx.engine.shared.Constants.PARAM_PATHS;
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
            header = new Header(Field.KEY, fields);
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

    private Table generateTableFromValues(boolean hasTimestamp, List<Field> fields, List<List<Object>> values) {
        Header header;
        if (hasTimestamp) {
            header = new Header(Field.KEY, fields);
        } else {
            header = new Header(fields);
        }
        List<Row> rows = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            if (hasTimestamp) {
                rows.add(new Row(header, i + 1, values.get(i).toArray()));
            } else {
                rows.add(new Row(header, values.get(i).toArray()));
            }
        }
        return new Table(header, rows);
    }

    private void assertStreamEqual(RowStream a, RowStream b) throws PhysicalException {
        Header headerA = a.getHeader();
        Header headerB = b.getHeader();
        assertEquals(headerA.getFieldSize(), headerB.getFieldSize());
        for (int i = 0; i < headerA.getFieldSize(); i++) {
            assertEquals(headerA.getField(i).getName(), headerB.getField(i).getName());
            assertEquals(headerA.getField(i).getType(), headerB.getField(i).getType());
        }

        while (a.hasNext() && b.hasNext()) {
            Row rowA = a.next();
            Row rowB = b.next();
            for (int i = 0; i < headerA.getFieldSize(); i++) {
                assertEquals(rowA.getValue(i), rowB.getValue(i));
            }
        }

        if (a.hasNext() || b.hasNext()) {
            // one of the streams has not been consumed.
            fail();
        }
    }

    @Test
    public void testCrossJoin() throws PhysicalException {
        Table tableA = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false)
            ));

        Table tableB = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false)
            ));

        {
            tableA.reset();
            tableB.reset();

            CrossJoin crossJoin = new CrossJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a",
                "b"
            );

            Table target = generateTableFromValues(
                false,
                Arrays.asList(
                    new Field("a.key", DataType.LONG),
                    new Field("a.a", DataType.INTEGER),
                    new Field("a.b", DataType.DOUBLE),
                    new Field("a.c", DataType.BOOLEAN),
                    new Field("b.key", DataType.LONG),
                    new Field("b.a", DataType.INTEGER),
                    new Field("b.b", DataType.DOUBLE),
                    new Field("b.c", DataType.BOOLEAN)
                ),
                Arrays.asList(
                    Arrays.asList(1L, 2, 2.1, true, 1L, 1, 1.1, true),
                    Arrays.asList(1L, 2, 2.1, true, 2L, 3, 3.1, false),
                    Arrays.asList(2L, 3, 3.1, false, 1L, 1, 1.1, true),
                    Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false)
                ));

            RowStream stream = getExecutor().executeBinaryOperator(crossJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }
    }

    @Test
    public void testInnerJoin() throws PhysicalException {
        Table tableA = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(3, 3.2, false),
                Arrays.asList(4, 4.1, true),
                Arrays.asList(5, 5.1, false),
                Arrays.asList(6, 6.1, true)
            ));

        Table tableB = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.k", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(3, 3.2, false),
                Arrays.asList(5, 5.1, true),
                Arrays.asList(7, 7.1, false),
                Arrays.asList(9, 9.1, true)
            ));

        Table target = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.k", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(2L, 3, 3.1, false, 3L, 3, 3.2, false),
                Arrays.asList(3L, 3, 3.2, false, 2L, 3, 3.1, false),
                Arrays.asList(3L, 3, 3.2, false, 3L, 3, 3.2, false),
                Arrays.asList(5L, 5, 5.1, false, 4L, 5, 5.1, true)
            ));

        Table usingTarget = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.k", DataType.INTEGER),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, false),
                Arrays.asList(3L, 3, 3.2, false, 3L, 3, false),
                Arrays.asList(5L, 5, 5.1, false, 4L, 5, true)
            ));

        {
            // NestedLoopJoin
            tableA.reset();
            tableB.reset();
            target.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                new PathFilter("a.a", Op.E, "b.k"),
                Collections.emptyList(),
                false,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }

        {
            // HashJoin
            tableA.reset();
            tableB.reset();
            target.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                new PathFilter("a.a", Op.E, "b.k"),
                Collections.emptyList(),
                false,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }

        {
            // SortedMergeJoin
            tableA.reset();
            tableB.reset();
            target.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                new PathFilter("a.a", Op.E, "b.k"),
                Collections.emptyList(),
                false,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }

        {
            // using & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            usingTarget.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                null,
                Collections.singletonList("b"),
                false,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, usingTarget);
        }

        {
            // using & HashJoin
            tableA.reset();
            tableB.reset();
            usingTarget.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                null,
                Collections.singletonList("b"),
                false,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, usingTarget);
        }

        {
            // using & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            usingTarget.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                null,
                Collections.singletonList("b"),
                false,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, usingTarget);
        }
        
        
    }

    @Test
    public void testOuterJoin() throws PhysicalException {
        Table tableA = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(4, 4.1, true),
                Arrays.asList(5, 5.1, false),
                Arrays.asList(6, 6.1, true),
                Arrays.asList(11, 11.1, false),
                Arrays.asList(12, 12.1, true),
                Arrays.asList(13, 13.1, false)
            ));

        Table tableB = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(5, 5.1, true),
                Arrays.asList(7, 7.1, false),
                Arrays.asList(9, 9.1, true),
                Arrays.asList(16, 16.1, false),
                Arrays.asList(17, 17.1, true),
                Arrays.asList(18, 18.1, false)
            ));

        Table leftTarget = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(1L, 2, 2.1, true, null, null, null, null),
                Arrays.asList(3L, 4, 4.1, true, null, null, null, null),
                Arrays.asList(5L, 6, 6.1, true, null, null, null, null),
                Arrays.asList(6L, 11, 11.1, false, null, null, null, null),
                Arrays.asList(7L, 12, 12.1, true, null, null, null, null),
                Arrays.asList(8L, 13, 13.1, false, null, null, null, null)
            ));

        Table rightTarget = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(null, null, null, null, 1L, 1, 1.1, true),
                Arrays.asList(null, null, null, null, 4L, 7, 7.1, false),
                Arrays.asList(null, null, null, null, 5L, 9, 9.1, true),
                Arrays.asList(null, null, null, null, 6L, 16, 16.1, false),
                Arrays.asList(null, null, null, null, 7L, 17, 17.1, true),
                Arrays.asList(null, null, null, null, 8L, 18, 18.1, false)
            ));

        Table fullTarget = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.b", DataType.DOUBLE),
                new Field("b.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(1L, 2, 2.1, true, null, null, null, null),
                Arrays.asList(3L, 4, 4.1, true, null, null, null, null),
                Arrays.asList(5L, 6, 6.1, true, null, null, null, null),
                Arrays.asList(6L, 11, 11.1, false, null, null, null, null),
                Arrays.asList(7L, 12, 12.1, true, null, null, null, null),
                Arrays.asList(8L, 13, 13.1, false, null, null, null, null),
                Arrays.asList(null, null, null, null, 1L, 1, 1.1, true),
                Arrays.asList(null, null, null, null, 4L, 7, 7.1, false),
                Arrays.asList(null, null, null, null, 5L, 9, 9.1, true),
                Arrays.asList(null, null, null, null, 6L, 16, 16.1, false),
                Arrays.asList(null, null, null, null, 7L, 17, 17.1, true),
                Arrays.asList(null, null, null, null, 8L, 18, 18.1, false)
            ));

        {
            // left & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            leftTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.LEFT,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, leftTarget);
        }

        {
            // left & HashJoin
            tableA.reset();
            tableB.reset();
            leftTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.LEFT,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, leftTarget);
        }

        {
            // left & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            leftTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.LEFT,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, leftTarget);
        }

        {
            // right & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            rightTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.RIGHT,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, rightTarget);
        }

        {
            // right & HashJoin
            tableA.reset();
            tableB.reset();
            rightTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.RIGHT,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, rightTarget);
        }

        {
            // right & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            rightTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.RIGHT,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, rightTarget);
        }

        {
            // full & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            fullTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.FULL,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, fullTarget);
        }

        {
            // full & HashJoin
            tableA.reset();
            tableB.reset();
            fullTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.FULL,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, fullTarget);
        }

        {
            // full & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            fullTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.FULL,
                new PathFilter("a.a", Op.E, "b.a"),
                Collections.emptyList(),
                false,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, fullTarget);
        }
    }

    @Test
    public void testNaturalJoin() throws PhysicalException {
        Table tableA = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2, 2.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(4, 4.1, true),
                Arrays.asList(5, 5.1, false),
                Arrays.asList(6, 6.1, true)
            ));

        Table tableB = generateTableFromValues(
            true,
            Arrays.asList(
                new Field("b.a", DataType.INTEGER),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(1, 1.1, true),
                Arrays.asList(3, 3.1, false),
                Arrays.asList(5, 5.1, true),
                Arrays.asList(7, 7.1, false),
                Arrays.asList(9, 9.1, true)
            ));

        Table target = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5.1, true)
            ));

        Table leftTarget = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.a", DataType.INTEGER),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3, 3.1, false, 2L, 3.1, false),
                Arrays.asList(4L, 5, 5.1, false, 3L, 5.1, true),
                Arrays.asList(1L, 2, 2.1, true, null, null, null),
                Arrays.asList(3L, 4, 4.1, true, null, null, null),
                Arrays.asList(5L, 6, 6.1, true, null, null, null)
            ));

        Table rightTarget = generateTableFromValues(
            false,
            Arrays.asList(
                new Field("a.key", DataType.LONG),
                new Field("a.b", DataType.DOUBLE),
                new Field("a.c", DataType.BOOLEAN),
                new Field("b.key", DataType.LONG),
                new Field("b.a", DataType.INTEGER),
                new Field("b.d", DataType.DOUBLE),
                new Field("b.e", DataType.BOOLEAN)
            ),
            Arrays.asList(
                Arrays.asList(2L, 3.1, false, 2L, 3, 3.1, false),
                Arrays.asList(4L, 5.1, false, 3L, 5, 5.1, true),
                Arrays.asList(null, null, null, 1L, 1, 1.1, true),
                Arrays.asList(null, null, null, 4L, 7, 7.1, false),
                Arrays.asList(null, null, null, 5L, 9, 9.1, true)
            ));

        {
            // inner & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            target.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }

        {
            // inner & HashJoin
            tableA.reset();
            tableB.reset();
            target.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }

        {
            // inner & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            target.reset();

            InnerJoin innerJoin = new InnerJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, target);
        }

        {
            // left & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            leftTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.LEFT,
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, leftTarget);
        }

        {
            // left & HashJoin
            tableA.reset();
            tableB.reset();
            leftTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.LEFT,
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, leftTarget);
        }

        {
            // left & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            leftTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.LEFT,
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, leftTarget);
        }

        {
            // right & NestedLoopJoin
            tableA.reset();
            tableB.reset();
            rightTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.RIGHT,
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.NestedLoopJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, rightTarget);
        }

        {
            // right & HashJoin
            tableA.reset();
            tableB.reset();
            rightTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.RIGHT,
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.HashJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, rightTarget);
        }

        {
            // right & SortedMergeJoin
            tableA.reset();
            tableB.reset();
            rightTarget.reset();

            OuterJoin outerJoin = new OuterJoin(
                EmptySource.EMPTY_SOURCE,
                EmptySource.EMPTY_SOURCE,
                "a", "b",
                OuterJoinType.RIGHT,
                null,
                Collections.emptyList(),
                true,
                JoinAlgType.SortedMergeJoin);

            RowStream stream = getExecutor().executeBinaryOperator(outerJoin, tableA, tableB);
            assertStreamEqual(stream, rightTarget);
        }
    }
    
    @Test
    public void testJoinWithTypeCast() throws PhysicalException {
        Table tableA = generateTableFromValues(
                true,
                Arrays.asList(
                        new Field("a.a", DataType.INTEGER),
                        new Field("a.b", DataType.DOUBLE),
                        new Field("a.c", DataType.BOOLEAN)
                ),
                Arrays.asList(
                        Arrays.asList(2, 2.0, true),
                        Arrays.asList(3, 3.0, false),
                        Arrays.asList(4, 4.0, true),
                        Arrays.asList(5, 5.0, false),
                        Arrays.asList(6, 6.0, true)
                ));
    
        Table tableB = generateTableFromValues(
                true,
                Arrays.asList(
                        new Field("b.b", DataType.INTEGER),
                        new Field("b.d", DataType.DOUBLE),
                        new Field("b.e", DataType.BOOLEAN)
                ),
                Arrays.asList(
                        Arrays.asList(1, 1.0, true),
                        Arrays.asList(3, 3.0, false),
                        Arrays.asList(5, 5.0, true),
                        Arrays.asList(7, 7.0, false),
                        Arrays.asList(9, 9.0, true)
                ));
    
        Table targetInner = generateTableFromValues(
                false,
                Arrays.asList(
                        new Field("a.key", DataType.LONG),
                        new Field("a.a", DataType.INTEGER),
                        new Field("a.b", DataType.DOUBLE),
                        new Field("a.c", DataType.BOOLEAN),
                        new Field("b.key", DataType.LONG),
                        new Field("b.b", DataType.INTEGER),
                        new Field("b.d", DataType.DOUBLE),
                        new Field("b.e", DataType.BOOLEAN)
                ),
                Arrays.asList(
                        Arrays.asList(2L, 3, 3.0, false, 2L, 3, 3.0, false),
                        Arrays.asList(4L, 5, 5.0, false, 3L, 5, 5.0, true)
                ));
    
        Table usingTargetInner = generateTableFromValues(
                false,
                Arrays.asList(
                        new Field("a.key", DataType.LONG),
                        new Field("a.a", DataType.INTEGER),
                        new Field("a.b", DataType.DOUBLE),
                        new Field("a.c", DataType.BOOLEAN),
                        new Field("b.key", DataType.LONG),
                        new Field("b.d", DataType.DOUBLE),
                        new Field("b.e", DataType.BOOLEAN)
                ),
                Arrays.asList(
                        Arrays.asList(2L, 3, 3.0, false, 2L, 3.0, false),
                        Arrays.asList(4L, 5, 5.0, false, 3L, 5.0, true)
                ));
    
        {
            // NestedLoopJoin
            tableA.reset();
            tableB.reset();
            targetInner.reset();
        
            InnerJoin innerJoin = new InnerJoin(
                    EmptySource.EMPTY_SOURCE,
                    EmptySource.EMPTY_SOURCE,
                    "a", "b",
                    new PathFilter("a.b", Op.E, "b.b"),
                    Collections.emptyList(),
                    false,
                    JoinAlgType.NestedLoopJoin);
        
            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, targetInner);
        }
    
        {
            // HashJoin
            tableA.reset();
            tableB.reset();
            targetInner.reset();
        
            InnerJoin innerJoin = new InnerJoin(
                    EmptySource.EMPTY_SOURCE,
                    EmptySource.EMPTY_SOURCE,
                    "a", "b",
                    new PathFilter("a.b", Op.E, "b.b"),
                    Collections.emptyList(),
                    false,
                    JoinAlgType.HashJoin);
        
            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, targetInner);
        }
    
        {
            // SortedMergeJoin
            tableA.reset();
            tableB.reset();
            targetInner.reset();
        
            InnerJoin innerJoin = new InnerJoin(
                    EmptySource.EMPTY_SOURCE,
                    EmptySource.EMPTY_SOURCE,
                    "a", "b",
                    new PathFilter("a.b", Op.E, "b.b"),
                    Collections.emptyList(),
                    false,
                    JoinAlgType.SortedMergeJoin);
        
            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, targetInner);
        }
    
        {
            // NestedLoopJoin
            tableA.reset();
            tableB.reset();
            usingTargetInner.reset();
    
            InnerJoin innerJoin = new InnerJoin(
                    EmptySource.EMPTY_SOURCE,
                    EmptySource.EMPTY_SOURCE,
                    "a", "b",
                    null,
                    Collections.singletonList("b"),
                    false,
                    JoinAlgType.NestedLoopJoin);
        
            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, usingTargetInner);
        }
    
        {
            // HashJoin
            tableA.reset();
            tableB.reset();
            usingTargetInner.reset();
        
            InnerJoin innerJoin = new InnerJoin(
                    EmptySource.EMPTY_SOURCE,
                    EmptySource.EMPTY_SOURCE,
                    "a", "b",
                    null,
                    Collections.singletonList("b"),
                    false,
                    JoinAlgType.HashJoin);
        
            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, usingTargetInner);
        }
    
        {
            // SortedMergeJoin
            tableA.reset();
            tableB.reset();
            usingTargetInner.reset();
        
            InnerJoin innerJoin = new InnerJoin(
                    EmptySource.EMPTY_SOURCE,
                    EmptySource.EMPTY_SOURCE,
                    "a", "b",
                    null,
                    Collections.singletonList("b"),
                    false,
                    JoinAlgType.SortedMergeJoin);
        
            RowStream stream = getExecutor().executeBinaryOperator(innerJoin, tableA, tableB);
            assertStreamEqual(stream, usingTargetInner);
        }
        
    }
    
    // for debug
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

    @Test
    public void testProjectWithPattern() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.*"), null);
        RowStream stream = getExecutor().executeUnaryOperator(project, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasKey());
        assertEquals(2, targetHeader.getFields().size());
        assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
        assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
        assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
        assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row.getKey(), targetRow.getKey());
            assertEquals(row.getValue(0), targetRow.getValue(0));
            assertEquals(row.getValue(2), targetRow.getValue(1));
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testReorder() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);

        // without wildcast
        {
            table.reset();
            Reorder reorder = new Reorder(EmptySource.EMPTY_SOURCE, Arrays.asList("a.a.b", "a.a.c", "a.b.c"));
            RowStream stream = getExecutor().executeUnaryOperator(reorder, table);
            Header targetHeader = stream.getHeader();
            assertTrue(targetHeader.hasKey());
            assertEquals(3, targetHeader.getFields().size());
            assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
            assertEquals("a.a.c", targetHeader.getFields().get(1).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());
            assertEquals("a.b.c", targetHeader.getFields().get(2).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(2).getType());

            int index = 0;
            while (stream.hasNext()) {
                Row targetRow = stream.next();
                assertEquals(index, targetRow.getKey());
                assertEquals(index, targetRow.getValue(0));
                assertEquals(index + 2, targetRow.getValue(1));
                assertEquals(index + 1, targetRow.getValue(2));
                index++;
            }
            assertEquals(table.getRowSize(), index);
        }

        // with wildcast 1
        {
            table.reset();
            Reorder reorder = new Reorder(EmptySource.EMPTY_SOURCE, Arrays.asList("a.a.*", "a.b.c"));
            RowStream stream = getExecutor().executeUnaryOperator(reorder, table);
            Header targetHeader = stream.getHeader();
            assertTrue(targetHeader.hasKey());
            assertEquals(3, targetHeader.getFields().size());
            assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
            assertEquals("a.a.c", targetHeader.getFields().get(1).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());
            assertEquals("a.b.c", targetHeader.getFields().get(2).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(2).getType());

            int index = 0;
            while (stream.hasNext()) {
                Row targetRow = stream.next();
                assertEquals(index, targetRow.getKey());
                assertEquals(index, targetRow.getValue(0));
                assertEquals(index + 2, targetRow.getValue(1));
                assertEquals(index + 1, targetRow.getValue(2));
                index++;
            }
            assertEquals(table.getRowSize(), index);
        }

        // with wildcast 2
        {
            table.reset();
            Reorder reorder = new Reorder(EmptySource.EMPTY_SOURCE, Arrays.asList("a.*", "a.b.c"));
            RowStream stream = getExecutor().executeUnaryOperator(reorder, table);
            Header targetHeader = stream.getHeader();
            assertTrue(targetHeader.hasKey());
            assertEquals(4, targetHeader.getFields().size());
            assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());
            assertEquals("a.b.c", targetHeader.getFields().get(1).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(1).getType());
            assertEquals("a.a.c", targetHeader.getFields().get(2).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(2).getType());
            assertEquals("a.b.c", targetHeader.getFields().get(3).getFullName());
            assertEquals(DataType.INTEGER, targetHeader.getFields().get(3).getType());

            int index = 0;
            while (stream.hasNext()) {
                Row targetRow = stream.next();
                assertEquals(index, targetRow.getKey());
                assertEquals(index, targetRow.getValue(0));
                assertEquals(index + 1, targetRow.getValue(1));
                assertEquals(index + 2, targetRow.getValue(2));
                assertEquals(index + 1, targetRow.getValue(3));
                index++;
            }
            assertEquals(table.getRowSize(), index);
        }
    }

    @Test
    public void testProjectWithoutPattern() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Project project = new Project(EmptySource.EMPTY_SOURCE, Collections.singletonList("a.a.b"), null);
        RowStream stream = getExecutor().executeUnaryOperator(project, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasKey());
        assertEquals(1, targetHeader.getFields().size());
        assertEquals("a.a.b", targetHeader.getFields().get(0).getFullName());
        assertEquals(DataType.INTEGER, targetHeader.getFields().get(0).getType());

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row.getKey(), targetRow.getKey());
            assertEquals(row.getValue(0), targetRow.getValue(0));
            index++;
        }
        assertEquals(table.getRowSize(), index);
    }

    @Test
    public void testProjectWithMixedPattern() throws PhysicalException {
        Table table = generateTableForUnaryOperator(true);
        Project project = new Project(EmptySource.EMPTY_SOURCE, Arrays.asList("a.*.c", "a.a.b"), null);
        RowStream stream = getExecutor().executeUnaryOperator(project, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasKey());
        assertEquals(3, targetHeader.getFields().size());
        for (Field field: table.getHeader().getFields()) {
            assertTrue(targetHeader.getFields().contains(field));
        }

        int index = 0;
        while (stream.hasNext()) {
            Row targetRow = stream.next();
            Row row = table.getRow(index);
            assertEquals(row.getKey(), targetRow.getKey());
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
        Filter filter = new KeyFilter(Op.GE, 5);
        Select select = new Select(EmptySource.EMPTY_SOURCE, filter, null);
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
        Select select = new Select(EmptySource.EMPTY_SOURCE, filter, null);
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
        Filter filter = new AndFilter(Arrays.asList(new KeyFilter(Op.LE, 5), new ValueFilter("a.a.b", Op.NE, new Value(3))));
        Select select = new Select(EmptySource.EMPTY_SOURCE, filter, null);
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
        Sort sort = new Sort(EmptySource.EMPTY_SOURCE, Constants.KEY, Sort.SortType.ASC);
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
        Sort sort = new Sort(EmptySource.EMPTY_SOURCE, Constants.KEY, Sort.SortType.DESC);
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

        Map<String, Value> params = new HashMap<>();
        params.put(PARAM_PATHS, new Value("a.a.b"));

        Downsample downsample = new Downsample(EmptySource.EMPTY_SOURCE, 3, 3,
                new FunctionCall(FunctionManager.getInstance().getFunction("avg"), params),
                new TimeRange(0, 11));
        RowStream stream = getExecutor().executeUnaryOperator(downsample, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasKey());
        assertEquals(1, targetHeader.getFields().size());
        assertEquals("avg(a.a.b)", targetHeader.getFields().get(0).getFullName());
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

        Map<String, Value> params = new HashMap<>();
        params.put(PARAM_PATHS, new Value("a.a.b"));

        Downsample downsample = new Downsample(EmptySource.EMPTY_SOURCE, 3, 3,
                new FunctionCall(FunctionManager.getInstance().getFunction("max"), params),
                new TimeRange(0, 11));
        getExecutor().executeUnaryOperator(downsample, table);
        fail();
    }

    @Test
    public void testMappingTransform() throws PhysicalException {
        Table table = generateTableForUnaryOperator(false);

        Map<String, Value> params = new HashMap<>();
        params.put(PARAM_PATHS, new Value("a.a.b"));

        MappingTransform mappingTransform = new MappingTransform(
            EmptySource.EMPTY_SOURCE,
            new FunctionCall(FunctionManager.getInstance().getFunction("last"), params)
        );

        RowStream stream = getExecutor().executeUnaryOperator(mappingTransform, table);

        Header targetHeader = stream.getHeader();
        assertTrue(targetHeader.hasKey());
        assertEquals(2, targetHeader.getFields().size());
        assertEquals("path", targetHeader.getFields().get(0).getFullName());
        assertEquals(DataType.BINARY, targetHeader.getFields().get(0).getType());
        assertEquals("value", targetHeader.getFields().get(1).getFullName());
        assertEquals(DataType.BINARY, targetHeader.getFields().get(1).getType());

        assertTrue(stream.hasNext());

        Row targetRow = stream.next();
        Row row = table.getRow(table.getRowSize() - 1);
        assertEquals(row.getKey(), targetRow.getKey());
        assertEquals("a.a.b", targetRow.getAsValue("path").getBinaryVAsString());
        assertEquals("9", targetRow.getAsValue("value").getBinaryVAsString());
        assertFalse(stream.hasNext());
    }

    @Test
    public void testSetTransform() throws PhysicalException {
        Table table = generateTableForUnaryOperator(false);

        Map<String, Value> params = new HashMap<>();
        params.put(PARAM_PATHS, new Value("a.a.b"));

        SetTransform setTransform = new SetTransform(
            EmptySource.EMPTY_SOURCE,
            new FunctionCall(FunctionManager.getInstance().getFunction("avg"), params)
        );

        RowStream stream = getExecutor().executeUnaryOperator(setTransform, table);

        Header targetHeader = stream.getHeader();
        assertFalse(targetHeader.hasKey());
        assertEquals(1, targetHeader.getFields().size());
        assertEquals("avg(a.a.b)", targetHeader.getFields().get(0).getFullName());
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

}
