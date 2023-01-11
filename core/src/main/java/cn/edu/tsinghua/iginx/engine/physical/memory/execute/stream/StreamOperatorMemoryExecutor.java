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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.stream;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.physical.exception.UnexpectedOperatorException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.OperatorMemoryExecutor;
import cn.edu.tsinghua.iginx.engine.shared.Constants;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.*;

public class StreamOperatorMemoryExecutor implements OperatorMemoryExecutor {

    private StreamOperatorMemoryExecutor() {
    }

    public static StreamOperatorMemoryExecutor getInstance() {
        return StreamOperatorMemoryExecutor.StreamOperatorMemoryExecutorHolder.INSTANCE;
    }


    @Override
    public RowStream executeUnaryOperator(UnaryOperator operator, RowStream stream) throws PhysicalException {
        switch (operator.getType()) {
            case Project:
                return executeProject((Project) operator, stream);
            case Select:
                return executeSelect((Select) operator, stream);
            case Sort:
                return executeSort((Sort) operator, stream);
            case Limit:
                return executeLimit((Limit) operator, stream);
            case Downsample:
                return executeDownsample((Downsample) operator, stream);
            case RowTransform:
                return executeRowTransform((RowTransform) operator, stream);
            case SetTransform:
                return executeSetTransform((SetTransform) operator, stream);
            case MappingTransform:
                return executeMappingTransform((MappingTransform) operator, stream);
            case Rename:
                return executeRename((Rename) operator, stream);
            case Reorder:
                return executeReorder((Reorder) operator, stream);
            case AddSchemaPrefix:
                return executeAddSchemaPrefix((AddSchemaPrefix) operator, stream);
            default:
                throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
        }
    }

    @Override
    public RowStream executeBinaryOperator(BinaryOperator operator, RowStream streamA, RowStream streamB) throws PhysicalException {
        switch (operator.getType()) {
            case Join:
                return executeJoin((Join) operator, streamA, streamB);
            case CrossJoin:
                return executeCrossJoin((CrossJoin) operator, streamA, streamB);
            case InnerJoin:
                return executeInnerJoin((InnerJoin) operator, streamA, streamB);
            case OuterJoin:
                return executeOuterJoin((OuterJoin) operator, streamA, streamB);
            case Union:
                return executeUnion((Union) operator, streamA, streamB);
            default:
                throw new UnexpectedOperatorException("unknown unary operator: " + operator.getType());
        }
    }

    private RowStream executeProject(Project project, RowStream stream) {
        return new ProjectLazyStream(project, stream);
    }

    private RowStream executeSelect(Select select, RowStream stream) {
        return new SelectLazyStream(select, stream);
    }

    private RowStream executeSort(Sort sort, RowStream stream) throws PhysicalException {
        if (!sort.getSortBy().equals(Constants.KEY)) {
            throw new InvalidOperatorParameterException("sort operator is not support for field " + sort.getSortBy() + " except for " + Constants.KEY);
        }
        return new SortLazyStream(sort, stream);
    }

    private RowStream executeLimit(Limit limit, RowStream stream) {
        return new LimitLazyStream(limit, stream);
    }

    private RowStream executeDownsample(Downsample downsample, RowStream stream) throws PhysicalException {
        if (!stream.getHeader().hasKey()) {
            throw new InvalidOperatorParameterException("downsample operator is not support for row stream without timestamps.");
        }
        return new DownsampleLazyStream(downsample, stream);
    }

    private RowStream executeRowTransform(RowTransform rowTransform, RowStream stream) {
        return new RowTransformLazyStream(rowTransform, stream);
    }

    private RowStream executeSetTransform(SetTransform setTransform, RowStream stream) {
        return new SetTransformLazyStream(setTransform, stream);
    }

    private RowStream executeMappingTransform(MappingTransform mappingTransform, RowStream stream) {
        return new MappingTransformLazyStream(mappingTransform, stream);
    }

    private RowStream executeRename(Rename rename, RowStream stream) {
        return new RenameLazyStream(rename, stream);
    }

    private RowStream executeReorder(Reorder reorder, RowStream stream) {
        return new ReorderLazyStream(reorder, stream);
    }

    private RowStream executeAddSchemaPrefix(AddSchemaPrefix addSchemaPrefix, RowStream stream) {
        return new AddSchemaPrefixLazyStream(addSchemaPrefix, stream);
    }

    private RowStream executeJoin(Join join, RowStream streamA, RowStream streamB) throws PhysicalException {
        if (!join.getJoinBy().equals(Constants.KEY) && !join.getJoinBy().equals(Constants.ORDINAL)) {
            throw new InvalidOperatorParameterException("join operator is not support for field " + join.getJoinBy() + " except for " + Constants.KEY
                + " and " + Constants.ORDINAL);
        }
        return new JoinLazyStream(join, streamA, streamB);
    }

    private RowStream executeCrossJoin(CrossJoin crossJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new CrossJoinLazyStream(crossJoin, streamA, streamB);
    }

    private RowStream executeInnerJoin(InnerJoin innerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        switch (innerJoin.getJoinAlgType()) {
            case NestedLoopJoin:
                return executeNestedLoopInnerJoin(innerJoin, streamA, streamB);
            case HashJoin:
                return executeHashInnerJoin(innerJoin, streamA, streamB);
            case SortedMergeJoin:
                return executeSortedMergeInnerJoin(innerJoin, streamA, streamB);
            default:
                throw new PhysicalException("Unknown join algorithm type: " + innerJoin.getJoinAlgType());
        }
    }

    private RowStream executeNestedLoopInnerJoin(InnerJoin innerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new NestedLoopInnerJoinLazyStream(innerJoin, streamA, streamB);
    }

    private RowStream executeHashInnerJoin(InnerJoin innerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new HashInnerJoinLazyStream(innerJoin, streamA, streamB);
    }

    private RowStream executeSortedMergeInnerJoin(InnerJoin innerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new SortedMergeInnerJoinLazyStream(innerJoin, streamA, streamB);
    }

    private RowStream executeOuterJoin(OuterJoin outerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        switch (outerJoin.getJoinAlgType()) {
            case NestedLoopJoin:
                return executeNestedLoopOuterJoin(outerJoin, streamA, streamB);
            case HashJoin:
                return executeHashOuterJoin(outerJoin, streamA, streamB);
            case SortedMergeJoin:
                return executeSortedMergeOuterJoin(outerJoin, streamA, streamB);
            default:
                throw new PhysicalException("Unknown join algorithm type: " + outerJoin.getJoinAlgType());
        }
    }

    private RowStream executeNestedLoopOuterJoin(OuterJoin outerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new NestedLoopOuterJoinLazyStream(outerJoin, streamA, streamB);
    }

    private RowStream executeHashOuterJoin(OuterJoin outerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new HashOuterJoinLazyStream(outerJoin, streamA, streamB);
    }

    private RowStream executeSortedMergeOuterJoin(OuterJoin outerJoin, RowStream streamA, RowStream streamB) throws PhysicalException {
        return new SortedMergeOuterJoinLazyStream(outerJoin, streamA, streamB);
    }

    private RowStream executeUnion(Union union, RowStream streamA, RowStream streamB) {
        return new UnionLazyStream(union, streamA, streamB);
    }

    private static class StreamOperatorMemoryExecutorHolder {

        private static final StreamOperatorMemoryExecutor INSTANCE = new StreamOperatorMemoryExecutor();

        private StreamOperatorMemoryExecutorHolder() {
        }

    }
}
