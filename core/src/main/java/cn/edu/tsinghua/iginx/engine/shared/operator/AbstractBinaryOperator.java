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
package cn.edu.tsinghua.iginx.engine.shared.operator;

import cn.edu.tsinghua.iginx.engine.shared.operator.type.OperatorType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public abstract class AbstractBinaryOperator extends AbstractOperator implements BinaryOperator {

    private Source sourceA;

    private Source sourceB;

    public AbstractBinaryOperator(OperatorType type, Source sourceA, Source sourceB) {
        super(type);
        if (sourceA == null || sourceB == null) {
            throw new IllegalArgumentException("source shouldn't be null");
        }
        this.sourceA = sourceA;
        this.sourceB = sourceB;
    }

    public AbstractBinaryOperator(Source sourceA, Source sourceB) {
        this(OperatorType.Binary, sourceA, sourceB);
    }

    @Override
    public Source getSourceA() {
        return sourceA;
    }

    @Override
    public Source getSourceB() {
        return sourceB;
    }

    @Override
    public void setSourceA(Source source) {
        this.sourceA = source;
    }

    @Override
    public void setSourceB(Source source) {
        this.sourceB = source;
    }
}
