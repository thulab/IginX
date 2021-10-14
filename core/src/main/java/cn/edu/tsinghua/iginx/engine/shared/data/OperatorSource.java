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
package cn.edu.tsinghua.iginx.engine.shared.data;

import cn.edu.tsinghua.iginx.engine.shared.TimeSpan;
import cn.edu.tsinghua.iginx.engine.shared.operator.Operator;

public class OperatorSource extends AbstractSource {

    private final Operator operator;

    private final TimeSpan logicalTimeSpan;

    public OperatorSource(Operator operator, TimeSpan logicalTimeSpan) {
        super(SourceType.Operator);
        if (operator == null) {
            throw new IllegalArgumentException("operator shouldn't be null");
        }
        this.operator = operator;
        this.logicalTimeSpan = logicalTimeSpan;
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public boolean hasTimestamp() {
        return logicalTimeSpan == null;
    }

    @Override
    public TimeSpan getLogicalTimeSpan() {
        return logicalTimeSpan;
    }
}
