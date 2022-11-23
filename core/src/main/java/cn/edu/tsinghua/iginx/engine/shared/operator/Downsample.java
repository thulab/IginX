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

import cn.edu.tsinghua.iginx.engine.shared.TimeRange;
import cn.edu.tsinghua.iginx.engine.shared.function.FunctionCall;
import cn.edu.tsinghua.iginx.engine.shared.function.MappingType;
import cn.edu.tsinghua.iginx.engine.shared.source.Source;

public class Downsample extends AbstractUnaryOperator {

    private final long precision;
    
    private final long slideDistance;

    private final FunctionCall functionCall;

    private final TimeRange timeRange;
    
    public Downsample(Source source, long precision, long slideDistance, FunctionCall functionCall, TimeRange timeRange) {
        super(OperatorType.Downsample, source);
        if (precision <= 0) {
            throw new IllegalArgumentException("precision should be greater than zero");
        }
        if (slideDistance <= 0) {
            throw new IllegalArgumentException("slide distance should be greater than zero");
        }
        if (functionCall == null || functionCall.getFunction() == null) {
            throw new IllegalArgumentException("function shouldn't be null");
        }
        if (functionCall.getFunction().getMappingType() != MappingType.SetMapping) {
            throw new IllegalArgumentException("function should be set mapping function");
        }
        if (timeRange == null) {
            throw new IllegalArgumentException("timeRange shouldn't be null");
        }
        this.precision = precision;
        this.slideDistance = slideDistance;
        this.functionCall = functionCall;
        this.timeRange = timeRange;
    }

    public long getPrecision() {
        return precision;
    }
    
    public long getSlideDistance() {
        return slideDistance;
    }
    
    public FunctionCall getFunctionCall() {
        return functionCall;
    }

    public TimeRange getTimeRange() {
        return timeRange;
    }

    @Override
    public Operator copy() {
        return new Downsample(getSource().copy(), precision, slideDistance, functionCall.copy(), timeRange.copy());
    }
}
