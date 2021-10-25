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

import cn.edu.tsinghua.iginx.thrift.DataType;

public class Value {

    private final DataType dataType;

    private boolean boolV;

    private int intV;

    private long longV;

    private float floatV;

    private double doubleV;

    private String binaryV;

    public Value(boolean boolV) {
        this.dataType = DataType.BOOLEAN;
        this.boolV = boolV;
    }

    public Value(int intV) {
        this.dataType = DataType.INTEGER;
        this.intV = intV;
    }

    public Value(long longV) {
        this.dataType = DataType.LONG;
        this.longV = longV;
    }

    public Value(float floatV) {
        this.dataType = DataType.FLOAT;
        this.floatV = floatV;
    }

    public Value(double doubleV) {
        this.dataType = DataType.DOUBLE;
        this.doubleV = doubleV;
    }

    public Value(String binaryV) {
        this.dataType = DataType.BINARY;
        this.binaryV = binaryV;
    }

    public DataType getDataType() {
        return dataType;
    }

    public boolean getBoolV() {
        return boolV;
    }

    public int getIntV() {
        return intV;
    }

    public long getLongV() {
        return longV;
    }

    public float getFloatV() {
        return floatV;
    }

    public double getDoubleV() {
        return doubleV;
    }

    public String getBinaryV() {
        return binaryV;
    }

}
