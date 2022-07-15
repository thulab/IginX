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
package cn.edu.tsinghua.iginx.engine.shared.operator.filter;

import cn.edu.tsinghua.iginx.exceptions.SQLParserException;

public enum Op {

    GE,
    G,
    LE,
    L,
    E,
    NE,
    LIKE;


    public static Op getOpposite(Op op) {
        switch (op) {
            case NE:
                return E;
            case E:
                return NE;
            case GE:
                return L;
            case LE:
                return G;
            case G:
                return LE;
            case L:
                return GE;
            default:
                return op;
        }
    }

    public static Op getDirectionOpposite(Op op) {
        switch (op) {
            case GE:
                return LE;
            case LE:
                return GE;
            case G:
                return L;
            case L:
                return G;
            default:
                return op;
        }
    }

    public static Op str2Op(String op) {
        switch (op) {
            case "=":
            case "==":
                return E;
            case "!=":
            case "<>":
                return NE;
            case ">":
                return G;
            case ">=":
                return GE;
            case "<":
                return L;
            case "<=":
                return LE;
            case "like":
                return LIKE;
            default:
                throw new SQLParserException(String.format("Not support comparison operator %s", op));
        }
    }

    public static String op2Str(Op op) {
        switch (op) {
            case GE:
                return ">=";
            case G:
                return ">";
            case LE:
                return "<=";
            case L:
                return "<";
            case E:
                return "==";
            case NE:
                return "!=";
            case LIKE:
                return "like";
            default:
                return "";
        }
    }

}
