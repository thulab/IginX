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

public enum FilterType {

    Key,
    Value,
    Path,
    Bool,  // holder

    And,
    Or,
    Not;

    public static boolean isLeafFilter(FilterType filterType) {
        return filterType == Key || filterType == Value || filterType == Path;
    }

    public static boolean isCompoundFilter(FilterType filterType) {
        return filterType != Key && filterType != Value && filterType != Path;
    }

    public static boolean isTimeFilter(Filter filter) {
        switch (filter.getType()) {
            case Value:
                return false;
            case Key:
                return true;
            case Not:
                NotFilter notFilter = (NotFilter) filter;
                return isTimeFilter(notFilter.getChild());
            case And:
                AndFilter andFilter = (AndFilter) filter;
                for (Filter f : andFilter.getChildren()) {
                    if (!isTimeFilter(f)) {
                        return false;
                    }
                }
                break;
            case Or:
                OrFilter orFilter = (OrFilter) filter;
                for (Filter f : orFilter.getChildren()) {
                    if (!isTimeFilter(f)) {
                        return false;
                    }
                }
                break;
        }
        return true;
    }

    public static boolean isValueFilter(Filter filter) {
        switch (filter.getType()) {
            case Value:
                return true;
            case Key:
                return false;
            case Not:
                NotFilter notFilter = (NotFilter) filter;
                return isValueFilter(notFilter.getChild());
            case And:
                AndFilter andFilter = (AndFilter) filter;
                for (Filter f : andFilter.getChildren()) {
                    if (!isValueFilter(f)) {
                        return false;
                    }
                }
                break;
            case Or:
                OrFilter orFilter = (OrFilter) filter;
                for (Filter f : orFilter.getChildren()) {
                    if (!isValueFilter(f)) {
                        return false;
                    }
                }
                break;
        }
        return true;
    }

}
