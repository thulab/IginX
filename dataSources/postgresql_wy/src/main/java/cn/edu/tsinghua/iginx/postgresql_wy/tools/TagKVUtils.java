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
package cn.edu.tsinghua.iginx.postgresql_wy.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.tag.AndTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.BaseTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.OrTagFilter;
import cn.edu.tsinghua.iginx.engine.shared.operator.tag.TagFilter;

public class TagKVUtils {

    public static String transformToFilterStr(TagFilter filter) {
        StringBuilder builder = new StringBuilder();
        transformToFilterStr(filter, builder);
        return builder.toString();
    }

    private static void transformToFilterStr(TagFilter filter, StringBuilder builder) {
        switch (filter.getType()) {
            case And:
                AndTagFilter andFilter = (AndTagFilter) filter;
                for (int i = 0; i < andFilter.getChildren().size(); i++) {
                    builder.append('(');
                    transformToFilterStr(andFilter.getChildren().get(i), builder);
                    builder.append(')');
                    if (i != andFilter.getChildren().size() - 1) { // 还不是最后一个
                        builder.append(" and ");
                    }
                }
                break;
            case Or:
                OrTagFilter orFilter = (OrTagFilter) filter;
                for (int i = 0; i < orFilter.getChildren().size(); i++) {
                    builder.append('(');
                    transformToFilterStr(orFilter.getChildren().get(i), builder);
                    builder.append(')');
                    if (i != orFilter.getChildren().size() - 1) { // 还不是最后一个
                        builder.append(" or ");
                    }
                }
                break;
            case Base:
                BaseTagFilter baseFilter = (BaseTagFilter) filter;
                builder.append(baseFilter.getTagKey());
                builder.append("=");
                builder.append(baseFilter.getTagValue());
                break;
            default:
                //do nothing
                break;
        }
    }
}