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


import cn.edu.tsinghua.iginx.engine.physical.exception.PhysicalException;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Field;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;
import cn.edu.tsinghua.iginx.engine.shared.operator.Project;
import cn.edu.tsinghua.iginx.utils.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ProjectLazyStream extends UnaryLazyStream {

    private final Project project;

    private Header header;

    private Row nextRow = null;

    public ProjectLazyStream(Project project, RowStream stream) {
        super(stream);
        this.project = project;
    }

    @Override
    public Header getHeader() throws PhysicalException {
        if (header == null) {
            List<String> patterns = project.getPatterns();
            Header header = stream.getHeader();
            List<Field> targetFields = new ArrayList<>();

            for (Field field : header.getFields()) {
                for (String pattern : patterns) {
                    if (!StringUtils.isPattern(pattern)) {
                        if (pattern.equals(field.getFullName())) {
                            targetFields.add(field);
                        }
                    } else {
                        if (Pattern.matches(StringUtils.reformatPath(pattern), field.getFullName())) {
                            targetFields.add(field);
                        }
                    }
                }
            }
            this.header = new Header(header.getKey(), targetFields);
        }
        return header;
    }

    @Override
    public boolean hasNext() throws PhysicalException {
        if (nextRow == null) {
            nextRow = calculateNext();
        }
        return nextRow != null;
    }

    private Row calculateNext() throws PhysicalException {
        Header header = getHeader();
        List<Field> fields = header.getFields();
        while(stream.hasNext()) {
            Row row = stream.next();
            Object[] objects = new Object[fields.size()];
            boolean allNull = true;
            for (int i = 0; i < fields.size(); i++) {
                objects[i] = row.getValue(fields.get(i));
                if (allNull && objects[i] != null) {
                    allNull = false;
                }
            }
            if (allNull) {
                continue;
            }
            if (header.hasKey()) {
                return new Row(header, row.getKey(), objects);
            } else {
                return new Row(header, objects);
            }
        }
        return null;
    }

    @Override
    public Row next() throws PhysicalException {
        if (!hasNext()) {
            throw new IllegalStateException("row stream doesn't have more data!");
        }
        Row row = nextRow;
        nextRow = null;
        return row;
    }
}
