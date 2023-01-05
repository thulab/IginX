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
package cn.edu.tsinghua.iginx.engine.physical.memory.execute.utils;

import cn.edu.tsinghua.iginx.engine.physical.exception.InvalidOperatorParameterException;
import cn.edu.tsinghua.iginx.engine.physical.memory.execute.Table;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Header;
import cn.edu.tsinghua.iginx.engine.shared.data.read.Row;
import cn.edu.tsinghua.iginx.engine.shared.data.read.RowStream;

import java.util.List;
import java.util.Objects;

public class TableUtils {
	
	public static RowStream UnionTable(Table tableA, Table tableB) throws InvalidOperatorParameterException {
		Header headerA = tableA.getHeader();
		Header headerB = tableB.getHeader();
		if (headerA.getFieldSize() != headerB.getFieldSize()) {
			throw new InvalidOperatorParameterException("union tables should have the same number of fields.");
		}
		for (int i = 0; i < headerA.getFieldSize(); i++) {
			if (!Objects.equals(headerA.getField(i).getName(), headerB.getField(i).getName()) || headerA.getField(i).getType() != headerB.getField(i).getType()) {
				throw new InvalidOperatorParameterException("union tables should have the same fields.");
			}
		}
		List<Row> rowsA = tableA.getRows();
		List<Row> rowsB = tableB.getRows();
		flag:
		for (Row rowB : rowsB) {
			for (Row rowA : rowsA) {
				if (rowA.equals(rowB)) {
					continue flag;
				}
			}
			rowsA.add(rowB);
		}
		return new Table(headerA, rowsA);
	}
}
