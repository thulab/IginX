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
package cn.edu.tsinghua.iginx.session;

import cn.edu.tsinghua.iginx.thrift.DataType;
import cn.edu.tsinghua.iginx.thrift.LastQueryResp;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static cn.edu.tsinghua.iginx.utils.ByteUtils.getLongArrayFromByteBuffer;
import static cn.edu.tsinghua.iginx.utils.ByteUtils.getValuesByDataType;

public class LastQueryDataSet {

    private final List<Point> points;

    LastQueryDataSet(List<Point> points) {
        this.points = points;
    }

    LastQueryDataSet(LastQueryResp resp) {
        points = new LinkedList<>();
        List<String> paths = resp.getPaths();
        List<DataType> dataTypes = resp.getDataTypeList();
        long[] timestamps = getLongArrayFromByteBuffer(resp.timestamps);
        Object[] values = getValuesByDataType(resp.valuesList, dataTypes);
        for (int i = 0; i < paths.size(); i++) {
            Point point = new Point(paths.get(i), dataTypes.get(i), timestamps[i], values[i]);
            points.add(point);
        }
    }

    void addPoint(Point point) {
        points.add(point);
    }

    void addPoints(List<Point> points) {
        this.points.addAll(points);
    }

    public List<Point> getPoints() {
        return points;
    }

    public void print() {
        System.out.println("Start to Print ResultSets:");
        System.out.print("Time\tPath\tValue\t");
        System.out.println();

        for (Point point: points) {
            System.out.print(point.getTimestamp() + "\t");
            System.out.print(point.getPath() + "\t");
            if (point.getValue() instanceof byte[]) {
                System.out.print(new String((byte[]) point.getValue()) + "\t");
            } else {
                System.out.print(point.getValue() + "\t");
            }
            System.out.println();
        }

        System.out.println("Printing ResultSets Finished.");

    }

}
