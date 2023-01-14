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
package cn.edu.tsinghua.iginx.engine.shared.data.write;

import cn.edu.tsinghua.iginx.utils.Bitmap;

public final class ColumnDataView extends DataView {

    private final int[] biases;

    public ColumnDataView(RawData data, int startPathIndex, int endPathIndex, int startTimeIndex, int endTimeIndex) {
        super(data, startPathIndex, endPathIndex, startTimeIndex, endTimeIndex);
        this.biases = new int[this.endPathIndex - this.startPathIndex];
        for (int i = this.startPathIndex; i < this.endPathIndex; i++) {
            Bitmap bitmap = data.getBitmaps().get(i);
            for (int j = 0; j < this.startKeyIndex; j++) {
                if (bitmap.get(j)) {
                    biases[i - this.startPathIndex]++;
                }
            }
        }
    }

    @Override
    public Object getValue(int index1, int index2) { // 第一个维度为序列，第二个维度为数组中的偏移量
        checkPathIndexRange(index1);
        return ((Object[]) data.getValuesList()[index1 + startPathIndex])[biases[index1] + index2];
    }

    @Override
    public BitmapView getBitmapView(int index) { // 对于列数据来说，第一个维度为序列，所以要 checkPath
        checkPathIndexRange(index);
        return new BitmapView(data.getBitmaps().get(startPathIndex + index), startKeyIndex,
            endKeyIndex);
    }
}
