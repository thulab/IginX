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

import java.util.Objects;

public class BitmapView {

    private final Bitmap bitmap;

    private final int start;

    private final int end;

    public BitmapView(Bitmap bitmap, int start, int end) {
        Objects.requireNonNull(bitmap);
        this.bitmap = bitmap;
        if (end <= start) {
            throw new IllegalArgumentException("end index should greater than start index");
        }
        if (end > bitmap.getSize()) {
            throw new IllegalArgumentException("end index shouldn't greater than the size of bitmap");
        }
        this.start = start;
        this.end = end;
    }

    public boolean get(int i) {
        if (i < 0 || i >= end - start) {
            throw new IllegalArgumentException("unexpected index");
        }
        return bitmap.get(i + start);
    }

    public int getSize() {
        return end - start;
    }

    public Bitmap getBitmap() {
        return bitmap;
    }
}
