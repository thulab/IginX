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
package cn.edu.tsinghua.iginx.utils;

public class Bitmap {

  private final int size;

  private final byte[] bitmap;

  public Bitmap(int size) {
    this.size = size;
    this.bitmap = new byte[(int) Math.ceil(this.size * 1.0 / 8)];
  }

  public Bitmap(int size, byte[] bitmap) {
    this.size = size;
    this.bitmap = bitmap;
  }

  public void mark(int i) {
    if (i < 0 || i >= size) {
      throw new IllegalArgumentException("unexpected index");
    }
    int index = i / 8;
    int indexWithinByte = i % 8;
    bitmap[index] |= (1 << indexWithinByte);
  }

  public boolean get(int i) {
    if (i < 0 || i >= size) {
      throw new IllegalArgumentException("unexpected index");
    }
    int index = i / 8;
    int indexWithinByte = i % 8;
    return (bitmap[index] & (1 << indexWithinByte)) != 0;
  }

  public byte[] getBytes() {
    return this.bitmap;
  }

  public int getSize() {
    return size;
  }
}
