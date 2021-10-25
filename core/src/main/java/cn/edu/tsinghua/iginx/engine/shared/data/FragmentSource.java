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

import cn.edu.tsinghua.iginx.engine.shared.TimeSpan;
import cn.edu.tsinghua.iginx.metadata.entity.FragmentMeta;

public class FragmentSource extends AbstractSource {

    private final FragmentMeta fragment;

    public FragmentSource(FragmentMeta fragment) {
        super(SourceType.Fragment);
        if (fragment == null) {
            throw new IllegalArgumentException("fragment shouldn't be null");
        }
        this.fragment = fragment;
    }

    public FragmentMeta getFragment() {
        return fragment;
    }

    @Override
    public boolean hasTimestamp() {
        return true;
    }

    @Override
    public TimeSpan getLogicalTimeSpan() {
        return null;
    }
}
