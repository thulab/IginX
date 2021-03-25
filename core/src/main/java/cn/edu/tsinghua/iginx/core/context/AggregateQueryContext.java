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
package cn.edu.tsinghua.iginx.core.context;

import cn.edu.tsinghua.iginx.thrift.AggregateQueryReq;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class AggregateQueryContext extends RequestContext {

    private AggregateQueryReq req;

    public AggregateQueryContext(AggregateQueryReq req) {
        super(req.sessionId, ContextType.AggregateQuery);
        this.req = req;
    }

    public AggregateQueryReq getReq() {
        return req;
    }

    public void setReq(AggregateQueryReq req) {
        this.req = req;
    }
}
