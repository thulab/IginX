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
package cn.edu.tsinghua.iginx.engine.physical.storage.fault_tolerance.proposal.content;

public class RestoreConnectionProposalContent {

    private static final int NOT_SET = 0;

    private static final int NOT_ALIVE = 1;

    private static final int ALIVE = 2;

    private final long id;

    private int isAlive;

    public RestoreConnectionProposalContent(long id) {
        this.id = id;
        this.isAlive = NOT_SET;
    }

    public long getId() {
        return id;
    }

    public boolean isAlive() {
        return isAlive == ALIVE;
    }

    public void setAlive(boolean alive) {
        isAlive = alive ? ALIVE : NOT_ALIVE;
    }

    public boolean isSetAlive() {
        return isAlive != NOT_SET;
    }

}
