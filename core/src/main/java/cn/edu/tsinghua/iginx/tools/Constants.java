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
package cn.edu.tsinghua.iginx.tools;

public class Constants {

    public static final Long MILLS_PER_DAY = 86400000L;

    //line_id.14.machine_id.14009.ZT3383
    //machine_id.7057.GZ100
    public static final String[] PATH = {"line_id.*.machine_id.%s.%s", "machine_id.%s.%s"};

    public static final String DIR_NAME = "%s_%s_%s";

    public static final String CSV_FILE_NAME = "%s_%s.csv";
}

