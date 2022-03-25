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
package cn.edu.tsinghua.iginx.rest.query.aggregator;

public enum QueryAggregatorType {
  MAX("max"),
  MIN("min"),
  SUM("sum"),
  COUNT("count"),
  AVG("avg"),
  FIRST("first"),
  LAST("last"),
  DEV("dev"),
  DIFF("diff"),
  DIV("div"),
  FILTER("filter"),
  SAVE_AS("save_as"),
  RATE("rate"),
  SAMPLER("sampler"),
  PERCENTILE("percentile"),
  NONE("");
  private final String type;

  QueryAggregatorType(String type) {
    this.type = type;
  }

  public String getType() {
    return type;
  }

}
