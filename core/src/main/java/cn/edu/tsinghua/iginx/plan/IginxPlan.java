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
package cn.edu.tsinghua.iginx.plan;

import static cn.edu.tsinghua.iginx.plan.IginxPlan.IginxPlanType.IGINX;

public abstract class IginxPlan {

	private long subPlanId;

	private IginxPlanType iginxPlanType;

	private boolean isQuery;

	private boolean canBeSplit;

	private boolean isSync;

	private long storageEngineId;

	protected IginxPlan(boolean isQuery) {
		this.iginxPlanType = IGINX;
		this.isQuery = isQuery;
	}

	public long getSubPlanId() {
		return subPlanId;
	}

	public void setSubPlanId(long subPlanId) {
		this.subPlanId = subPlanId;
	}

	public IginxPlanType getIginxPlanType() {
		return iginxPlanType;
	}

	public boolean isQuery() {
		return isQuery;
	}

	public boolean canBeSplit() {
		return canBeSplit;
	}

	public boolean isSync() {
		return isSync;
	}

	public long getStorageEngineId() {
		return storageEngineId;
	}

	public void setIginxPlanType(IginxPlanType iginxPlanType) {
		this.iginxPlanType = iginxPlanType;
	}

	public void setQuery(boolean isQuery) {
		this.isQuery = isQuery;
	}

	public void setCanBeSplit(boolean canBeSplit) {
		this.canBeSplit = canBeSplit;
	}

	public void setSync(boolean isSync) {
		this.isSync = isSync;
	}

	public void setStorageEngineId(long storageEngineId) {
		this.storageEngineId = storageEngineId;
	}

	public enum IginxPlanType {
		UNKNOWN,
		IGINX, DATABASE, CREATE_DATABASE, DROP_DATABASE, NON_DATABASE, COLUMN, ADD_COLUMNS,
		DELETE_COLUMNS, DATA, INSERT_RECORDS, INSERT_COLUMN_RECORDS, INSERT_ROW_RECORDS,
		DELETE_DATA_IN_COLUMNS, QUERY_DATA, AGGREGATE_QUERY, MAX, MIN, SUM, COUNT, AVG, FIRST, LAST,
		DOWNSAMPLE_QUERY, DOWNSAMPLE_MAX, DOWNSAMPLE_MIN, DOWNSAMPLE_SUM, DOWNSAMPLE_COUNT, DOWNSAMPLE_AVG,
		DOWNSAMPLE_FIRST, DOWNSAMPLE_LAST
	}
}
