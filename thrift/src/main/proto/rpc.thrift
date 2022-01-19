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
namespace java cn.edu.tsinghua.iginx.thrift
namespace py iginx.thrift.rpc

enum DataType {
    BOOLEAN,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BINARY,
}

enum AggregateType {
    MAX,
    MIN,
    SUM,
    COUNT,
    AVG,
    FIRST_VALUE,
    LAST_VALUE,
    FIRST,
    LAST
}

enum SqlType {
    Unknown,
    Insert,
    Delete,
    SimpleQuery,
    AggregateQuery,
    DownsampleQuery,
    ValueFilterQuery,
    NotSupportQuery,
    GetReplicaNum,
    AddStorageEngines,
    CountPoints,
    ClearData,
    DeleteTimeSeries,
    ShowTimeSeries,
    ShowClusterInfo,
    NewQuery,
}

enum AuthType {
    Read,
    Write,
    Admin,
    Cluster
}

enum UserType {
    Administrator,
    OrdinaryUser
}

struct Status {
    1: required i32 code
    2: optional string message
    3: optional list<Status> subStatus
}

struct OpenSessionReq {
    1: optional string username
    2: optional string password
}

struct OpenSessionResp {
    1: required Status status
    2: optional i64 sessionId
}

struct CloseSessionReq {
    1: required i64 sessionId
}

struct DeleteColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
}

struct InsertColumnRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> attributesList
}

struct InsertNonAlignedColumnRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> attributesList
}

struct InsertRowRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> attributesList
}

struct InsertNonAlignedRowRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> attributesList
}

struct DeleteDataInColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
}

struct QueryDataSet {
    1: required binary timestamps
    2: required list<binary> valuesList
    3: required list<binary> bitmapList
}

struct QueryDataReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
}

struct QueryDataResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<DataType> dataTypeList
    4: optional QueryDataSet queryDataSet
}

struct AddStorageEnginesReq {
    1: required i64 sessionId
    2: required list<StorageEngine> storageEngines
}

struct StorageEngine {
    1: required string ip
    2: required i32 port
    3: required string type
    4: required map<string, string> extraParams
}

struct AggregateQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
    5: required AggregateType aggregateType
}

struct AggregateQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<DataType> dataTypeList
    4: optional binary timestamps
    5: optional binary valuesList
}

struct LastQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
}

struct LastQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<DataType> dataTypeList
    4: optional QueryDataSet queryDataSet
}

struct DownsampleQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
    5: required AggregateType aggregateType
    6: required i64 precision
}

struct DownsampleQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<DataType> dataTypeList
    4: optional QueryDataSet queryDataSet
}

struct ShowColumnsReq {
    1: required i64 sessionId
}

struct ShowColumnsResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<DataType> dataTypeList
}

struct GetReplicaNumReq {
    1: required i64 sessionId
}

struct GetReplicaNumResp {
    1: required Status status
    2: optional i32 replicaNum
}


struct ExecuteSqlReq {
    1: required i64 sessionId
    2: required string statement
}

struct ExecuteSqlResp {
    1: required Status status
    2: required SqlType type
    3: optional list<string> paths
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
    6: optional binary timestamps
    7: optional binary valuesList
    8: optional i32 replicaNum
    9: optional i64 pointsNum;
    10: optional AggregateType aggregateType
    11: optional string parseErrorMsg
    12: optional i32 limit
    13: optional i32 offset
    14: optional string orderByPath
    15: optional bool ascending
    16: optional list<IginxInfo> iginxInfos
    17: optional list<StorageEngineInfo> storageEngineInfos
    18: optional list<MetaStorageInfo>  metaStorageInfos
    19: optional LocalMetaStorageInfo localMetaStorageInfo
}

struct UpdateUserReq {
    1: required i64 sessionId
    2: required string username
    3: optional string password
    4: optional set<AuthType> auths
}

struct AddUserReq {
    1: required i64 sessionId
    2: required string username
    3: required string password
    4: required set<AuthType> auths
}

struct DeleteUserReq {
    1: required i64 sessionId
    2: required string username
}

struct GetUserReq {
    1: required i64 sessionId
    2: optional list<string> usernames
}

struct GetUserResp {
    1: required Status status
    2: optional list<string> usernames
    3: optional list<UserType> userTypes
    4: optional list<set<AuthType>> auths
}

struct GetClusterInfoReq {
    1: required i64 sessionId
}

struct IginxInfo {
    1: required i64 id
    2: required string ip
    3: required i32 port
}

struct StorageEngineInfo {
    1: required i64 id
    2: required string ip
    3: required i32 port
    4: required string type
}

struct MetaStorageInfo {
    1: required string ip
    2: required i32 port
    3: required string type
}

struct LocalMetaStorageInfo {
    1: required string path
}

struct GetClusterInfoResp {
    1: required Status status
    2: optional list<IginxInfo> iginxInfos
    3: optional list<StorageEngineInfo> storageEngineInfos
    4: optional list<MetaStorageInfo>  metaStorageInfos
    5: optional LocalMetaStorageInfo localMetaStorageInfo
}

service IService {

    OpenSessionResp openSession(1:OpenSessionReq req);

    Status closeSession(1:CloseSessionReq req);

    Status deleteColumns(1:DeleteColumnsReq req);

    Status insertColumnRecords(1:InsertColumnRecordsReq req);

    Status insertNonAlignedColumnRecords(1:InsertNonAlignedColumnRecordsReq req);

    Status insertRowRecords(1:InsertRowRecordsReq req);

    Status insertNonAlignedRowRecords(1:InsertNonAlignedRowRecordsReq req);

    Status deleteDataInColumns(1:DeleteDataInColumnsReq req);

    QueryDataResp queryData(1:QueryDataReq req);

    Status addStorageEngines(1: AddStorageEnginesReq req);

    AggregateQueryResp aggregateQuery(1:AggregateQueryReq req);

    LastQueryResp lastQuery(1: LastQueryReq req);

    DownsampleQueryResp downsampleQuery(DownsampleQueryReq req);

    ShowColumnsResp showColumns(ShowColumnsReq req);

    GetReplicaNumResp getReplicaNum(GetReplicaNumReq req);

    ExecuteSqlResp executeSql(1: ExecuteSqlReq req);

    Status updateUser(1: UpdateUserReq req);

    Status addUser(1: AddUserReq req);

    Status deleteUser(1: DeleteUserReq req);

    GetUserResp getUser(1: GetUserReq req);

    GetClusterInfoResp getClusterInfo(1: GetClusterInfoReq req);
}
