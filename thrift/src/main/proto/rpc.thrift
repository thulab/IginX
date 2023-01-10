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
    Query,
    GetReplicaNum,
    AddStorageEngines,
    CountPoints,
    ClearData,
    DeleteTimeSeries,
    ShowTimeSeries,
    ShowClusterInfo,
    ShowRegisterTask,
    RegisterTask,
    DropTask,
    CommitTransformJob,
    ShowJobStatus,
    CancelJob,
    ShowEligibleJob
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

enum ExportType {
    Log,
    File,
    IginX
}

enum TaskType {
    IginX,
    Python
}

enum DataFlowType {
    Batch,
    Stream
}

enum JobState {
    JOB_UNKNOWN,
    JOB_FINISHED,
    JOB_CREATED,
    JOB_RUNNING,
    JOB_FAILING,
    JOB_FAILED,
    JOB_CLOSING,
    JOB_CLOSED
}

enum UDFType {
    UDAF,
    UDTF,
    UDSF,
    TRANSFORM
}

enum TimePrecision {
    YEAR,
    MONTH,
    WEEK,
    DAY,
    HOUR,
    MIN,
    S,
    MS,
    US,
    NS
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
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct InsertNonAlignedColumnRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct InsertRowRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct InsertNonAlignedRowRecordsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<DataType> dataTypeList
    7: optional list<map<string, string>> tagsList
    8: optional TimePrecision timePrecision
}

struct DeleteDataInColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
    5: optional map<string, list<string>> tagsList
    6: optional TimePrecision timePrecision
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
    5: optional map<string, list<string>> tagsList
    6: optional TimePrecision timePrecision
}

struct QueryDataResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
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
    6: optional map<string, list<string>> tagsList
    7: optional TimePrecision timePrecision
}

struct AggregateQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional binary timestamps
    6: optional binary valuesList
}

struct LastQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: optional map<string, list<string>> tagsList
    5: optional TimePrecision timePrecision
}

struct LastQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
}

struct DownsampleQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
    5: required AggregateType aggregateType
    6: required i64 precision
    7: optional map<string, list<string>> tagsList
    8: optional TimePrecision timePrecision
}

struct DownsampleQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
    5: optional QueryDataSet queryDataSet
}

struct ShowColumnsReq {
    1: required i64 sessionId
}

struct ShowColumnsResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<map<string, string>> tagsList
    4: optional list<DataType> dataTypeList
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
    4: optional list<map<string, string>> tagsList
    5: optional list<DataType> dataTypeList
    6: optional QueryDataSet queryDataSet
    7: optional binary timestamps
    8: optional binary valuesList
    9: optional i32 replicaNum
    10: optional i64 pointsNum;
    11: optional AggregateType aggregateType
    12: optional string parseErrorMsg
    13: optional i32 limit
    14: optional i32 offset
    15: optional string orderByPath
    16: optional bool ascending
    17: optional list<IginxInfo> iginxInfos
    18: optional list<StorageEngineInfo> storageEngineInfos
    19: optional list<MetaStorageInfo>  metaStorageInfos
    20: optional LocalMetaStorageInfo localMetaStorageInfo
    21: optional list<RegisterTaskInfo> registerTaskInfos
    22: optional i64 jobId
    23: optional JobState jobState
    24: optional list<i64> jobIdList
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

struct ExecuteStatementReq {
    1: required i64 sessionId
    2: required string statement
    3: optional i32 fetchSize
    4: optional i64 timeout
}

struct ExecuteStatementResp {
    1: required Status status
    2: required SqlType type
    3: optional i64 queryId
    4: optional list<string> columns
    5: optional list<map<string, string>> tagsList
    6: optional list<DataType> dataTypeList
    7: optional QueryDataSetV2 queryDataSet
}

struct QueryDataSetV2 {
    1: required list<binary> valuesList
    2: required list<binary> bitmapList
}

struct CloseStatementReq {
    1: required i64 sessionId
    2: required i64 queryId
}

struct FetchResultsReq {
    1: required i64 sessionId
    2: required i64 queryId
    3: optional i32 fetchSize
    4: optional i64 timeout
}

struct FetchResultsResp {
    1: required Status status
    2: required bool hasMoreResults
    3: optional QueryDataSetV2 queryDataSet
}

struct TaskInfo {
    1: required TaskType taskType
    2: required DataFlowType dataFlowType
    3: optional i64 timeout
    4: optional list<string> sqlList
    5: optional string pyTaskName
}

struct CommitTransformJobReq {
    1: required i64 sessionId
    2: required list<TaskInfo> taskList
    3: required ExportType exportType
    4: optional string fileName
}

struct CommitTransformJobResp {
    1: required Status status
    2: required i64 jobId
}

struct QueryTransformJobStatusReq {
    1: required i64 sessionId
    2: required i64 jobId
}

struct QueryTransformJobStatusResp {
    1: required Status status
    2: required JobState jobState
}

struct ShowEligibleJobReq {
    1: required i64 sessionId
    2: required JobState jobState
}

struct ShowEligibleJobResp {
    1: required Status status
    2: required list<i64> jobIdList
}

struct CancelTransformJobReq {
    1: required i64 sessionId
    2: required i64 jobId
}

struct RegisterTaskReq {
    1: required i64 sessionId
    2: required string name
    3: required string filePath
    4: required string className
    5: required UDFType type;
}

struct DropTaskReq {
    1: required i64 sessionId
    2: required string name
}

struct GetRegisterTaskInfoReq {
    1: required i64 sessionId
}

struct RegisterTaskInfo {
    1: required string name
    2: required string className
    3: required string fileName
    4: required string ip
    5: required UDFType type;
}

struct GetRegisterTaskInfoResp {
    1: required Status status
    2: optional list<RegisterTaskInfo> registerTaskInfoList
}

struct CurveMatchReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
    5: required list<double> curveQuery
    6: required i64 curveUnit
}

struct CurveMatchResp {
    1: required Status status
    2: optional string matchedPath
    3: optional i64 matchedTimestamp
}

enum DebugInfoType {
    GET_META,
}

struct GetMetaReq {
    1: required bool byCache
}

struct Fragment {
    1: required string storageUnitId
    2: required i64 startTime
    3: required i64 endTime
    4: required string startTs
    5: required string endTs
}

struct Storage {
    1: required i64 id
    2: required string ip
    3: required i64 port
    4: required string type
}

struct StorageUnit {
    1: required string id
    2: required string masterId
    3: required i64 storageId
}

struct GetMetaResp {
    1: required list<Fragment> fragments
    2: required list<Storage> storages
    3: required list<StorageUnit> storageUnits
}

struct DebugInfoReq {
    1: required DebugInfoType payloadType
    2: required binary payload
}

struct DebugInfoResp {
    1: required Status status
    2: optional binary payload
}

struct RemoveHistoryDataSourceReq {
    1: required i64 sessionId
    2: required i64 dummyStorageId
}

service IService {

    OpenSessionResp openSession(1: OpenSessionReq req);

    Status closeSession(1: CloseSessionReq req);

    Status deleteColumns(1: DeleteColumnsReq req);

    Status insertColumnRecords(1: InsertColumnRecordsReq req);

    Status insertNonAlignedColumnRecords(1: InsertNonAlignedColumnRecordsReq req);

    Status insertRowRecords(1: InsertRowRecordsReq req);

    Status insertNonAlignedRowRecords(1: InsertNonAlignedRowRecordsReq req);

    Status deleteDataInColumns(1: DeleteDataInColumnsReq req);

    QueryDataResp queryData(1: QueryDataReq req);

    Status addStorageEngines(1: AddStorageEnginesReq req);

    Status removeHistoryDataSource(1: RemoveHistoryDataSourceReq req);

    AggregateQueryResp aggregateQuery(1: AggregateQueryReq req);

    LastQueryResp lastQuery(1: LastQueryReq req);

    DownsampleQueryResp downsampleQuery(1: DownsampleQueryReq req);

    ShowColumnsResp showColumns(1: ShowColumnsReq req);

    GetReplicaNumResp getReplicaNum(1: GetReplicaNumReq req);

    ExecuteSqlResp executeSql(1: ExecuteSqlReq req);

    Status updateUser(1: UpdateUserReq req);

    Status addUser(1: AddUserReq req);

    Status deleteUser(1: DeleteUserReq req);

    GetUserResp getUser(1: GetUserReq req);

    GetClusterInfoResp getClusterInfo(1: GetClusterInfoReq req);

    ExecuteStatementResp executeStatement(1: ExecuteStatementReq req);

    FetchResultsResp fetchResults(1: FetchResultsReq req);

    Status closeStatement(1: CloseStatementReq req);

    CommitTransformJobResp commitTransformJob(1: CommitTransformJobReq req);

    QueryTransformJobStatusResp queryTransformJobStatus(1: QueryTransformJobStatusReq req);

    ShowEligibleJobResp showEligibleJob(1: ShowEligibleJobReq req);

    Status cancelTransformJob (1: CancelTransformJobReq req);

    Status registerTask(1: RegisterTaskReq req);

    Status dropTask(1: DropTaskReq req);

    GetRegisterTaskInfoResp getRegisterTaskInfo(1: GetRegisterTaskInfoReq req);

    CurveMatchResp curveMatch(1: CurveMatchReq req);

    DebugInfoResp debugInfo(1: DebugInfoReq req);
}
