namespace java cn.edu.tsinghua.iginx.thrift

enum DataType {
    BOOLEAN,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    BINARY,
}

enum StorageEngineType {
    IOTDB,
    INFLUXDB,
}

enum AggregateType {
    MAX,
    MIN,
    SUM,
    COUNT,
    AVG,
    FIRST_VALUE,
    LAST_VALUE,
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
    ShowTimeSeries,
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
    3: required StorageEngineType type
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

struct ValueFilterQueryReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: required i64 startTime
    4: required i64 endTime
    5: required string booleanExpression
}

struct ValueFilterQueryResp {
    1: required Status status
    2: optional list<string> paths
    3: optional list<DataType> dataTypeList
    4: optional QueryDataSet queryDataSet
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
    4: optional binary timestamps
    5: optional binary valuesList
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

    ValueFilterQueryResp valueFilterQuery(1:ValueFilterQueryReq req);

    LastQueryResp lastQuery(1: LastQueryReq req);

    DownsampleQueryResp downsampleQuery(DownsampleQueryReq req);

    ShowColumnsResp showColumns(ShowColumnsReq req);

    GetReplicaNumResp getReplicaNum(GetReplicaNumReq req);

    ExecuteSqlResp executeSql(1: ExecuteSqlReq req);

    Status updateUser(1: UpdateUserReq req);

    Status addUser(1: AddUserReq req);

    Status deleteUser(1: DeleteUserReq req);

    GetUserResp getUser(1: GetUserReq req);
}
