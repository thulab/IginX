namespace java cn.edu.tsinghua.iginx.thrift

enum DataType {
    BOOLEAN,
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
}

enum StorageEngineType {
    IOTDB,
}

enum AggregateType {
    MAX_TIME,
    MAX_VALUE,
    MIN_TIME,
    MIN_VALUE,
    SUM,
    COUNT,
    AVG,
    FIRST_TIME,
    FIRST_VALUE,
    LAST_TIME,
    LAST_VALUE,
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

struct CreateDatabaseReq {
    1: required i64 sessionId
    2: required string databaseName
}

struct DropDatabaseReq {
    1: required i64 sessionId
    2: required string databaseName
}

struct AddColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
    3: optional list<map<string, string>> attributesList
}

struct DeleteColumnsReq {
    1: required i64 sessionId
    2: required list<string> paths
}

struct InsertRecordsReq {
    1: required i64 sessionId
    // TODO add prefixes
    2: required list<string> paths
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<DataType> dataTypeList
    6: optional list<map<string, string>> attributesList
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

struct AddStorageEngineReq {
    1: required i64 sessionId
    2: required string ip
    3: required i32 port
    4: required StorageEngineType type
    5: required map<string, string> extraParams
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
    4: required binary valuesList
}

service IService {
    OpenSessionResp openSession(1:OpenSessionReq req);

    Status closeSession(1:CloseSessionReq req);

    Status createDatabase(1:CreateDatabaseReq req);

    Status dropDatabase(1:DropDatabaseReq req);

    Status addColumns(1:AddColumnsReq req);

    Status deleteColumns(1:DeleteColumnsReq req);

    Status insertRecords(1:InsertRecordsReq req);

    Status deleteDataInColumns(1:DeleteDataInColumnsReq req);

    QueryDataResp queryData(1:QueryDataReq req);

    Status addStorageEngine(1: AddStorageEngineReq req);

    AggregateQueryResp aggregateQuery(1:AggregateQueryReq req);

}
