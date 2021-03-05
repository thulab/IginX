namespace java cn.edu.tsinghua.iginx.thrift

struct Status {
  1: required i32 code
  2: optional string message
  3: optional list<Status> subStatus
}

struct QueryDataSet {
  // ByteBuffer for time column
  // TODO list<i64>
  1: required binary time
  // ByteBuffer for each column values
  2: required list<binary> valueList
  // Bitmap for each column to indicate whether it is a null value
  3: required list<binary> bitmapList
}

struct OpenSessionReq {
  1: optional string username
  2: optional string password
  3: optional map<string, string> configuration
}

struct OpenSessionResp {
  1: required Status status
  2: optional i64 sessionId
  3: optional map<string, string> configuration
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
  3: optional list<map<string, string>> attributes
}

struct DeleteColumnsReq {
  1: required i64 sessionId
  2: required list<string> paths
}

struct InsertRecordsReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required list<i64> timestamps
  4: required list<binary> values
  // TODO 考虑将 DataType 作为 required 参数
  5: optional list<map<string, string>> attributes
}

struct DeleteDataInColumnsReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required i64 startTime
  4: required i64 endTime
}

struct QueryDataReq {
  1: required i64 sessionId
  2: required list<string> paths
  3: required i64 startTime
  4: required i64 endTime
}

struct QueryDataResp {
  1: required Status status
  2: optional list<i32> types
  3: optional list<string> paths
  4: optional QueryDataSet queryDataSet
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
}
