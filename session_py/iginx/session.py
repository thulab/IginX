# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import logging
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from .cluster_info import ClusterInfo
from .dataset import QueryDataSet, AggregateQueryDataSet, StatementExecuteDataSet
from .thrift.rpc.IService import Client
from .thrift.rpc.ttypes import (
    OpenSessionReq,
    CloseSessionReq,
    AddUserReq,
    UpdateUserReq,
    DeleteUserReq,
    GetClusterInfoReq,
    GetReplicaNumReq,
    LastQueryReq,
    ShowColumnsReq,
    AddStorageEnginesReq,
    DeleteColumnsReq,
    QueryDataReq,
    DeleteDataInColumnsReq,
    DownsampleQueryReq,
    AggregateQueryReq,
    InsertRowRecordsReq,
    InsertNonAlignedRowRecordsReq,
    InsertColumnRecordsReq,
    InsertNonAlignedColumnRecordsReq,
    ExecuteSqlReq,
    ExecuteStatementReq,
    FetchResultsReq,
    CloseStatementReq,

    StorageEngine,
)
from .time_series import TimeSeries
from .utils.bitmap import Bitmap
from .utils.byte_utils import timestamps_to_bytes, row_values_to_bytes, column_values_to_bytes, bitmap_to_bytes

logger = logging.getLogger("IginX")

class Session(object):

    SUCCESS_CODE = 200
    DEFAULT_USER = "root"
    DEFAULT_PASSWORD = "root"

    def __init__(self, host, port, user=DEFAULT_USER, password=DEFAULT_PASSWORD):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password

        self.__is_close = True
        self.__transport = None
        self.__client = None
        self.__session_id = None


    def open(self):
        if not self.__is_close:
            return

        self.__transport = TSocket.TSocket(self.__host, self.__port)

        if not self.__transport.isOpen():
            try:
                self.__transport.open()
            except TTransport.TTransportException as e:
                logger.exception("TTransportException!", exc_info=e)

        self.__client = Client(TBinaryProtocol.TBinaryProtocol(self.__transport))

        req = OpenSessionReq(self.__user, self.__password)

        try:
            resp = self.__client.openSession(req)
            Session.verify_status(resp.status)
            self.__session_id = resp.sessionId
            self.__is_close = False
        except Exception as e:
            self.__transport.close()
            logger.exception("session closed because: ", exc_info=e)


    def close(self):
        if self.__is_close:
            return

        req = CloseSessionReq(self.__session_id)
        try:
            self.__client.closeSession(req)
        except TTransport.TException as e:
            logger.exception(
                "Error occurs when closing session. Error message: ",
                exc_info=e,
            )
        finally:
            self.__is_close = True
            if self.__transport is not None:
                self.__transport.close()


    def list_time_series(self):
        req = ShowColumnsReq(sessionId=self.__session_id)
        resp = self.__client.showColumns(req)
        Session.verify_status(resp.status)

        time_series_list = []
        for i in range(len(resp.paths)):
            time_series_list.append(TimeSeries(resp.paths[i], resp.dataTypeList[i]))

        return time_series_list


    def add_storage_engine(self, ip, port, type, extra_params=None):
        self.batch_add_storage_engine([StorageEngine(ip, port, type, extraParams=extra_params)])


    def batch_add_storage_engine(self, storage_engines):
        req = AddStorageEnginesReq(sessionId=self.__session_id, storageEngines=storage_engines)
        status = self.__client.addStorageEngines(req)
        Session.verify_status(status)


    def delete_time_series(self, path):
        self.batch_delete_time_series([path])


    def batch_delete_time_series(self, paths):
        req = DeleteColumnsReq(sessionId=self.__session_id, paths=paths)
        status = self.__client.deleteColumns(req)
        Session.verify_status(status)


    def get_replica_num(self):
        req = GetReplicaNumReq(sessionId=self.__session_id)
        resp = self.__client.getReplicaNum(req)
        Session.verify_status(resp.status)
        return resp.replicaNum


    def insert_row_records(self, paths, timestamps, values_list, data_type_list):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(timestamps) != len(values_list):
            raise RuntimeError("the sizes of timestamps and values_list should be equal")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        sorted_values_list = []
        for i in range(len(values_list)):
            sorted_values_list.append(values_list[index[i]])

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_data_type_list = []
        for i in index:
            sorted_data_type_list.append(data_type_list[index[i]])
        # 重排数据
        for i in range(len(sorted_values_list)):
            values = []
            for j in range(len(paths)):
                values.append(sorted_values_list[i][index[j]])
            sorted_values_list[i] = values


        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(timestamps)):
            values = sorted_values_list[i]
            values_buffer_list.append(row_values_to_bytes(values, sorted_data_type_list))
            bitmap = Bitmap(len(values))
            for j in range(len(values)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))


        req = InsertRowRecordsReq(sessionId=self.__session_id, paths=paths, timestamps=timestamps_to_bytes(timestamps), valuesList=values_buffer_list,
                                  bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list)
        status = self.__client.insertRowRecords(req)
        Session.verify_status(status)


    def insert_non_aligned_row_records(self , paths, timestamps, values_list, data_type_list):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(timestamps) != len(values_list):
            raise RuntimeError("the sizes of timestamps and values_list should be equal")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        sorted_values_list = []
        for i in range(len(values_list)):
            sorted_values_list.append(values_list[index[i]])

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_data_type_list = []
        for i in index:
            sorted_data_type_list.append(data_type_list[i])
        # 重排数据
        for i in range(len(sorted_values_list)):
            values = []
            for j in range(len(paths)):
                values.append(sorted_values_list[i][index[j]])
            sorted_values_list[i] = values


        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(timestamps)):
            values = sorted_values_list[i]
            values_buffer_list.append(row_values_to_bytes(values, sorted_data_type_list))
            bitmap = Bitmap(len(values))
            for j in range(len(values)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertNonAlignedRowRecordsReq(sessionId=self.__session_id, paths=paths, timestamps=timestamps_to_bytes(timestamps), valuesList=values_buffer_list,
                                  bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list)
        status = self.__client.insertNonAlignedRowRecords(req)
        Session.verify_status(status)


    def insert_column_records(self, paths, timestamps, values_list, data_type_list):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(paths) != len(values_list):
            raise RuntimeError("the sizes of paths and values_list should be equal")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        for i in range(len(values_list)):
            values = []
            for j in range(len(timestamps)):
                values.append(values_list[i][index[j]])
            values_list[i] = values

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_values_list = []
        sorted_data_type_list = []
        for i in index:
            sorted_values_list.append(values_list[index[i]])
            sorted_data_type_list.append(data_type_list[index[i]])

        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(paths)):
            values = sorted_values_list[i]
            values_buffer_list.append(column_values_to_bytes(values, sorted_data_type_list[i]))
            bitmap = Bitmap(len(timestamps))
            for j in range(len(timestamps)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertColumnRecordsReq(sessionId=self.__session_id, paths=paths, timestamps=timestamps_to_bytes(timestamps),
                                  valuesList=values_buffer_list,
                                  bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list)
        status = self.__client.insertColumnRecords(req)
        Session.verify_status(status)


    def insert_non_aligned_column_records(self, paths, timestamps, values_list, data_type_list):
        if len(paths) == 0 or len(timestamps) == 0 or len(values_list) == 0 or len(data_type_list) == 0:
            raise RuntimeError("invalid insert request")
        if len(paths) != len(data_type_list):
            raise RuntimeError("the sizes of paths and data_type_list should be equal")
        if len(paths) != len(values_list):
            raise RuntimeError("the sizes of paths and values_list should be equal")

        # 保证时间戳递增
        index = [x for x in range(len(timestamps))]
        index = sorted(index, key=lambda x: timestamps[x])
        timestamps = sorted(timestamps)
        for i in range(len(values_list)):
            values = []
            for j in range(len(timestamps)):
                values.append(values_list[i][index[j]])
            values_list[i] = values

        # 保证序列递增
        index = [x for x in range(len(paths))]
        index = sorted(index, key=lambda x: paths[x])
        paths = sorted(paths)
        # 重排数据类型
        sorted_values_list = []
        sorted_data_type_list = []
        for i in index:
            sorted_values_list.append(values_list[index[i]])
            sorted_data_type_list.append(data_type_list[index[i]])

        values_buffer_list = []
        bitmap_buffer_list = []
        for i in range(len(paths)):
            values = sorted_values_list[i]
            values_buffer_list.append(column_values_to_bytes(values, sorted_data_type_list[i]))
            bitmap = Bitmap(len(timestamps))
            for j in range(len(timestamps)):
                if values[j] is not None:
                    bitmap.set(j)
            bitmap_buffer_list.append(bitmap_to_bytes(bitmap.get_bytes()))

        req = InsertNonAlignedColumnRecordsReq(sessionId=self.__session_id, paths=paths, timestamps=timestamps_to_bytes(timestamps),
                                  valuesList=values_buffer_list,
                                  bitmapList=bitmap_buffer_list, dataTypeList=sorted_data_type_list)
        status = self.__client.insertNonAlignedColumnRecords(req)
        Session.verify_status(status)


    def query(self, paths, start_time, end_time):
        req = QueryDataReq(sessionId=self.__session_id, paths=Session.merge_and_sort_paths(paths),
                           startTime=start_time, endTime=end_time)
        resp = self.__client.queryData(req)
        Session.verify_status(resp.status)
        paths = resp.paths
        data_types = resp.dataTypeList
        raw_data_set = resp.queryDataSet
        return QueryDataSet(paths, data_types, raw_data_set.timestamps, raw_data_set.valuesList, raw_data_set.bitmapList)


    def last_query(self, paths, start_time=0):
        if len(paths) == 0:
            logger.warning("paths shouldn't be empty")
            return None
        req = LastQueryReq(sessionId=self.__session_id, paths=Session.merge_and_sort_paths(paths), startTime=start_time)
        resp = self.__client.lastQuery(req)
        Session.verify_status(resp.status)
        paths = resp.paths
        data_types = resp.dataTypeList
        raw_data_set = resp.queryDataSet
        return QueryDataSet(paths, data_types, raw_data_set.timestamps, raw_data_set.valuesList,
                            raw_data_set.bitmapList)


    def downsample_query(self, paths, start_time, end_time, type, precision):
        req = DownsampleQueryReq(sessionId=self.__session_id, paths=paths, startTime=start_time, endTime=end_time, aggregateType=type,
                                 precision=precision)
        resp = self.__client.downsampleQuery(req)
        Session.verify_status(resp.status)
        paths = resp.paths
        data_types = resp.dataTypeList
        raw_data_set = resp.queryDataSet
        return QueryDataSet(paths, data_types, raw_data_set.timestamps, raw_data_set.valuesList,
                                raw_data_set.bitmapList)


    def aggregate_query(self, paths, start_time, end_time, type):
        req = AggregateQueryReq(sessionId=self.__session_id, paths=paths, startTime=start_time, endTime=end_time, aggregateType=type)
        resp = self.__client.aggregateQuery(req)
        Session.verify_status(resp.status)
        return AggregateQueryDataSet(resp=resp, type=type)


    def delete_data(self, path, start_time, end_time):
        self.batch_delete_data([path], start_time, end_time)


    def batch_delete_data(self, paths, start_time, end_time):
        req = DeleteDataInColumnsReq(sessionId=self.__session_id, paths=paths, startTime=start_time, endTime=end_time)
        status = self.__client.deleteDataInColumns(req)
        Session.verify_status(status)


    def add_user(self, username, password, auths):
        req = AddUserReq(sessionId=self.__session_id, username=username, password=password, auths=auths)
        status = self.__client.addUser(req)
        Session.verify_status(status)


    def delete_user(self, username):
        req = DeleteUserReq(sessionId=self.__session_id, username=username)
        status = self.__client.deleteUser(req)
        Session.verify_status(status)


    def update_user(self, username, password=None, auths=None):
        req = UpdateUserReq(sessionId=self.__session_id, username=username)
        if password:
            req.password = password
        if auths:
            req.auths = auths
        status = self.__client.updateUser(req)
        Session.verify_status(status )


    def get_cluster_info(self):
        req = GetClusterInfoReq(sessionId=self.__session_id)
        resp = self.__client.getClusterInfo(req)
        Session.verify_status(resp.status)
        return ClusterInfo(resp)


    def execute_sql(self, statement):
        req = ExecuteSqlReq(sessionId=self.__session_id, statement=statement)
        resp = self.__client.executeSql(req)
        Session.verify_status(resp.status)
        return resp


    def execute_statement(self, statement, fetch_size=2147483647):
        req = ExecuteStatementReq(sessionId=self.__session_id, statement=statement, fetchSize=fetch_size)
        resp = self.__client.executeStatement(req)
        Session.verify_status(resp.status)
        return StatementExecuteDataSet(self, resp.queryId, resp.columns, resp.dataTypeList, fetch_size,
                                       resp.queryDataSet.valuesList, resp.queryDataSet.bitmapList)


    def _fetch(self, query_id, fetch_size):
        req = FetchResultsReq(sessionId=self.__session_id, queryId=query_id, fetchSize=fetch_size)
        resp = self.__client.fetchResults(req)
        Session.verify_status(resp.status)
        return (resp.hasMoreResults, resp.queryDataSet)

    def _close_statement(self, query_id):
        req = CloseStatementReq(sessionId=self.__session_id, queryId=query_id)
        status = self.__client.closeStatement(req)
        Session.verify_status(status)


    @staticmethod
    def verify_status(status):
        if status.code != Session.SUCCESS_CODE:
            raise RuntimeError("Error occurs: " + status.message)


    @staticmethod
    def merge_and_sort_paths(paths):
        for path in paths:
            if path == '*':
                return ['*']

        prefixes = []
        for path in paths:
            index = path.find('*')
            if index != -1:
                prefixes.append(path[:index])

        if len(prefixes) == 0:
            return sorted(paths)

        merged_paths = []
        for path in paths:
            if '*' not in path:
                skip = False
                for prefix in prefixes:
                    if path.startswith(prefix):
                        skip = True
                        break
                if skip:
                    continue
            merged_paths.append(path)

        return sorted(merged_paths)