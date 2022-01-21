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
from .thrift.rpc.ttypes import SqlType, AggregateType, ExecuteSqlResp
from .utils.bitmap import Bitmap
from .utils.byte_utils import get_long_array, get_values_by_data_type, BytesParser


class Point(object):

    def __init__(self, path, type, timestamp, value):
        self.__path = path
        self.__type = type
        self.__timestamp = timestamp
        self.__value = value


    def get_path(self):
        return self.__path


    def get_type(self):
        return self.__type


    def get_timestamp(self):
        return self.__timestamp


    def get_value(self):
        return self.__value


class QueryDataSet(object):

    def __init__(self, paths, types, timestamps, values_list, bitmap_list):
        self.__paths = paths

        if timestamps is None:
            self.__timestamps = []
        else:
            self.__timestamps = get_long_array(timestamps)

        self.__values = []
        if values_list is not None:
            for i in range(len(values_list)):
                values = []
                bitmap = Bitmap(len(types), bitmap_list[i])
                value_parser = BytesParser(values_list[i])
                for j in range(len(types)):
                    if bitmap.get(j):
                        values.append(value_parser.next(types[j]))
                    else:
                        values.append(None)
                self.__values.append(values)


    def get_paths(self):
        return self.__paths


    def get_timestamps(self):
        return self.__timestamps


    def get_values(self):
        return self.__values


    def __str__(self):
        value = "Time\t"
        for path in self.__paths:
            value += path + "\t"
        value += "\n"

        for i in range(len(self.__timestamps)):
            value += str(self.__timestamps[i]) + "\t"
            for j in range(len(self.__paths)):
                if self.__values[i][j] is None:
                    value += "null\t"
                else:
                    value += str(self.__values[i][j]) + "\t"
            value += "\n"
        return value



class AggregateQueryDataSet(object):

    def __init__(self, resp, type):
        self.__type = type
        self.__paths = resp.paths
        self.__timestamps = None
        if resp.timestamps is not None:
            self.__timestamps = get_long_array(resp.timestamps)
        self.__values = get_values_by_data_type(resp.valuesList, resp.dataTypeList)


    def get_type(self):
        return self.__type


    def get_paths(self):
        return self.__paths


    def get_timestamps(self):
        return self.__timestamps


    def get_values(self):
        return self.__values


    def __str__(self):
        value = ""
        if self.__timestamps:
            for i in range(len(self.__timestamps)):
                value += "Time\t" + AggregateType._VALUES_TO_NAMES[self.__type] + "(" + self.__paths[i] + ")\n"
                value += str(self.__timestamps[i]) + "\t" + str(self.__values[i]) + "\n"
        else:
            for path in self.__paths:
                value += AggregateType._VALUES_TO_NAMES[self.__type] + "(" + path + ")\t"
            value += "\n"
            for v in self.__values:
                value += str(v) + "\t"
            value += "\n"
        return value


class SqlExecuteResult(object):

    def __init__(self, resp):
        self.__type = resp.type
        self.__parse_error_message = resp.parseErrorMsg

        if self.__type == SqlType.GetReplicaNum:
            self.__replica_num = resp.replicaNum
        elif self.__type == SqlType.CountPoints:
            self.__points_num = resp.pointsNum
        elif self.__type in [SqlType.AggregateQuery, SqlType.SimpleQuery, SqlType.DownsampleQuery, SqlType.ValueFilterQuery]:
            self._construct_query_result(resp)
        elif self.__type == SqlType.ShowTimeSeries:
            self.__paths = resp.paths
            self.__data_type_list = resp.dataTypeList
        elif self.__type == SqlType.ShowClusterInfo:
            self.__iginx_list = resp.iginxInfos
            self.__storage_engine_list = resp.storageEngineInfos
            self.__meta_storage_list = resp.metaStorageInfos
            self.__local_meta_storage = resp.localMetaStorageInfo


    def _construct_query_result(self, resp=ExecuteSqlResp()):
        self.__paths = resp.paths
        self.__data_type_list = resp.dataTypeList
        self.__limit = resp.limit
        self.__offset = resp.offset
        self.__order_by = resp.orderByPath
        self.__ascending = resp.ascending

        if resp.timestamps is not None:
            self.__timestamps = get_long_array(resp.timestamps)

        pass


    def is_query(self):
        return self.__type in [SqlType.AggregateQuery, SqlType.SimpleQuery, SqlType.DownsampleQuery, SqlType.ValueFilterQuery]


    def get_replica_num(self):
        return self.__replica_num


    def get_points_num(self):
        return self.__points_num


    def get_parse_error_msg(self):
        return self.__parse_error_message


    def get_iginx_list(self):
        return self.__iginx_list


    def get_storage_engine_list(self):
        return self.__storage_engine_list


    def get_meta_storage_list(self):
        return self.__meta_storage_list


    def get_local_meta_storage(self):
        return self.__local_meta_storage
