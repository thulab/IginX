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
from enum import Enum
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


class StatementExecuteDataSet(object):

    class State(Enum):
        HAS_MORE = 1,
        NO_MORE = 2,
        UNKNOWN = 3

    def __init__(self, session, query_id, columns, types, fetch_size, values_list, bitmap_list):
        self.__session = session
        self.__query_id = query_id
        self.__columns = columns
        self.__types = types
        self.__fetch_size = fetch_size
        self.__values_list = values_list
        self.__bitmap_list = bitmap_list
        self.__state = StatementExecuteDataSet.State.UNKNOWN
        self.__index = 0


    def fetch(self):
        if self.__bitmap_list and self.__index != len(self.__bitmap_list):
            return

        self.__bitmap_list = None
        self.__values_list = None
        self.__index = 0

        tp = self.__session._fetch(self.__query_id, self.__fetch_size)

        if tp[0]:
            self.__state = StatementExecuteDataSet.State.HAS_MORE
        else:
            self.__state = StatementExecuteDataSet.State.NO_MORE

        if tp[1]:
            self.__bitmap_list = tp[1].bitmapList
            self.__values_list = tp[1].valuesList


    def has_more(self):
        if self.__values_list and self.__index < len(self.__values_list):
            return True

        self.__bitmap_list = None
        self.__values_list = None
        self.__index = 0

        if self.__state == StatementExecuteDataSet.State.HAS_MORE or self.__state == StatementExecuteDataSet.State.UNKNOWN:
            self.fetch()

        return self.__values_list


    def next(self):
        if not self.has_more():
            return None

        values_buffer = self.__values_list[self.__index]
        bitmap_buffer = self.__bitmap_list[self.__index]
        self.__index += 1

        bitmap = Bitmap(len(self.__types), bitmap_buffer)
        value_parser = BytesParser(values_buffer)
        values = []
        for i in range(len(self.__types)):
            if bitmap.get(i):
                values.append(value_parser.next(self.__types[i]))
            else:
                values.append(None)
        return values



    def close(self):
        self.__session._close_statement(query_id=self.__query_id)

    def columns(self):
        return self.__columns

    def types(self):
        return self.__types