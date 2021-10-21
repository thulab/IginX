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

import struct

from ..thrift.rpc.ttypes import DataType

def get_long_array(bytes):
    array = []
    parser = BytesParser(bytes)
    for i in range(len(bytes) // 8):
        array.append(parser.next_long())
    return array


def get_values_by_data_type(bytes, types):
    values = []
    parser = BytesParser(bytes)
    for type in types:
        if type == DataType.BOOLEAN:
            values.append(parser.next_boolean())
        elif type == DataType.INTEGER:
            values.append(parser.next_int())
        elif type == DataType.LONG:
            values.append(parser.next_long())
        elif type == DataType.FLOAT:
            values.append(parser.next_float())
        elif type == DataType.DOUBLE:
            values.append(parser.next_double())
        elif type == DataType.BINARY:
            values.append(parser.next_binary())
        else:
            raise RuntimeError("unknown data type " + type)

    return values


class BytesParser(object):

    def __init__(self, bytes):
        self.__bytes = bytes
        self.__index = 0


    def _next(self, length):
        bytes = self.__bytes[self.__index: self.__index + length]
        self.__index += length
        return bytes


    def next_int(self):
        bytes = self._next(4)
        return struct.unpack(">i", bytes)[0]


    def next_long(self):
        bytes = self._next(8)
        return struct.unpack(">q", bytes)[0]


    def next_binary(self):
        size = self.next_int()
        return self._next(size)


    def next_boolean(self):
        bytes = self._next(1)
        return struct.unpack(">?", bytes)[0]


    def next_float(self):
        bytes = self._next(4)
        return struct.unpack(">f", bytes)[0]


    def next_double(self):
        bytes = self._next(8)
        return struct.unpack(">d", bytes)[0]


    def next(self, type):
        if type == DataType.BOOLEAN:
            return self.next_boolean()
        elif type == DataType.INTEGER:
            return self.next_int()
        elif type == DataType.LONG:
            return self.next_long()
        elif type == DataType.FLOAT:
            return self.next_float()
        elif type == DataType.DOUBLE:
            return self.next_double()
        elif type == DataType.BINARY:
            return self.next_binary()
        else:
            raise RuntimeError("unknown data type " + type)