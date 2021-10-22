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

from thrift.Thrift import TType, TMessageType, TFrozenDict, TException, TApplicationException
from thrift.protocol.TProtocol import TProtocolException
from thrift.TRecursive import fix_spec

import sys

from thrift.transport import TTransport
all_structs = []


class DataType(object):
    BOOLEAN = 0
    INTEGER = 1
    LONG = 2
    FLOAT = 3
    DOUBLE = 4
    BINARY = 5

    _VALUES_TO_NAMES = {
        0: "BOOLEAN",
        1: "INTEGER",
        2: "LONG",
        3: "FLOAT",
        4: "DOUBLE",
        5: "BINARY",
    }

    _NAMES_TO_VALUES = {
        "BOOLEAN": 0,
        "INTEGER": 1,
        "LONG": 2,
        "FLOAT": 3,
        "DOUBLE": 4,
        "BINARY": 5,
    }


class AggregateType(object):
    MAX = 0
    MIN = 1
    SUM = 2
    COUNT = 3
    AVG = 4
    FIRST_VALUE = 5
    LAST_VALUE = 6
    FIRST = 7
    LAST = 8

    _VALUES_TO_NAMES = {
        0: "MAX",
        1: "MIN",
        2: "SUM",
        3: "COUNT",
        4: "AVG",
        5: "FIRST_VALUE",
        6: "LAST_VALUE",
        7: "FIRST",
        8: "LAST",
    }

    _NAMES_TO_VALUES = {
        "MAX": 0,
        "MIN": 1,
        "SUM": 2,
        "COUNT": 3,
        "AVG": 4,
        "FIRST_VALUE": 5,
        "LAST_VALUE": 6,
        "FIRST": 7,
        "LAST": 8,
    }


class SqlType(object):
    Unknown = 0
    Insert = 1
    Delete = 2
    SimpleQuery = 3
    AggregateQuery = 4
    DownsampleQuery = 5
    ValueFilterQuery = 6
    NotSupportQuery = 7
    GetReplicaNum = 8
    AddStorageEngines = 9
    CountPoints = 10
    ClearData = 11
    ShowTimeSeries = 12
    ShowClusterInfo = 13

    _VALUES_TO_NAMES = {
        0: "Unknown",
        1: "Insert",
        2: "Delete",
        3: "SimpleQuery",
        4: "AggregateQuery",
        5: "DownsampleQuery",
        6: "ValueFilterQuery",
        7: "NotSupportQuery",
        8: "GetReplicaNum",
        9: "AddStorageEngines",
        10: "CountPoints",
        11: "ClearData",
        12: "ShowTimeSeries",
        13: "ShowClusterInfo",
    }

    _NAMES_TO_VALUES = {
        "Unknown": 0,
        "Insert": 1,
        "Delete": 2,
        "SimpleQuery": 3,
        "AggregateQuery": 4,
        "DownsampleQuery": 5,
        "ValueFilterQuery": 6,
        "NotSupportQuery": 7,
        "GetReplicaNum": 8,
        "AddStorageEngines": 9,
        "CountPoints": 10,
        "ClearData": 11,
        "ShowTimeSeries": 12,
        "ShowClusterInfo": 13,
    }


class AuthType(object):
    Read = 0
    Write = 1
    Admin = 2
    Cluster = 3

    _VALUES_TO_NAMES = {
        0: "Read",
        1: "Write",
        2: "Admin",
        3: "Cluster",
    }

    _NAMES_TO_VALUES = {
        "Read": 0,
        "Write": 1,
        "Admin": 2,
        "Cluster": 3,
    }


class UserType(object):
    Administrator = 0
    OrdinaryUser = 1

    _VALUES_TO_NAMES = {
        0: "Administrator",
        1: "OrdinaryUser",
    }

    _NAMES_TO_VALUES = {
        "Administrator": 0,
        "OrdinaryUser": 1,
    }


class Status(object):
    """
    Attributes:
     - code
     - message
     - subStatus

    """


    def __init__(self, code=None, message=None, subStatus=None,):
        self.code = code
        self.message = message
        self.subStatus = subStatus

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I32:
                    self.code = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.message = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.subStatus = []
                    (_etype3, _size0) = iprot.readListBegin()
                    for _i4 in range(_size0):
                        _elem5 = Status()
                        _elem5.read(iprot)
                        self.subStatus.append(_elem5)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('Status')
        if self.code is not None:
            oprot.writeFieldBegin('code', TType.I32, 1)
            oprot.writeI32(self.code)
            oprot.writeFieldEnd()
        if self.message is not None:
            oprot.writeFieldBegin('message', TType.STRING, 2)
            oprot.writeString(self.message.encode('utf-8') if sys.version_info[0] == 2 else self.message)
            oprot.writeFieldEnd()
        if self.subStatus is not None:
            oprot.writeFieldBegin('subStatus', TType.LIST, 3)
            oprot.writeListBegin(TType.STRUCT, len(self.subStatus))
            for iter6 in self.subStatus:
                iter6.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.code is None:
            raise TProtocolException(message='Required field code is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class OpenSessionReq(object):
    """
    Attributes:
     - username
     - password

    """


    def __init__(self, username=None, password=None,):
        self.username = username
        self.password = password

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.username = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.password = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('OpenSessionReq')
        if self.username is not None:
            oprot.writeFieldBegin('username', TType.STRING, 1)
            oprot.writeString(self.username.encode('utf-8') if sys.version_info[0] == 2 else self.username)
            oprot.writeFieldEnd()
        if self.password is not None:
            oprot.writeFieldBegin('password', TType.STRING, 2)
            oprot.writeString(self.password.encode('utf-8') if sys.version_info[0] == 2 else self.password)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class OpenSessionResp(object):
    """
    Attributes:
     - status
     - sessionId

    """


    def __init__(self, status=None, sessionId=None,):
        self.status = status
        self.sessionId = sessionId

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('OpenSessionResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 2)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class CloseSessionReq(object):
    """
    Attributes:
     - sessionId

    """


    def __init__(self, sessionId=None,):
        self.sessionId = sessionId

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('CloseSessionReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class DeleteColumnsReq(object):
    """
    Attributes:
     - sessionId
     - paths

    """


    def __init__(self, sessionId=None, paths=None,):
        self.sessionId = sessionId
        self.paths = paths

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype10, _size7) = iprot.readListBegin()
                    for _i11 in range(_size7):
                        _elem12 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem12)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('DeleteColumnsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter13 in self.paths:
                oprot.writeString(iter13.encode('utf-8') if sys.version_info[0] == 2 else iter13)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class InsertColumnRecordsReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - timestamps
     - valuesList
     - bitmapList
     - dataTypeList
     - attributesList

    """


    def __init__(self, sessionId=None, paths=None, timestamps=None, valuesList=None, bitmapList=None, dataTypeList=None, attributesList=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.timestamps = timestamps
        self.valuesList = valuesList
        self.bitmapList = bitmapList
        self.dataTypeList = dataTypeList
        self.attributesList = attributesList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype17, _size14) = iprot.readListBegin()
                    for _i18 in range(_size14):
                        _elem19 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem19)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.valuesList = []
                    (_etype23, _size20) = iprot.readListBegin()
                    for _i24 in range(_size20):
                        _elem25 = iprot.readBinary()
                        self.valuesList.append(_elem25)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.LIST:
                    self.bitmapList = []
                    (_etype29, _size26) = iprot.readListBegin()
                    for _i30 in range(_size26):
                        _elem31 = iprot.readBinary()
                        self.bitmapList.append(_elem31)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype35, _size32) = iprot.readListBegin()
                    for _i36 in range(_size32):
                        _elem37 = iprot.readI32()
                        self.dataTypeList.append(_elem37)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 7:
                if ftype == TType.LIST:
                    self.attributesList = []
                    (_etype41, _size38) = iprot.readListBegin()
                    for _i42 in range(_size38):
                        _elem43 = {}
                        (_ktype45, _vtype46, _size44) = iprot.readMapBegin()
                        for _i48 in range(_size44):
                            _key49 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _val50 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _elem43[_key49] = _val50
                        iprot.readMapEnd()
                        self.attributesList.append(_elem43)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('InsertColumnRecordsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter51 in self.paths:
                oprot.writeString(iter51.encode('utf-8') if sys.version_info[0] == 2 else iter51)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 3)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.LIST, 4)
            oprot.writeListBegin(TType.STRING, len(self.valuesList))
            for iter52 in self.valuesList:
                oprot.writeBinary(iter52)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.bitmapList is not None:
            oprot.writeFieldBegin('bitmapList', TType.LIST, 5)
            oprot.writeListBegin(TType.STRING, len(self.bitmapList))
            for iter53 in self.bitmapList:
                oprot.writeBinary(iter53)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 6)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter54 in self.dataTypeList:
                oprot.writeI32(iter54)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.attributesList is not None:
            oprot.writeFieldBegin('attributesList', TType.LIST, 7)
            oprot.writeListBegin(TType.MAP, len(self.attributesList))
            for iter55 in self.attributesList:
                oprot.writeMapBegin(TType.STRING, TType.STRING, len(iter55))
                for kiter56, viter57 in iter55.items():
                    oprot.writeString(kiter56.encode('utf-8') if sys.version_info[0] == 2 else kiter56)
                    oprot.writeString(viter57.encode('utf-8') if sys.version_info[0] == 2 else viter57)
                oprot.writeMapEnd()
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.timestamps is None:
            raise TProtocolException(message='Required field timestamps is unset!')
        if self.valuesList is None:
            raise TProtocolException(message='Required field valuesList is unset!')
        if self.bitmapList is None:
            raise TProtocolException(message='Required field bitmapList is unset!')
        if self.dataTypeList is None:
            raise TProtocolException(message='Required field dataTypeList is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class InsertNonAlignedColumnRecordsReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - timestamps
     - valuesList
     - bitmapList
     - dataTypeList
     - attributesList

    """


    def __init__(self, sessionId=None, paths=None, timestamps=None, valuesList=None, bitmapList=None, dataTypeList=None, attributesList=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.timestamps = timestamps
        self.valuesList = valuesList
        self.bitmapList = bitmapList
        self.dataTypeList = dataTypeList
        self.attributesList = attributesList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype61, _size58) = iprot.readListBegin()
                    for _i62 in range(_size58):
                        _elem63 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem63)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.valuesList = []
                    (_etype67, _size64) = iprot.readListBegin()
                    for _i68 in range(_size64):
                        _elem69 = iprot.readBinary()
                        self.valuesList.append(_elem69)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.LIST:
                    self.bitmapList = []
                    (_etype73, _size70) = iprot.readListBegin()
                    for _i74 in range(_size70):
                        _elem75 = iprot.readBinary()
                        self.bitmapList.append(_elem75)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype79, _size76) = iprot.readListBegin()
                    for _i80 in range(_size76):
                        _elem81 = iprot.readI32()
                        self.dataTypeList.append(_elem81)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 7:
                if ftype == TType.LIST:
                    self.attributesList = []
                    (_etype85, _size82) = iprot.readListBegin()
                    for _i86 in range(_size82):
                        _elem87 = {}
                        (_ktype89, _vtype90, _size88) = iprot.readMapBegin()
                        for _i92 in range(_size88):
                            _key93 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _val94 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _elem87[_key93] = _val94
                        iprot.readMapEnd()
                        self.attributesList.append(_elem87)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('InsertNonAlignedColumnRecordsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter95 in self.paths:
                oprot.writeString(iter95.encode('utf-8') if sys.version_info[0] == 2 else iter95)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 3)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.LIST, 4)
            oprot.writeListBegin(TType.STRING, len(self.valuesList))
            for iter96 in self.valuesList:
                oprot.writeBinary(iter96)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.bitmapList is not None:
            oprot.writeFieldBegin('bitmapList', TType.LIST, 5)
            oprot.writeListBegin(TType.STRING, len(self.bitmapList))
            for iter97 in self.bitmapList:
                oprot.writeBinary(iter97)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 6)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter98 in self.dataTypeList:
                oprot.writeI32(iter98)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.attributesList is not None:
            oprot.writeFieldBegin('attributesList', TType.LIST, 7)
            oprot.writeListBegin(TType.MAP, len(self.attributesList))
            for iter99 in self.attributesList:
                oprot.writeMapBegin(TType.STRING, TType.STRING, len(iter99))
                for kiter100, viter101 in iter99.items():
                    oprot.writeString(kiter100.encode('utf-8') if sys.version_info[0] == 2 else kiter100)
                    oprot.writeString(viter101.encode('utf-8') if sys.version_info[0] == 2 else viter101)
                oprot.writeMapEnd()
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.timestamps is None:
            raise TProtocolException(message='Required field timestamps is unset!')
        if self.valuesList is None:
            raise TProtocolException(message='Required field valuesList is unset!')
        if self.bitmapList is None:
            raise TProtocolException(message='Required field bitmapList is unset!')
        if self.dataTypeList is None:
            raise TProtocolException(message='Required field dataTypeList is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class InsertRowRecordsReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - timestamps
     - valuesList
     - bitmapList
     - dataTypeList
     - attributesList

    """


    def __init__(self, sessionId=None, paths=None, timestamps=None, valuesList=None, bitmapList=None, dataTypeList=None, attributesList=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.timestamps = timestamps
        self.valuesList = valuesList
        self.bitmapList = bitmapList
        self.dataTypeList = dataTypeList
        self.attributesList = attributesList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype105, _size102) = iprot.readListBegin()
                    for _i106 in range(_size102):
                        _elem107 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem107)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.valuesList = []
                    (_etype111, _size108) = iprot.readListBegin()
                    for _i112 in range(_size108):
                        _elem113 = iprot.readBinary()
                        self.valuesList.append(_elem113)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.LIST:
                    self.bitmapList = []
                    (_etype117, _size114) = iprot.readListBegin()
                    for _i118 in range(_size114):
                        _elem119 = iprot.readBinary()
                        self.bitmapList.append(_elem119)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype123, _size120) = iprot.readListBegin()
                    for _i124 in range(_size120):
                        _elem125 = iprot.readI32()
                        self.dataTypeList.append(_elem125)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 7:
                if ftype == TType.LIST:
                    self.attributesList = []
                    (_etype129, _size126) = iprot.readListBegin()
                    for _i130 in range(_size126):
                        _elem131 = {}
                        (_ktype133, _vtype134, _size132) = iprot.readMapBegin()
                        for _i136 in range(_size132):
                            _key137 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _val138 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _elem131[_key137] = _val138
                        iprot.readMapEnd()
                        self.attributesList.append(_elem131)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('InsertRowRecordsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter139 in self.paths:
                oprot.writeString(iter139.encode('utf-8') if sys.version_info[0] == 2 else iter139)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 3)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.LIST, 4)
            oprot.writeListBegin(TType.STRING, len(self.valuesList))
            for iter140 in self.valuesList:
                oprot.writeBinary(iter140)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.bitmapList is not None:
            oprot.writeFieldBegin('bitmapList', TType.LIST, 5)
            oprot.writeListBegin(TType.STRING, len(self.bitmapList))
            for iter141 in self.bitmapList:
                oprot.writeBinary(iter141)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 6)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter142 in self.dataTypeList:
                oprot.writeI32(iter142)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.attributesList is not None:
            oprot.writeFieldBegin('attributesList', TType.LIST, 7)
            oprot.writeListBegin(TType.MAP, len(self.attributesList))
            for iter143 in self.attributesList:
                oprot.writeMapBegin(TType.STRING, TType.STRING, len(iter143))
                for kiter144, viter145 in iter143.items():
                    oprot.writeString(kiter144.encode('utf-8') if sys.version_info[0] == 2 else kiter144)
                    oprot.writeString(viter145.encode('utf-8') if sys.version_info[0] == 2 else viter145)
                oprot.writeMapEnd()
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.timestamps is None:
            raise TProtocolException(message='Required field timestamps is unset!')
        if self.valuesList is None:
            raise TProtocolException(message='Required field valuesList is unset!')
        if self.bitmapList is None:
            raise TProtocolException(message='Required field bitmapList is unset!')
        if self.dataTypeList is None:
            raise TProtocolException(message='Required field dataTypeList is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class InsertNonAlignedRowRecordsReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - timestamps
     - valuesList
     - bitmapList
     - dataTypeList
     - attributesList

    """


    def __init__(self, sessionId=None, paths=None, timestamps=None, valuesList=None, bitmapList=None, dataTypeList=None, attributesList=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.timestamps = timestamps
        self.valuesList = valuesList
        self.bitmapList = bitmapList
        self.dataTypeList = dataTypeList
        self.attributesList = attributesList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype149, _size146) = iprot.readListBegin()
                    for _i150 in range(_size146):
                        _elem151 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem151)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.valuesList = []
                    (_etype155, _size152) = iprot.readListBegin()
                    for _i156 in range(_size152):
                        _elem157 = iprot.readBinary()
                        self.valuesList.append(_elem157)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.LIST:
                    self.bitmapList = []
                    (_etype161, _size158) = iprot.readListBegin()
                    for _i162 in range(_size158):
                        _elem163 = iprot.readBinary()
                        self.bitmapList.append(_elem163)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype167, _size164) = iprot.readListBegin()
                    for _i168 in range(_size164):
                        _elem169 = iprot.readI32()
                        self.dataTypeList.append(_elem169)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 7:
                if ftype == TType.LIST:
                    self.attributesList = []
                    (_etype173, _size170) = iprot.readListBegin()
                    for _i174 in range(_size170):
                        _elem175 = {}
                        (_ktype177, _vtype178, _size176) = iprot.readMapBegin()
                        for _i180 in range(_size176):
                            _key181 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _val182 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                            _elem175[_key181] = _val182
                        iprot.readMapEnd()
                        self.attributesList.append(_elem175)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('InsertNonAlignedRowRecordsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter183 in self.paths:
                oprot.writeString(iter183.encode('utf-8') if sys.version_info[0] == 2 else iter183)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 3)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.LIST, 4)
            oprot.writeListBegin(TType.STRING, len(self.valuesList))
            for iter184 in self.valuesList:
                oprot.writeBinary(iter184)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.bitmapList is not None:
            oprot.writeFieldBegin('bitmapList', TType.LIST, 5)
            oprot.writeListBegin(TType.STRING, len(self.bitmapList))
            for iter185 in self.bitmapList:
                oprot.writeBinary(iter185)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 6)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter186 in self.dataTypeList:
                oprot.writeI32(iter186)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.attributesList is not None:
            oprot.writeFieldBegin('attributesList', TType.LIST, 7)
            oprot.writeListBegin(TType.MAP, len(self.attributesList))
            for iter187 in self.attributesList:
                oprot.writeMapBegin(TType.STRING, TType.STRING, len(iter187))
                for kiter188, viter189 in iter187.items():
                    oprot.writeString(kiter188.encode('utf-8') if sys.version_info[0] == 2 else kiter188)
                    oprot.writeString(viter189.encode('utf-8') if sys.version_info[0] == 2 else viter189)
                oprot.writeMapEnd()
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.timestamps is None:
            raise TProtocolException(message='Required field timestamps is unset!')
        if self.valuesList is None:
            raise TProtocolException(message='Required field valuesList is unset!')
        if self.bitmapList is None:
            raise TProtocolException(message='Required field bitmapList is unset!')
        if self.dataTypeList is None:
            raise TProtocolException(message='Required field dataTypeList is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class DeleteDataInColumnsReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - startTime
     - endTime

    """


    def __init__(self, sessionId=None, paths=None, startTime=None, endTime=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.startTime = startTime
        self.endTime = endTime

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype193, _size190) = iprot.readListBegin()
                    for _i194 in range(_size190):
                        _elem195 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem195)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.startTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.I64:
                    self.endTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('DeleteDataInColumnsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter196 in self.paths:
                oprot.writeString(iter196.encode('utf-8') if sys.version_info[0] == 2 else iter196)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.startTime is not None:
            oprot.writeFieldBegin('startTime', TType.I64, 3)
            oprot.writeI64(self.startTime)
            oprot.writeFieldEnd()
        if self.endTime is not None:
            oprot.writeFieldBegin('endTime', TType.I64, 4)
            oprot.writeI64(self.endTime)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.startTime is None:
            raise TProtocolException(message='Required field startTime is unset!')
        if self.endTime is None:
            raise TProtocolException(message='Required field endTime is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class QueryDataSet(object):
    """
    Attributes:
     - timestamps
     - valuesList
     - bitmapList

    """


    def __init__(self, timestamps=None, valuesList=None, bitmapList=None,):
        self.timestamps = timestamps
        self.valuesList = valuesList
        self.bitmapList = bitmapList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.valuesList = []
                    (_etype200, _size197) = iprot.readListBegin()
                    for _i201 in range(_size197):
                        _elem202 = iprot.readBinary()
                        self.valuesList.append(_elem202)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.bitmapList = []
                    (_etype206, _size203) = iprot.readListBegin()
                    for _i207 in range(_size203):
                        _elem208 = iprot.readBinary()
                        self.bitmapList.append(_elem208)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('QueryDataSet')
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 1)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.valuesList))
            for iter209 in self.valuesList:
                oprot.writeBinary(iter209)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.bitmapList is not None:
            oprot.writeFieldBegin('bitmapList', TType.LIST, 3)
            oprot.writeListBegin(TType.STRING, len(self.bitmapList))
            for iter210 in self.bitmapList:
                oprot.writeBinary(iter210)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.timestamps is None:
            raise TProtocolException(message='Required field timestamps is unset!')
        if self.valuesList is None:
            raise TProtocolException(message='Required field valuesList is unset!')
        if self.bitmapList is None:
            raise TProtocolException(message='Required field bitmapList is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class QueryDataReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - startTime
     - endTime

    """


    def __init__(self, sessionId=None, paths=None, startTime=None, endTime=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.startTime = startTime
        self.endTime = endTime

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype214, _size211) = iprot.readListBegin()
                    for _i215 in range(_size211):
                        _elem216 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem216)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.startTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.I64:
                    self.endTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('QueryDataReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter217 in self.paths:
                oprot.writeString(iter217.encode('utf-8') if sys.version_info[0] == 2 else iter217)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.startTime is not None:
            oprot.writeFieldBegin('startTime', TType.I64, 3)
            oprot.writeI64(self.startTime)
            oprot.writeFieldEnd()
        if self.endTime is not None:
            oprot.writeFieldBegin('endTime', TType.I64, 4)
            oprot.writeI64(self.endTime)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.startTime is None:
            raise TProtocolException(message='Required field startTime is unset!')
        if self.endTime is None:
            raise TProtocolException(message='Required field endTime is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class QueryDataResp(object):
    """
    Attributes:
     - status
     - paths
     - dataTypeList
     - queryDataSet

    """


    def __init__(self, status=None, paths=None, dataTypeList=None, queryDataSet=None,):
        self.status = status
        self.paths = paths
        self.dataTypeList = dataTypeList
        self.queryDataSet = queryDataSet

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype221, _size218) = iprot.readListBegin()
                    for _i222 in range(_size218):
                        _elem223 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem223)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype227, _size224) = iprot.readListBegin()
                    for _i228 in range(_size224):
                        _elem229 = iprot.readI32()
                        self.dataTypeList.append(_elem229)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRUCT:
                    self.queryDataSet = QueryDataSet()
                    self.queryDataSet.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('QueryDataResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter230 in self.paths:
                oprot.writeString(iter230.encode('utf-8') if sys.version_info[0] == 2 else iter230)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter231 in self.dataTypeList:
                oprot.writeI32(iter231)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.queryDataSet is not None:
            oprot.writeFieldBegin('queryDataSet', TType.STRUCT, 4)
            self.queryDataSet.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class AddStorageEnginesReq(object):
    """
    Attributes:
     - sessionId
     - storageEngines

    """


    def __init__(self, sessionId=None, storageEngines=None,):
        self.sessionId = sessionId
        self.storageEngines = storageEngines

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.storageEngines = []
                    (_etype235, _size232) = iprot.readListBegin()
                    for _i236 in range(_size232):
                        _elem237 = StorageEngine()
                        _elem237.read(iprot)
                        self.storageEngines.append(_elem237)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('AddStorageEnginesReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.storageEngines is not None:
            oprot.writeFieldBegin('storageEngines', TType.LIST, 2)
            oprot.writeListBegin(TType.STRUCT, len(self.storageEngines))
            for iter238 in self.storageEngines:
                iter238.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.storageEngines is None:
            raise TProtocolException(message='Required field storageEngines is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class StorageEngine(object):
    """
    Attributes:
     - ip
     - port
     - type
     - extraParams

    """


    def __init__(self, ip=None, port=None, type=None, extraParams=None,):
        self.ip = ip
        self.port = port
        self.type = type
        self.extraParams = extraParams

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.ip = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.port = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.type = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.MAP:
                    self.extraParams = {}
                    (_ktype240, _vtype241, _size239) = iprot.readMapBegin()
                    for _i243 in range(_size239):
                        _key244 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        _val245 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.extraParams[_key244] = _val245
                    iprot.readMapEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('StorageEngine')
        if self.ip is not None:
            oprot.writeFieldBegin('ip', TType.STRING, 1)
            oprot.writeString(self.ip.encode('utf-8') if sys.version_info[0] == 2 else self.ip)
            oprot.writeFieldEnd()
        if self.port is not None:
            oprot.writeFieldBegin('port', TType.I32, 2)
            oprot.writeI32(self.port)
            oprot.writeFieldEnd()
        if self.type is not None:
            oprot.writeFieldBegin('type', TType.STRING, 3)
            oprot.writeString(self.type.encode('utf-8') if sys.version_info[0] == 2 else self.type)
            oprot.writeFieldEnd()
        if self.extraParams is not None:
            oprot.writeFieldBegin('extraParams', TType.MAP, 4)
            oprot.writeMapBegin(TType.STRING, TType.STRING, len(self.extraParams))
            for kiter246, viter247 in self.extraParams.items():
                oprot.writeString(kiter246.encode('utf-8') if sys.version_info[0] == 2 else kiter246)
                oprot.writeString(viter247.encode('utf-8') if sys.version_info[0] == 2 else viter247)
            oprot.writeMapEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.ip is None:
            raise TProtocolException(message='Required field ip is unset!')
        if self.port is None:
            raise TProtocolException(message='Required field port is unset!')
        if self.type is None:
            raise TProtocolException(message='Required field type is unset!')
        if self.extraParams is None:
            raise TProtocolException(message='Required field extraParams is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class AggregateQueryReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - startTime
     - endTime
     - aggregateType

    """


    def __init__(self, sessionId=None, paths=None, startTime=None, endTime=None, aggregateType=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.startTime = startTime
        self.endTime = endTime
        self.aggregateType = aggregateType

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype251, _size248) = iprot.readListBegin()
                    for _i252 in range(_size248):
                        _elem253 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem253)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.startTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.I64:
                    self.endTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.I32:
                    self.aggregateType = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('AggregateQueryReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter254 in self.paths:
                oprot.writeString(iter254.encode('utf-8') if sys.version_info[0] == 2 else iter254)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.startTime is not None:
            oprot.writeFieldBegin('startTime', TType.I64, 3)
            oprot.writeI64(self.startTime)
            oprot.writeFieldEnd()
        if self.endTime is not None:
            oprot.writeFieldBegin('endTime', TType.I64, 4)
            oprot.writeI64(self.endTime)
            oprot.writeFieldEnd()
        if self.aggregateType is not None:
            oprot.writeFieldBegin('aggregateType', TType.I32, 5)
            oprot.writeI32(self.aggregateType)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.startTime is None:
            raise TProtocolException(message='Required field startTime is unset!')
        if self.endTime is None:
            raise TProtocolException(message='Required field endTime is unset!')
        if self.aggregateType is None:
            raise TProtocolException(message='Required field aggregateType is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class AggregateQueryResp(object):
    """
    Attributes:
     - status
     - paths
     - dataTypeList
     - timestamps
     - valuesList

    """


    def __init__(self, status=None, paths=None, dataTypeList=None, timestamps=None, valuesList=None,):
        self.status = status
        self.paths = paths
        self.dataTypeList = dataTypeList
        self.timestamps = timestamps
        self.valuesList = valuesList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype258, _size255) = iprot.readListBegin()
                    for _i259 in range(_size255):
                        _elem260 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem260)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype264, _size261) = iprot.readListBegin()
                    for _i265 in range(_size261):
                        _elem266 = iprot.readI32()
                        self.dataTypeList.append(_elem266)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.STRING:
                    self.valuesList = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('AggregateQueryResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter267 in self.paths:
                oprot.writeString(iter267.encode('utf-8') if sys.version_info[0] == 2 else iter267)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter268 in self.dataTypeList:
                oprot.writeI32(iter268)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 4)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.STRING, 5)
            oprot.writeBinary(self.valuesList)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ValueFilterQueryReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - startTime
     - endTime
     - booleanExpression

    """


    def __init__(self, sessionId=None, paths=None, startTime=None, endTime=None, booleanExpression=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.startTime = startTime
        self.endTime = endTime
        self.booleanExpression = booleanExpression

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype272, _size269) = iprot.readListBegin()
                    for _i273 in range(_size269):
                        _elem274 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem274)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.startTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.I64:
                    self.endTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.STRING:
                    self.booleanExpression = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('ValueFilterQueryReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter275 in self.paths:
                oprot.writeString(iter275.encode('utf-8') if sys.version_info[0] == 2 else iter275)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.startTime is not None:
            oprot.writeFieldBegin('startTime', TType.I64, 3)
            oprot.writeI64(self.startTime)
            oprot.writeFieldEnd()
        if self.endTime is not None:
            oprot.writeFieldBegin('endTime', TType.I64, 4)
            oprot.writeI64(self.endTime)
            oprot.writeFieldEnd()
        if self.booleanExpression is not None:
            oprot.writeFieldBegin('booleanExpression', TType.STRING, 5)
            oprot.writeString(self.booleanExpression.encode('utf-8') if sys.version_info[0] == 2 else self.booleanExpression)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.startTime is None:
            raise TProtocolException(message='Required field startTime is unset!')
        if self.endTime is None:
            raise TProtocolException(message='Required field endTime is unset!')
        if self.booleanExpression is None:
            raise TProtocolException(message='Required field booleanExpression is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ValueFilterQueryResp(object):
    """
    Attributes:
     - status
     - paths
     - dataTypeList
     - queryDataSet

    """


    def __init__(self, status=None, paths=None, dataTypeList=None, queryDataSet=None,):
        self.status = status
        self.paths = paths
        self.dataTypeList = dataTypeList
        self.queryDataSet = queryDataSet

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype279, _size276) = iprot.readListBegin()
                    for _i280 in range(_size276):
                        _elem281 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem281)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype285, _size282) = iprot.readListBegin()
                    for _i286 in range(_size282):
                        _elem287 = iprot.readI32()
                        self.dataTypeList.append(_elem287)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRUCT:
                    self.queryDataSet = QueryDataSet()
                    self.queryDataSet.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('ValueFilterQueryResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter288 in self.paths:
                oprot.writeString(iter288.encode('utf-8') if sys.version_info[0] == 2 else iter288)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter289 in self.dataTypeList:
                oprot.writeI32(iter289)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.queryDataSet is not None:
            oprot.writeFieldBegin('queryDataSet', TType.STRUCT, 4)
            self.queryDataSet.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class LastQueryReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - startTime

    """


    def __init__(self, sessionId=None, paths=None, startTime=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.startTime = startTime

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype293, _size290) = iprot.readListBegin()
                    for _i294 in range(_size290):
                        _elem295 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem295)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.startTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('LastQueryReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter296 in self.paths:
                oprot.writeString(iter296.encode('utf-8') if sys.version_info[0] == 2 else iter296)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.startTime is not None:
            oprot.writeFieldBegin('startTime', TType.I64, 3)
            oprot.writeI64(self.startTime)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.startTime is None:
            raise TProtocolException(message='Required field startTime is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class LastQueryResp(object):
    """
    Attributes:
     - status
     - paths
     - dataTypeList
     - timestamps
     - valuesList

    """


    def __init__(self, status=None, paths=None, dataTypeList=None, timestamps=None, valuesList=None,):
        self.status = status
        self.paths = paths
        self.dataTypeList = dataTypeList
        self.timestamps = timestamps
        self.valuesList = valuesList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype300, _size297) = iprot.readListBegin()
                    for _i301 in range(_size297):
                        _elem302 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem302)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype306, _size303) = iprot.readListBegin()
                    for _i307 in range(_size303):
                        _elem308 = iprot.readI32()
                        self.dataTypeList.append(_elem308)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.STRING:
                    self.valuesList = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('LastQueryResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter309 in self.paths:
                oprot.writeString(iter309.encode('utf-8') if sys.version_info[0] == 2 else iter309)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter310 in self.dataTypeList:
                oprot.writeI32(iter310)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 4)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.STRING, 5)
            oprot.writeBinary(self.valuesList)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class DownsampleQueryReq(object):
    """
    Attributes:
     - sessionId
     - paths
     - startTime
     - endTime
     - aggregateType
     - precision

    """


    def __init__(self, sessionId=None, paths=None, startTime=None, endTime=None, aggregateType=None, precision=None,):
        self.sessionId = sessionId
        self.paths = paths
        self.startTime = startTime
        self.endTime = endTime
        self.aggregateType = aggregateType
        self.precision = precision

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype314, _size311) = iprot.readListBegin()
                    for _i315 in range(_size311):
                        _elem316 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem316)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I64:
                    self.startTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.I64:
                    self.endTime = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.I32:
                    self.aggregateType = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.I64:
                    self.precision = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('DownsampleQueryReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter317 in self.paths:
                oprot.writeString(iter317.encode('utf-8') if sys.version_info[0] == 2 else iter317)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.startTime is not None:
            oprot.writeFieldBegin('startTime', TType.I64, 3)
            oprot.writeI64(self.startTime)
            oprot.writeFieldEnd()
        if self.endTime is not None:
            oprot.writeFieldBegin('endTime', TType.I64, 4)
            oprot.writeI64(self.endTime)
            oprot.writeFieldEnd()
        if self.aggregateType is not None:
            oprot.writeFieldBegin('aggregateType', TType.I32, 5)
            oprot.writeI32(self.aggregateType)
            oprot.writeFieldEnd()
        if self.precision is not None:
            oprot.writeFieldBegin('precision', TType.I64, 6)
            oprot.writeI64(self.precision)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.paths is None:
            raise TProtocolException(message='Required field paths is unset!')
        if self.startTime is None:
            raise TProtocolException(message='Required field startTime is unset!')
        if self.endTime is None:
            raise TProtocolException(message='Required field endTime is unset!')
        if self.aggregateType is None:
            raise TProtocolException(message='Required field aggregateType is unset!')
        if self.precision is None:
            raise TProtocolException(message='Required field precision is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class DownsampleQueryResp(object):
    """
    Attributes:
     - status
     - paths
     - dataTypeList
     - queryDataSet

    """


    def __init__(self, status=None, paths=None, dataTypeList=None, queryDataSet=None,):
        self.status = status
        self.paths = paths
        self.dataTypeList = dataTypeList
        self.queryDataSet = queryDataSet

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype321, _size318) = iprot.readListBegin()
                    for _i322 in range(_size318):
                        _elem323 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem323)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype327, _size324) = iprot.readListBegin()
                    for _i328 in range(_size324):
                        _elem329 = iprot.readI32()
                        self.dataTypeList.append(_elem329)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRUCT:
                    self.queryDataSet = QueryDataSet()
                    self.queryDataSet.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('DownsampleQueryResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter330 in self.paths:
                oprot.writeString(iter330.encode('utf-8') if sys.version_info[0] == 2 else iter330)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter331 in self.dataTypeList:
                oprot.writeI32(iter331)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.queryDataSet is not None:
            oprot.writeFieldBegin('queryDataSet', TType.STRUCT, 4)
            self.queryDataSet.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ShowColumnsReq(object):
    """
    Attributes:
     - sessionId

    """


    def __init__(self, sessionId=None,):
        self.sessionId = sessionId

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('ShowColumnsReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ShowColumnsResp(object):
    """
    Attributes:
     - status
     - paths
     - dataTypeList

    """


    def __init__(self, status=None, paths=None, dataTypeList=None,):
        self.status = status
        self.paths = paths
        self.dataTypeList = dataTypeList

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype335, _size332) = iprot.readListBegin()
                    for _i336 in range(_size332):
                        _elem337 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem337)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype341, _size338) = iprot.readListBegin()
                    for _i342 in range(_size338):
                        _elem343 = iprot.readI32()
                        self.dataTypeList.append(_elem343)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('ShowColumnsResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter344 in self.paths:
                oprot.writeString(iter344.encode('utf-8') if sys.version_info[0] == 2 else iter344)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter345 in self.dataTypeList:
                oprot.writeI32(iter345)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class GetReplicaNumReq(object):
    """
    Attributes:
     - sessionId

    """


    def __init__(self, sessionId=None,):
        self.sessionId = sessionId

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('GetReplicaNumReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class GetReplicaNumResp(object):
    """
    Attributes:
     - status
     - replicaNum

    """


    def __init__(self, status=None, replicaNum=None,):
        self.status = status
        self.replicaNum = replicaNum

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.replicaNum = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('GetReplicaNumResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.replicaNum is not None:
            oprot.writeFieldBegin('replicaNum', TType.I32, 2)
            oprot.writeI32(self.replicaNum)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ExecuteSqlReq(object):
    """
    Attributes:
     - sessionId
     - statement

    """


    def __init__(self, sessionId=None, statement=None,):
        self.sessionId = sessionId
        self.statement = statement

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.statement = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('ExecuteSqlReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.statement is not None:
            oprot.writeFieldBegin('statement', TType.STRING, 2)
            oprot.writeString(self.statement.encode('utf-8') if sys.version_info[0] == 2 else self.statement)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.statement is None:
            raise TProtocolException(message='Required field statement is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class ExecuteSqlResp(object):
    """
    Attributes:
     - status
     - type
     - paths
     - dataTypeList
     - queryDataSet
     - timestamps
     - valuesList
     - replicaNum
     - pointsNum
     - aggregateType
     - parseErrorMsg
     - limit
     - offset
     - orderByPath
     - ascending
     - iginxInfos
     - storageEngineInfos
     - metaStorageInfos
     - localMetaStorageInfo

    """


    def __init__(self, status=None, type=None, paths=None, dataTypeList=None, queryDataSet=None, timestamps=None, valuesList=None, replicaNum=None, pointsNum=None, aggregateType=None, parseErrorMsg=None, limit=None, offset=None, orderByPath=None, ascending=None, iginxInfos=None, storageEngineInfos=None, metaStorageInfos=None, localMetaStorageInfo=None,):
        self.status = status
        self.type = type
        self.paths = paths
        self.dataTypeList = dataTypeList
        self.queryDataSet = queryDataSet
        self.timestamps = timestamps
        self.valuesList = valuesList
        self.replicaNum = replicaNum
        self.pointsNum = pointsNum
        self.aggregateType = aggregateType
        self.parseErrorMsg = parseErrorMsg
        self.limit = limit
        self.offset = offset
        self.orderByPath = orderByPath
        self.ascending = ascending
        self.iginxInfos = iginxInfos
        self.storageEngineInfos = storageEngineInfos
        self.metaStorageInfos = metaStorageInfos
        self.localMetaStorageInfo = localMetaStorageInfo

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.type = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.paths = []
                    (_etype349, _size346) = iprot.readListBegin()
                    for _i350 in range(_size346):
                        _elem351 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.paths.append(_elem351)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.dataTypeList = []
                    (_etype355, _size352) = iprot.readListBegin()
                    for _i356 in range(_size352):
                        _elem357 = iprot.readI32()
                        self.dataTypeList.append(_elem357)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.STRUCT:
                    self.queryDataSet = QueryDataSet()
                    self.queryDataSet.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 6:
                if ftype == TType.STRING:
                    self.timestamps = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 7:
                if ftype == TType.STRING:
                    self.valuesList = iprot.readBinary()
                else:
                    iprot.skip(ftype)
            elif fid == 8:
                if ftype == TType.I32:
                    self.replicaNum = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 9:
                if ftype == TType.I64:
                    self.pointsNum = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 10:
                if ftype == TType.I32:
                    self.aggregateType = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 11:
                if ftype == TType.STRING:
                    self.parseErrorMsg = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 12:
                if ftype == TType.I32:
                    self.limit = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 13:
                if ftype == TType.I32:
                    self.offset = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 14:
                if ftype == TType.STRING:
                    self.orderByPath = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 15:
                if ftype == TType.BOOL:
                    self.ascending = iprot.readBool()
                else:
                    iprot.skip(ftype)
            elif fid == 16:
                if ftype == TType.LIST:
                    self.iginxInfos = []
                    (_etype361, _size358) = iprot.readListBegin()
                    for _i362 in range(_size358):
                        _elem363 = IginxInfo()
                        _elem363.read(iprot)
                        self.iginxInfos.append(_elem363)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 17:
                if ftype == TType.LIST:
                    self.storageEngineInfos = []
                    (_etype367, _size364) = iprot.readListBegin()
                    for _i368 in range(_size364):
                        _elem369 = StorageEngineInfo()
                        _elem369.read(iprot)
                        self.storageEngineInfos.append(_elem369)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 18:
                if ftype == TType.LIST:
                    self.metaStorageInfos = []
                    (_etype373, _size370) = iprot.readListBegin()
                    for _i374 in range(_size370):
                        _elem375 = MetaStorageInfo()
                        _elem375.read(iprot)
                        self.metaStorageInfos.append(_elem375)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 19:
                if ftype == TType.STRUCT:
                    self.localMetaStorageInfo = LocalMetaStorageInfo()
                    self.localMetaStorageInfo.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('ExecuteSqlResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.type is not None:
            oprot.writeFieldBegin('type', TType.I32, 2)
            oprot.writeI32(self.type)
            oprot.writeFieldEnd()
        if self.paths is not None:
            oprot.writeFieldBegin('paths', TType.LIST, 3)
            oprot.writeListBegin(TType.STRING, len(self.paths))
            for iter376 in self.paths:
                oprot.writeString(iter376.encode('utf-8') if sys.version_info[0] == 2 else iter376)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.dataTypeList is not None:
            oprot.writeFieldBegin('dataTypeList', TType.LIST, 4)
            oprot.writeListBegin(TType.I32, len(self.dataTypeList))
            for iter377 in self.dataTypeList:
                oprot.writeI32(iter377)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.queryDataSet is not None:
            oprot.writeFieldBegin('queryDataSet', TType.STRUCT, 5)
            self.queryDataSet.write(oprot)
            oprot.writeFieldEnd()
        if self.timestamps is not None:
            oprot.writeFieldBegin('timestamps', TType.STRING, 6)
            oprot.writeBinary(self.timestamps)
            oprot.writeFieldEnd()
        if self.valuesList is not None:
            oprot.writeFieldBegin('valuesList', TType.STRING, 7)
            oprot.writeBinary(self.valuesList)
            oprot.writeFieldEnd()
        if self.replicaNum is not None:
            oprot.writeFieldBegin('replicaNum', TType.I32, 8)
            oprot.writeI32(self.replicaNum)
            oprot.writeFieldEnd()
        if self.pointsNum is not None:
            oprot.writeFieldBegin('pointsNum', TType.I64, 9)
            oprot.writeI64(self.pointsNum)
            oprot.writeFieldEnd()
        if self.aggregateType is not None:
            oprot.writeFieldBegin('aggregateType', TType.I32, 10)
            oprot.writeI32(self.aggregateType)
            oprot.writeFieldEnd()
        if self.parseErrorMsg is not None:
            oprot.writeFieldBegin('parseErrorMsg', TType.STRING, 11)
            oprot.writeString(self.parseErrorMsg.encode('utf-8') if sys.version_info[0] == 2 else self.parseErrorMsg)
            oprot.writeFieldEnd()
        if self.limit is not None:
            oprot.writeFieldBegin('limit', TType.I32, 12)
            oprot.writeI32(self.limit)
            oprot.writeFieldEnd()
        if self.offset is not None:
            oprot.writeFieldBegin('offset', TType.I32, 13)
            oprot.writeI32(self.offset)
            oprot.writeFieldEnd()
        if self.orderByPath is not None:
            oprot.writeFieldBegin('orderByPath', TType.STRING, 14)
            oprot.writeString(self.orderByPath.encode('utf-8') if sys.version_info[0] == 2 else self.orderByPath)
            oprot.writeFieldEnd()
        if self.ascending is not None:
            oprot.writeFieldBegin('ascending', TType.BOOL, 15)
            oprot.writeBool(self.ascending)
            oprot.writeFieldEnd()
        if self.iginxInfos is not None:
            oprot.writeFieldBegin('iginxInfos', TType.LIST, 16)
            oprot.writeListBegin(TType.STRUCT, len(self.iginxInfos))
            for iter378 in self.iginxInfos:
                iter378.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.storageEngineInfos is not None:
            oprot.writeFieldBegin('storageEngineInfos', TType.LIST, 17)
            oprot.writeListBegin(TType.STRUCT, len(self.storageEngineInfos))
            for iter379 in self.storageEngineInfos:
                iter379.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.metaStorageInfos is not None:
            oprot.writeFieldBegin('metaStorageInfos', TType.LIST, 18)
            oprot.writeListBegin(TType.STRUCT, len(self.metaStorageInfos))
            for iter380 in self.metaStorageInfos:
                iter380.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.localMetaStorageInfo is not None:
            oprot.writeFieldBegin('localMetaStorageInfo', TType.STRUCT, 19)
            self.localMetaStorageInfo.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        if self.type is None:
            raise TProtocolException(message='Required field type is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class UpdateUserReq(object):
    """
    Attributes:
     - sessionId
     - username
     - password
     - auths

    """


    def __init__(self, sessionId=None, username=None, password=None, auths=None,):
        self.sessionId = sessionId
        self.username = username
        self.password = password
        self.auths = auths

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.username = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.password = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.SET:
                    self.auths = set()
                    (_etype384, _size381) = iprot.readSetBegin()
                    for _i385 in range(_size381):
                        _elem386 = iprot.readI32()
                        self.auths.add(_elem386)
                    iprot.readSetEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('UpdateUserReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.username is not None:
            oprot.writeFieldBegin('username', TType.STRING, 2)
            oprot.writeString(self.username.encode('utf-8') if sys.version_info[0] == 2 else self.username)
            oprot.writeFieldEnd()
        if self.password is not None:
            oprot.writeFieldBegin('password', TType.STRING, 3)
            oprot.writeString(self.password.encode('utf-8') if sys.version_info[0] == 2 else self.password)
            oprot.writeFieldEnd()
        if self.auths is not None:
            oprot.writeFieldBegin('auths', TType.SET, 4)
            oprot.writeSetBegin(TType.I32, len(self.auths))
            for iter387 in self.auths:
                oprot.writeI32(iter387)
            oprot.writeSetEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.username is None:
            raise TProtocolException(message='Required field username is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class AddUserReq(object):
    """
    Attributes:
     - sessionId
     - username
     - password
     - auths

    """


    def __init__(self, sessionId=None, username=None, password=None, auths=None,):
        self.sessionId = sessionId
        self.username = username
        self.password = password
        self.auths = auths

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.username = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.password = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.SET:
                    self.auths = set()
                    (_etype391, _size388) = iprot.readSetBegin()
                    for _i392 in range(_size388):
                        _elem393 = iprot.readI32()
                        self.auths.add(_elem393)
                    iprot.readSetEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('AddUserReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.username is not None:
            oprot.writeFieldBegin('username', TType.STRING, 2)
            oprot.writeString(self.username.encode('utf-8') if sys.version_info[0] == 2 else self.username)
            oprot.writeFieldEnd()
        if self.password is not None:
            oprot.writeFieldBegin('password', TType.STRING, 3)
            oprot.writeString(self.password.encode('utf-8') if sys.version_info[0] == 2 else self.password)
            oprot.writeFieldEnd()
        if self.auths is not None:
            oprot.writeFieldBegin('auths', TType.SET, 4)
            oprot.writeSetBegin(TType.I32, len(self.auths))
            for iter394 in self.auths:
                oprot.writeI32(iter394)
            oprot.writeSetEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.username is None:
            raise TProtocolException(message='Required field username is unset!')
        if self.password is None:
            raise TProtocolException(message='Required field password is unset!')
        if self.auths is None:
            raise TProtocolException(message='Required field auths is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class DeleteUserReq(object):
    """
    Attributes:
     - sessionId
     - username

    """


    def __init__(self, sessionId=None, username=None,):
        self.sessionId = sessionId
        self.username = username

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.username = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('DeleteUserReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.username is not None:
            oprot.writeFieldBegin('username', TType.STRING, 2)
            oprot.writeString(self.username.encode('utf-8') if sys.version_info[0] == 2 else self.username)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        if self.username is None:
            raise TProtocolException(message='Required field username is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class GetUserReq(object):
    """
    Attributes:
     - sessionId
     - usernames

    """


    def __init__(self, sessionId=None, usernames=None,):
        self.sessionId = sessionId
        self.usernames = usernames

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.usernames = []
                    (_etype398, _size395) = iprot.readListBegin()
                    for _i399 in range(_size395):
                        _elem400 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.usernames.append(_elem400)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('GetUserReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        if self.usernames is not None:
            oprot.writeFieldBegin('usernames', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.usernames))
            for iter401 in self.usernames:
                oprot.writeString(iter401.encode('utf-8') if sys.version_info[0] == 2 else iter401)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class GetUserResp(object):
    """
    Attributes:
     - status
     - usernames
     - userTypes
     - auths

    """


    def __init__(self, status=None, usernames=None, userTypes=None, auths=None,):
        self.status = status
        self.usernames = usernames
        self.userTypes = userTypes
        self.auths = auths

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.usernames = []
                    (_etype405, _size402) = iprot.readListBegin()
                    for _i406 in range(_size402):
                        _elem407 = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                        self.usernames.append(_elem407)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.userTypes = []
                    (_etype411, _size408) = iprot.readListBegin()
                    for _i412 in range(_size408):
                        _elem413 = iprot.readI32()
                        self.userTypes.append(_elem413)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.auths = []
                    (_etype417, _size414) = iprot.readListBegin()
                    for _i418 in range(_size414):
                        _elem419 = set()
                        (_etype423, _size420) = iprot.readSetBegin()
                        for _i424 in range(_size420):
                            _elem425 = iprot.readI32()
                            _elem419.add(_elem425)
                        iprot.readSetEnd()
                        self.auths.append(_elem419)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('GetUserResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.usernames is not None:
            oprot.writeFieldBegin('usernames', TType.LIST, 2)
            oprot.writeListBegin(TType.STRING, len(self.usernames))
            for iter426 in self.usernames:
                oprot.writeString(iter426.encode('utf-8') if sys.version_info[0] == 2 else iter426)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.userTypes is not None:
            oprot.writeFieldBegin('userTypes', TType.LIST, 3)
            oprot.writeListBegin(TType.I32, len(self.userTypes))
            for iter427 in self.userTypes:
                oprot.writeI32(iter427)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.auths is not None:
            oprot.writeFieldBegin('auths', TType.LIST, 4)
            oprot.writeListBegin(TType.SET, len(self.auths))
            for iter428 in self.auths:
                oprot.writeSetBegin(TType.I32, len(iter428))
                for iter429 in iter428:
                    oprot.writeI32(iter429)
                oprot.writeSetEnd()
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class GetClusterInfoReq(object):
    """
    Attributes:
     - sessionId

    """


    def __init__(self, sessionId=None,):
        self.sessionId = sessionId

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.sessionId = iprot.readI64()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('GetClusterInfoReq')
        if self.sessionId is not None:
            oprot.writeFieldBegin('sessionId', TType.I64, 1)
            oprot.writeI64(self.sessionId)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.sessionId is None:
            raise TProtocolException(message='Required field sessionId is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class IginxInfo(object):
    """
    Attributes:
     - id
     - ip
     - port

    """


    def __init__(self, id=None, ip=None, port=None,):
        self.id = id
        self.ip = ip
        self.port = port

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.id = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.ip = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I32:
                    self.port = iprot.readI32()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('IginxInfo')
        if self.id is not None:
            oprot.writeFieldBegin('id', TType.I64, 1)
            oprot.writeI64(self.id)
            oprot.writeFieldEnd()
        if self.ip is not None:
            oprot.writeFieldBegin('ip', TType.STRING, 2)
            oprot.writeString(self.ip.encode('utf-8') if sys.version_info[0] == 2 else self.ip)
            oprot.writeFieldEnd()
        if self.port is not None:
            oprot.writeFieldBegin('port', TType.I32, 3)
            oprot.writeI32(self.port)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.id is None:
            raise TProtocolException(message='Required field id is unset!')
        if self.ip is None:
            raise TProtocolException(message='Required field ip is unset!')
        if self.port is None:
            raise TProtocolException(message='Required field port is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class StorageEngineInfo(object):
    """
    Attributes:
     - id
     - ip
     - port
     - type

    """


    def __init__(self, id=None, ip=None, port=None, type=None,):
        self.id = id
        self.ip = ip
        self.port = port
        self.type = type

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.I64:
                    self.id = iprot.readI64()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.STRING:
                    self.ip = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.I32:
                    self.port = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.STRING:
                    self.type = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('StorageEngineInfo')
        if self.id is not None:
            oprot.writeFieldBegin('id', TType.I64, 1)
            oprot.writeI64(self.id)
            oprot.writeFieldEnd()
        if self.ip is not None:
            oprot.writeFieldBegin('ip', TType.STRING, 2)
            oprot.writeString(self.ip.encode('utf-8') if sys.version_info[0] == 2 else self.ip)
            oprot.writeFieldEnd()
        if self.port is not None:
            oprot.writeFieldBegin('port', TType.I32, 3)
            oprot.writeI32(self.port)
            oprot.writeFieldEnd()
        if self.type is not None:
            oprot.writeFieldBegin('type', TType.STRING, 4)
            oprot.writeString(self.type.encode('utf-8') if sys.version_info[0] == 2 else self.type)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.id is None:
            raise TProtocolException(message='Required field id is unset!')
        if self.ip is None:
            raise TProtocolException(message='Required field ip is unset!')
        if self.port is None:
            raise TProtocolException(message='Required field port is unset!')
        if self.type is None:
            raise TProtocolException(message='Required field type is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class MetaStorageInfo(object):
    """
    Attributes:
     - ip
     - port
     - type

    """


    def __init__(self, ip=None, port=None, type=None,):
        self.ip = ip
        self.port = port
        self.type = type

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.ip = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.I32:
                    self.port = iprot.readI32()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.STRING:
                    self.type = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('MetaStorageInfo')
        if self.ip is not None:
            oprot.writeFieldBegin('ip', TType.STRING, 1)
            oprot.writeString(self.ip.encode('utf-8') if sys.version_info[0] == 2 else self.ip)
            oprot.writeFieldEnd()
        if self.port is not None:
            oprot.writeFieldBegin('port', TType.I32, 2)
            oprot.writeI32(self.port)
            oprot.writeFieldEnd()
        if self.type is not None:
            oprot.writeFieldBegin('type', TType.STRING, 3)
            oprot.writeString(self.type.encode('utf-8') if sys.version_info[0] == 2 else self.type)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.ip is None:
            raise TProtocolException(message='Required field ip is unset!')
        if self.port is None:
            raise TProtocolException(message='Required field port is unset!')
        if self.type is None:
            raise TProtocolException(message='Required field type is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class LocalMetaStorageInfo(object):
    """
    Attributes:
     - path

    """


    def __init__(self, path=None,):
        self.path = path

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRING:
                    self.path = iprot.readString().decode('utf-8') if sys.version_info[0] == 2 else iprot.readString()
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('LocalMetaStorageInfo')
        if self.path is not None:
            oprot.writeFieldBegin('path', TType.STRING, 1)
            oprot.writeString(self.path.encode('utf-8') if sys.version_info[0] == 2 else self.path)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.path is None:
            raise TProtocolException(message='Required field path is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)


class GetClusterInfoResp(object):
    """
    Attributes:
     - status
     - iginxInfos
     - storageEngineInfos
     - metaStorageInfos
     - localMetaStorageInfo

    """


    def __init__(self, status=None, iginxInfos=None, storageEngineInfos=None, metaStorageInfos=None, localMetaStorageInfo=None,):
        self.status = status
        self.iginxInfos = iginxInfos
        self.storageEngineInfos = storageEngineInfos
        self.metaStorageInfos = metaStorageInfos
        self.localMetaStorageInfo = localMetaStorageInfo

    def read(self, iprot):
        if iprot._fast_decode is not None and isinstance(iprot.trans, TTransport.CReadableTransport) and self.thrift_spec is not None:
            iprot._fast_decode(self, iprot, [self.__class__, self.thrift_spec])
            return
        iprot.readStructBegin()
        while True:
            (fname, ftype, fid) = iprot.readFieldBegin()
            if ftype == TType.STOP:
                break
            if fid == 1:
                if ftype == TType.STRUCT:
                    self.status = Status()
                    self.status.read(iprot)
                else:
                    iprot.skip(ftype)
            elif fid == 2:
                if ftype == TType.LIST:
                    self.iginxInfos = []
                    (_etype433, _size430) = iprot.readListBegin()
                    for _i434 in range(_size430):
                        _elem435 = IginxInfo()
                        _elem435.read(iprot)
                        self.iginxInfos.append(_elem435)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 3:
                if ftype == TType.LIST:
                    self.storageEngineInfos = []
                    (_etype439, _size436) = iprot.readListBegin()
                    for _i440 in range(_size436):
                        _elem441 = StorageEngineInfo()
                        _elem441.read(iprot)
                        self.storageEngineInfos.append(_elem441)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 4:
                if ftype == TType.LIST:
                    self.metaStorageInfos = []
                    (_etype445, _size442) = iprot.readListBegin()
                    for _i446 in range(_size442):
                        _elem447 = MetaStorageInfo()
                        _elem447.read(iprot)
                        self.metaStorageInfos.append(_elem447)
                    iprot.readListEnd()
                else:
                    iprot.skip(ftype)
            elif fid == 5:
                if ftype == TType.STRUCT:
                    self.localMetaStorageInfo = LocalMetaStorageInfo()
                    self.localMetaStorageInfo.read(iprot)
                else:
                    iprot.skip(ftype)
            else:
                iprot.skip(ftype)
            iprot.readFieldEnd()
        iprot.readStructEnd()

    def write(self, oprot):
        if oprot._fast_encode is not None and self.thrift_spec is not None:
            oprot.trans.write(oprot._fast_encode(self, [self.__class__, self.thrift_spec]))
            return
        oprot.writeStructBegin('GetClusterInfoResp')
        if self.status is not None:
            oprot.writeFieldBegin('status', TType.STRUCT, 1)
            self.status.write(oprot)
            oprot.writeFieldEnd()
        if self.iginxInfos is not None:
            oprot.writeFieldBegin('iginxInfos', TType.LIST, 2)
            oprot.writeListBegin(TType.STRUCT, len(self.iginxInfos))
            for iter448 in self.iginxInfos:
                iter448.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.storageEngineInfos is not None:
            oprot.writeFieldBegin('storageEngineInfos', TType.LIST, 3)
            oprot.writeListBegin(TType.STRUCT, len(self.storageEngineInfos))
            for iter449 in self.storageEngineInfos:
                iter449.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.metaStorageInfos is not None:
            oprot.writeFieldBegin('metaStorageInfos', TType.LIST, 4)
            oprot.writeListBegin(TType.STRUCT, len(self.metaStorageInfos))
            for iter450 in self.metaStorageInfos:
                iter450.write(oprot)
            oprot.writeListEnd()
            oprot.writeFieldEnd()
        if self.localMetaStorageInfo is not None:
            oprot.writeFieldBegin('localMetaStorageInfo', TType.STRUCT, 5)
            self.localMetaStorageInfo.write(oprot)
            oprot.writeFieldEnd()
        oprot.writeFieldStop()
        oprot.writeStructEnd()

    def validate(self):
        if self.status is None:
            raise TProtocolException(message='Required field status is unset!')
        return

    def __repr__(self):
        L = ['%s=%r' % (key, value)
             for key, value in self.__dict__.items()]
        return '%s(%s)' % (self.__class__.__name__, ', '.join(L))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
all_structs.append(Status)
Status.thrift_spec = (
    None,  # 0
    (1, TType.I32, 'code', None, None, ),  # 1
    (2, TType.STRING, 'message', 'UTF8', None, ),  # 2
    (3, TType.LIST, 'subStatus', (TType.STRUCT, [Status, None], False), None, ),  # 3
)
all_structs.append(OpenSessionReq)
OpenSessionReq.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'username', 'UTF8', None, ),  # 1
    (2, TType.STRING, 'password', 'UTF8', None, ),  # 2
)
all_structs.append(OpenSessionResp)
OpenSessionResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.I64, 'sessionId', None, None, ),  # 2
)
all_structs.append(CloseSessionReq)
CloseSessionReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
)
all_structs.append(DeleteColumnsReq)
DeleteColumnsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
)
all_structs.append(InsertColumnRecordsReq)
InsertColumnRecordsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.STRING, 'timestamps', 'BINARY', None, ),  # 3
    (4, TType.LIST, 'valuesList', (TType.STRING, 'BINARY', False), None, ),  # 4
    (5, TType.LIST, 'bitmapList', (TType.STRING, 'BINARY', False), None, ),  # 5
    (6, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 6
    (7, TType.LIST, 'attributesList', (TType.MAP, (TType.STRING, 'UTF8', TType.STRING, 'UTF8', False), False), None, ),  # 7
)
all_structs.append(InsertNonAlignedColumnRecordsReq)
InsertNonAlignedColumnRecordsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.STRING, 'timestamps', 'BINARY', None, ),  # 3
    (4, TType.LIST, 'valuesList', (TType.STRING, 'BINARY', False), None, ),  # 4
    (5, TType.LIST, 'bitmapList', (TType.STRING, 'BINARY', False), None, ),  # 5
    (6, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 6
    (7, TType.LIST, 'attributesList', (TType.MAP, (TType.STRING, 'UTF8', TType.STRING, 'UTF8', False), False), None, ),  # 7
)
all_structs.append(InsertRowRecordsReq)
InsertRowRecordsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.STRING, 'timestamps', 'BINARY', None, ),  # 3
    (4, TType.LIST, 'valuesList', (TType.STRING, 'BINARY', False), None, ),  # 4
    (5, TType.LIST, 'bitmapList', (TType.STRING, 'BINARY', False), None, ),  # 5
    (6, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 6
    (7, TType.LIST, 'attributesList', (TType.MAP, (TType.STRING, 'UTF8', TType.STRING, 'UTF8', False), False), None, ),  # 7
)
all_structs.append(InsertNonAlignedRowRecordsReq)
InsertNonAlignedRowRecordsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.STRING, 'timestamps', 'BINARY', None, ),  # 3
    (4, TType.LIST, 'valuesList', (TType.STRING, 'BINARY', False), None, ),  # 4
    (5, TType.LIST, 'bitmapList', (TType.STRING, 'BINARY', False), None, ),  # 5
    (6, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 6
    (7, TType.LIST, 'attributesList', (TType.MAP, (TType.STRING, 'UTF8', TType.STRING, 'UTF8', False), False), None, ),  # 7
)
all_structs.append(DeleteDataInColumnsReq)
DeleteDataInColumnsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.I64, 'startTime', None, None, ),  # 3
    (4, TType.I64, 'endTime', None, None, ),  # 4
)
all_structs.append(QueryDataSet)
QueryDataSet.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'timestamps', 'BINARY', None, ),  # 1
    (2, TType.LIST, 'valuesList', (TType.STRING, 'BINARY', False), None, ),  # 2
    (3, TType.LIST, 'bitmapList', (TType.STRING, 'BINARY', False), None, ),  # 3
)
all_structs.append(QueryDataReq)
QueryDataReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.I64, 'startTime', None, None, ),  # 3
    (4, TType.I64, 'endTime', None, None, ),  # 4
)
all_structs.append(QueryDataResp)
QueryDataResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 3
    (4, TType.STRUCT, 'queryDataSet', [QueryDataSet, None], None, ),  # 4
)
all_structs.append(AddStorageEnginesReq)
AddStorageEnginesReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'storageEngines', (TType.STRUCT, [StorageEngine, None], False), None, ),  # 2
)
all_structs.append(StorageEngine)
StorageEngine.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'ip', 'UTF8', None, ),  # 1
    (2, TType.I32, 'port', None, None, ),  # 2
    (3, TType.STRING, 'type', 'UTF8', None, ),  # 3
    (4, TType.MAP, 'extraParams', (TType.STRING, 'UTF8', TType.STRING, 'UTF8', False), None, ),  # 4
)
all_structs.append(AggregateQueryReq)
AggregateQueryReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.I64, 'startTime', None, None, ),  # 3
    (4, TType.I64, 'endTime', None, None, ),  # 4
    (5, TType.I32, 'aggregateType', None, None, ),  # 5
)
all_structs.append(AggregateQueryResp)
AggregateQueryResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 3
    (4, TType.STRING, 'timestamps', 'BINARY', None, ),  # 4
    (5, TType.STRING, 'valuesList', 'BINARY', None, ),  # 5
)
all_structs.append(ValueFilterQueryReq)
ValueFilterQueryReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.I64, 'startTime', None, None, ),  # 3
    (4, TType.I64, 'endTime', None, None, ),  # 4
    (5, TType.STRING, 'booleanExpression', 'UTF8', None, ),  # 5
)
all_structs.append(ValueFilterQueryResp)
ValueFilterQueryResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 3
    (4, TType.STRUCT, 'queryDataSet', [QueryDataSet, None], None, ),  # 4
)
all_structs.append(LastQueryReq)
LastQueryReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.I64, 'startTime', None, None, ),  # 3
)
all_structs.append(LastQueryResp)
LastQueryResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 3
    (4, TType.STRING, 'timestamps', 'BINARY', None, ),  # 4
    (5, TType.STRING, 'valuesList', 'BINARY', None, ),  # 5
)
all_structs.append(DownsampleQueryReq)
DownsampleQueryReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.I64, 'startTime', None, None, ),  # 3
    (4, TType.I64, 'endTime', None, None, ),  # 4
    (5, TType.I32, 'aggregateType', None, None, ),  # 5
    (6, TType.I64, 'precision', None, None, ),  # 6
)
all_structs.append(DownsampleQueryResp)
DownsampleQueryResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 3
    (4, TType.STRUCT, 'queryDataSet', [QueryDataSet, None], None, ),  # 4
)
all_structs.append(ShowColumnsReq)
ShowColumnsReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
)
all_structs.append(ShowColumnsResp)
ShowColumnsResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 3
)
all_structs.append(GetReplicaNumReq)
GetReplicaNumReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
)
all_structs.append(GetReplicaNumResp)
GetReplicaNumResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.I32, 'replicaNum', None, None, ),  # 2
)
all_structs.append(ExecuteSqlReq)
ExecuteSqlReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.STRING, 'statement', 'UTF8', None, ),  # 2
)
all_structs.append(ExecuteSqlResp)
ExecuteSqlResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.I32, 'type', None, None, ),  # 2
    (3, TType.LIST, 'paths', (TType.STRING, 'UTF8', False), None, ),  # 3
    (4, TType.LIST, 'dataTypeList', (TType.I32, None, False), None, ),  # 4
    (5, TType.STRUCT, 'queryDataSet', [QueryDataSet, None], None, ),  # 5
    (6, TType.STRING, 'timestamps', 'BINARY', None, ),  # 6
    (7, TType.STRING, 'valuesList', 'BINARY', None, ),  # 7
    (8, TType.I32, 'replicaNum', None, None, ),  # 8
    (9, TType.I64, 'pointsNum', None, None, ),  # 9
    (10, TType.I32, 'aggregateType', None, None, ),  # 10
    (11, TType.STRING, 'parseErrorMsg', 'UTF8', None, ),  # 11
    (12, TType.I32, 'limit', None, None, ),  # 12
    (13, TType.I32, 'offset', None, None, ),  # 13
    (14, TType.STRING, 'orderByPath', 'UTF8', None, ),  # 14
    (15, TType.BOOL, 'ascending', None, None, ),  # 15
    (16, TType.LIST, 'iginxInfos', (TType.STRUCT, [IginxInfo, None], False), None, ),  # 16
    (17, TType.LIST, 'storageEngineInfos', (TType.STRUCT, [StorageEngineInfo, None], False), None, ),  # 17
    (18, TType.LIST, 'metaStorageInfos', (TType.STRUCT, [MetaStorageInfo, None], False), None, ),  # 18
    (19, TType.STRUCT, 'localMetaStorageInfo', [LocalMetaStorageInfo, None], None, ),  # 19
)
all_structs.append(UpdateUserReq)
UpdateUserReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.STRING, 'username', 'UTF8', None, ),  # 2
    (3, TType.STRING, 'password', 'UTF8', None, ),  # 3
    (4, TType.SET, 'auths', (TType.I32, None, False), None, ),  # 4
)
all_structs.append(AddUserReq)
AddUserReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.STRING, 'username', 'UTF8', None, ),  # 2
    (3, TType.STRING, 'password', 'UTF8', None, ),  # 3
    (4, TType.SET, 'auths', (TType.I32, None, False), None, ),  # 4
)
all_structs.append(DeleteUserReq)
DeleteUserReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.STRING, 'username', 'UTF8', None, ),  # 2
)
all_structs.append(GetUserReq)
GetUserReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
    (2, TType.LIST, 'usernames', (TType.STRING, 'UTF8', False), None, ),  # 2
)
all_structs.append(GetUserResp)
GetUserResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'usernames', (TType.STRING, 'UTF8', False), None, ),  # 2
    (3, TType.LIST, 'userTypes', (TType.I32, None, False), None, ),  # 3
    (4, TType.LIST, 'auths', (TType.SET, (TType.I32, None, False), False), None, ),  # 4
)
all_structs.append(GetClusterInfoReq)
GetClusterInfoReq.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'sessionId', None, None, ),  # 1
)
all_structs.append(IginxInfo)
IginxInfo.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'id', None, None, ),  # 1
    (2, TType.STRING, 'ip', 'UTF8', None, ),  # 2
    (3, TType.I32, 'port', None, None, ),  # 3
)
all_structs.append(StorageEngineInfo)
StorageEngineInfo.thrift_spec = (
    None,  # 0
    (1, TType.I64, 'id', None, None, ),  # 1
    (2, TType.STRING, 'ip', 'UTF8', None, ),  # 2
    (3, TType.I32, 'port', None, None, ),  # 3
    (4, TType.STRING, 'type', 'UTF8', None, ),  # 4
)
all_structs.append(MetaStorageInfo)
MetaStorageInfo.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'ip', 'UTF8', None, ),  # 1
    (2, TType.I32, 'port', None, None, ),  # 2
    (3, TType.STRING, 'type', 'UTF8', None, ),  # 3
)
all_structs.append(LocalMetaStorageInfo)
LocalMetaStorageInfo.thrift_spec = (
    None,  # 0
    (1, TType.STRING, 'path', 'UTF8', None, ),  # 1
)
all_structs.append(GetClusterInfoResp)
GetClusterInfoResp.thrift_spec = (
    None,  # 0
    (1, TType.STRUCT, 'status', [Status, None], None, ),  # 1
    (2, TType.LIST, 'iginxInfos', (TType.STRUCT, [IginxInfo, None], False), None, ),  # 2
    (3, TType.LIST, 'storageEngineInfos', (TType.STRUCT, [StorageEngineInfo, None], False), None, ),  # 3
    (4, TType.LIST, 'metaStorageInfos', (TType.STRUCT, [MetaStorageInfo, None], False), None, ),  # 4
    (5, TType.STRUCT, 'localMetaStorageInfo', [LocalMetaStorageInfo, None], None, ),  # 5
)
fix_spec(all_structs)
del all_structs
