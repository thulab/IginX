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
from iginx.session import Session
from iginx.thrift.rpc.ttypes import DataType, AggregateType


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()

    cluster_info = session.get_cluster_info()
    print(cluster_info)

    paths = ["a.a.a", "a.a.b", "a.b.b", "a.c.c"]
    timestamps = [1, 2, 3, 4]
    values_list = [
        ['a', 'b', None, None],
        [None, None, 'b', None],
        [None, None, None, 'c'],
        ['Q', 'W', 'E', 'R'],
    ]
    data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
    session.insert_row_records(paths, timestamps, values_list, data_type_list)

    dataset = session.query(["*"], 0, 10)
    print(dataset)

    dataset = session.aggregate_query(["*"], 0, 10, AggregateType.COUNT)
    print(dataset)

    dataset = session.last_query(["a.a.*"], 0)
    print(dataset)

    values_list = dataset.get_values()

    timestamps = dataset.get_timestamps()
    timestamp = max(timestamps)
    row = {}

    row[u'timestamp'] = timestamp
    for i in range(len(values_list)):
        if timestamps[i] != timestamp:
            continue
        values = values_list[i]
        row[values[0]] = values[1]


    session.delete_time_series("a.b.b")

    dataset = session.query(["*"], 0, 10)
    print(dataset)

    dataset = session.query(["*"], 0, 10)
    print(dataset)

    session.close()
