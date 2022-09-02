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

    # 获取集群拓扑信息
    cluster_info = session.get_cluster_info()
    print(cluster_info)

    # 写入数据
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

    # 查询写入的数据
    dataset = session.query(["a.*"], 0, 10)
    print(dataset)

    # 使用 SQL 语句查询写入的数据
    dataset = session.execute_statement("select * from a", fetch_size=2)

    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()

    dataset.close()

    # 使用 SQL 语句查询集群信息
    dataset = session.execute_statement("show cluster info", fetch_size=2)

    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()

    dataset.close()

    # 使用 SQL 语句查询副本数量
    dataset = session.execute_statement("show replica number", fetch_size=2)

    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()

    dataset.close()

    # 使用 SQL 语句查询时间序列
    dataset = session.execute_statement("show time series", fetch_size=2)

    columns = dataset.columns()
    for column in columns:
        print(column, end="\t")
    print()

    while dataset.has_more():
        row = dataset.next()
        for field in row:
            print(str(field), end="\t\t")
        print()
    print()

    dataset.close()


    # 查询写入的数据
    dataset = session.query(["*"], 0, 10)
    print(dataset)


    # 统计每个序列的点数
    dataset = session.aggregate_query(["*"], 0, 10, AggregateType.COUNT)
    print(dataset)

    # 获取部分序列的最后一个数据点
    dataset = session.last_query(["a.a.*"], 0)
    print(dataset)

    # 删除部分数据
    session.delete_time_series("a.b.b")

    # 查询删除后剩余的数据
    dataset = session.query(["*"], 0, 10)
    print(dataset)

    session.batch_delete_time_series(["*"])

    # 查询删除全部后剩余的数据
    dataset = session.query(["*"], 0, 10)
    print(dataset)

    session.close()
    print("关闭 session 成功")
