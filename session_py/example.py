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
from iginx.session import Session, DataType, AggregateType


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()

    # 获取集群拓普信息
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


    # 写入数据
    paths = ["a.d.a", "a.d.b"]
    timestamps = [5, 6, 7, 8, 9]
    values_list = [
        [1, 10],
        [3, 8],
        [5, 6],
        [7, 4],
        [9, 2],
    ]
    data_type_list = [DataType.INTEGER, DataType.INTEGER]
    session.insert_row_records(paths, timestamps, values_list, data_type_list)

    # 使用 SQL 语句进行查询
    dataset = session.execute_sql("select * from a.d where time in [5, 10) order by b desc offset 4 limit 2").get_query_data_set()
    print(dataset)

    paths = session.list_sub_time_series("a.d")
    print(paths)

    session.close()
    print("关闭 session 成功")
