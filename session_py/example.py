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

    session.close()
    print("关闭 session 成功")
