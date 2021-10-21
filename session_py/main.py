from iginx.session import Session, DataType


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()
    print("开启 session 成功")


    # paths = ["a.a.a", "a.a.b", "a.b.b", "a.c.c"]
    # timestamps = [2, 1, 3]
    # values_list = [
    #     ['a', 'b', None, None],
    #     [None, None, 'b', None],
    #     [None, None, None, 'c'],
    # ]
    # data_type_list = [DataType.BINARY, DataType.BINARY, DataType.BINARY, DataType.BINARY]
    # session.insert_row_records(paths, timestamps, values_list, data_type_list)

    paths = ["a.a.a", "b.b.b"]
    timestamps = [1, 2, 3]
    values_list = [
        ["a", "a", "a"],
        ["b", "b", "b"]
    ]
    data_type_list = [DataType.BINARY, DataType.BINARY]
    session.insert_column_records(paths, timestamps, values_list, data_type_list)

    dataset = session.query(["*"], 0, 10)
    print(dataset)

    session.close()
    print("关闭 session 成功")
