from iginx.session import Session, AggregateType


if __name__ == '__main__':
    session = Session('127.0.0.1', 6888, "root", "root")
    session.open()
    print("开启 session 成功")


    print("查询：")
    dataset = session.aggregate_query(["sg.d1.s1", "sg.d2.s1"], 0, 40000, AggregateType.COUNT)

    print(dataset)

    session.close()
    print("关闭 session 成功")
