from iginx.session import Session
from iginx.thrift.rpc.ttypes import DataType

from datetime import datetime

import random
import time

def get_now_time_sign():
    return int(round(datetime.now().timestamp() * 1000))


if __name__ == '__main__':
    session = Session('11.101.17.23', 2333, "root", "root")
    session.open()

    paths = ["computing_center.host_a.cpu_usage", "computing_center.host_a.memory_usage", "computing_center.host_b.cpu_usage", "computing_center.host_b.memory_usage"]
    data_type_list = [DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE]

    cnt = 0
    while cnt < 50:
        timestamp = get_now_time_sign()
        print("Write data to IginX, current timestamp:" + str(timestamp))
        values = []
        for i in range(len(paths)):
            values.append(random.randint(0, 10000) / 100.0)
        session.insert_row_records(paths, [timestamp], [values], data_type_list)
        time.sleep(5)
        cnt += 1

    session.close()