import os
import socket
import sys
import threading
import pyarrow as pa
from concurrent.futures.thread import ThreadPoolExecutor

from class_loader import load_class
from constant import Status


def main(argv):
    if len(argv) != 4:
        print("arguments len must be 4.")
        exit()
    worker = Worker(argv[1], argv[2], int(argv[3]))
    worker.send_auth_msg()
    worker.run()
    pass


class Worker(threading.Thread):
    def __init__(self, file_name, clazz_name, sender_port, host="127.0.0.1",
                 link_size=5, read_size=1024 * 1024, encoding='utf-8'):
        threading.Thread.__init__(self)
        self._status = Status.SUCCESS
        self._file_name = file_name
        self._clazz_name = clazz_name
        self._sender_port = sender_port
        self._host = host
        self._link_size = link_size
        self._read_size = read_size
        self._encoding = encoding
        self._pid = os.getpid()
        try:
            self._receiver = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error as e:
            self._status = Status.FAIL_TO_CREATE_SOCKET
            print("Failed to create socket. Error: %s" % e)
        try:
            self._receiver.bind((host, 0))
        except socket.error as e:
            self._status = Status.FAIL_TO_BIND_ADDR
            print("Failed to bind address. Error: %s" % e)

        self._receive_port = self._receiver.getsockname()[1]
        self._receiver.listen(5)
        self._pool = ThreadPoolExecutor(max_workers=self._link_size)

        # load class by name
        self._clazz, success = load_class(file_name, clazz_name)
        if not success:
            self._status = Status.FAIL_TO_LOAD_CLASS
        pass

    def run(self):
        while True:
            # 获取一个客户端连接
            client, addr = self._receiver.accept()
            print("socket address:%s" % str(addr))
            try:
                self._pool.submit(self.process_msg, client)
            except Exception as e:
                print("Failed to submit a new task: %s" % e)
        pass

    def send_auth_msg(self):
        """
        send the auth info like listen port and pid to the JVM
        """
        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect((self._host, self._sender_port))
        conn_file = sender.makefile(mode="wb")

        # get the pid and py port
        print("python worker pid is", self._pid)
        print("python worker port is", self._receive_port)
        print("python worker status is", self._status)
        data = [pa.array([self._pid]), pa.array([self._receive_port]), pa.array([int(self._status)])]
        batch = pa.record_batch(data, names=["pid", "port", "status"])

        try:
            with pa.ipc.new_stream(conn_file, batch.schema) as writer:
                writer.write_batch(batch)
            sender.close()
        except Exception as e:
            print("Failed to send auth msg: %s" % e)
        pass

    def process_msg(self, client):
        conn_file = client.makefile(mode="rb")
        reader = pa.ipc.RecordBatchStreamReader(conn_file)
        table = reader.read_all()

        df = table.to_pandas()
        conn_file.close()
        client.close()

        # user define logic
        ret = self._clazz.transform(df)

        self.send_msg(ret)
        pass

    def send_msg(self, df):
        sender = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sender.connect((self._host, self._sender_port))
        conn_file = sender.makefile(mode="wb")

        batch = pa.record_batch(df, names=df.columns.values.tolist())
        try:
            with pa.ipc.new_stream(conn_file, batch.schema) as writer:
                writer.write_batch(batch)
            sender.close()
        except Exception as e:
            print("Failed to send msg: %s" % e)
        pass


if __name__ == "__main__":
    main(sys.argv)
