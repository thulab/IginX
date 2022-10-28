import argparse

from zk import ZooKeeperMetaServer
from iginx_db import IGinXMetaServer
from plt import plot

ZK = 'zookeeper'
IGINX = 'iginx'

parse = argparse.ArgumentParser(prog='分片信息可视化工具', usage='python main.py [source] [ip] [port] [target path]',
                                description='选定数据源，生成分片信息图，并存储到目标文件中',
                                epilog='Example: python main.py zookeeper 127.0.0.1 2181 fragment.png')

parse.add_argument('source', type=str, help='信息源：支持 iginx、zookeeper', choices=[ZK, IGINX])
parse.add_argument('ip', type=str, help='数据源 ip')
parse.add_argument('port', type=str, help='数据源 port')
parse.add_argument('target', type=str, help='图片存储的目标文件')


if __name__ == "__main__":

    args = parse.parse_args()

    meta_server = None

    if args.source == ZK:
        meta_server = ZooKeeperMetaServer(args.ip + ':' + args.port)
    elif args.source == IGINX:
        meta_server = IGinXMetaServer(args.ip, args.port)

    if meta_server is None:
        print("unknown source " + args.source)
        exit(1)

    meta_server.start()

    storage_units = meta_server.get_storage_units()
    storage_engines = meta_server.get_storage_engines()
    fragments = meta_server.get_fragments()

    plot(storage_engines, storage_units, fragments, args.target)