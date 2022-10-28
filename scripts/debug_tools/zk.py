import json

from kazoo.client import KazooClient
from entity import TimeRange, TimeSeriesRange, Fragment, StorageEngine, StorageUnit

class ZooKeeperMetaServer(object):

    FRAGMENT = "/fragment"

    STORAGE_ENGINE = "/storage"

    UNIT = "/unit"

    def __init__(self, hosts):
        self.zk = KazooClient(hosts=hosts)

    def start(self, timeout=10):
        self.zk.start(timeout=timeout)

    def close(self):
        self.zk.stop()
        self.zk.close()

    def get_fragments(self):
        fragments = []
        series_ranges = self.zk.get_children(ZooKeeperMetaServer.FRAGMENT)
        for series_range in series_ranges:
            start_times = self.zk.get_children(ZooKeeperMetaServer.FRAGMENT + "/" + series_range)
            for start_time in start_times:
                value = self.zk.get(ZooKeeperMetaServer.FRAGMENT + "/" + series_range + "/" + start_time)[0]
                data = json.loads(value)
                time_range = TimeRange(data['timeInterval']['startTime'], data['timeInterval']['endTime'])
                ts_range = TimeSeriesRange(data['tsInterval'].get('startTimeSeries', TimeSeriesRange.UNBOUNDED_FROM), data['tsInterval'].get('endTimeSeries', TimeSeriesRange.UNBOUNDED_TO))
                fragments.append(Fragment(time_range, ts_range, data['masterStorageUnitId']))
        return fragments


    def get_storage_units(self):
        storage_units = []
        unit_name_list = self.zk.get_children(ZooKeeperMetaServer.UNIT)
        for unit_name in unit_name_list:
            value = self.zk.get(ZooKeeperMetaServer.UNIT + "/" + unit_name)[0]
            data = json.loads(value)
            storage_units.append(StorageUnit(data['id'], data['masterId'], data['storageEngineId']))
        return storage_units

    def get_storage_engines(self):
        storage_engines = []
        storage_list = self.zk.get_children(ZooKeeperMetaServer.STORAGE_ENGINE)
        for storage in storage_list:
            value = self.zk.get(ZooKeeperMetaServer.STORAGE_ENGINE + "/" + storage)[0]
            data = json.loads(value)
            storage_engines.append(StorageEngine(data['id'], data['ip'], data['port']))
        return storage_engines