import json
import iginx.thrift.rpc.ttypes as ttypes

from iginx.session import Session
from entity import TimeRange, TimeSeriesRange, Fragment, StorageEngine, StorageUnit

class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        else:
            return super(MyEncoder, self).default(obj)


class IGinXMetaServer(object):

    def __init__(self, host, port):
        self.session = Session(host=host, port=port)
        self.payload = None

    def start(self, timeout=None):
        self.session.open()

    def close(self):
        self.session.close()

    def get_fragments(self):
        self._get_payload()
        fragments = []
        for fragment in self.payload['fragments']:
            time_range = TimeRange(fragment['startTime'], fragment['endTime'])
            ts_range = TimeSeriesRange(fragment.get('startTs', TimeSeriesRange.UNBOUNDED_FROM),
                                       fragment.get('endTs', TimeSeriesRange.UNBOUNDED_TO))
            fragments.append(Fragment(time_range, ts_range, fragment['storageUnitId']))
        return fragments


    def get_storage_units(self):
        self._get_payload()
        units = []
        print(self.payload)
        for unit in self.payload['storageUnits']:
            units.append(StorageUnit(unit['id'], unit['masterId'], unit['storageId']))
        return units

    def get_storage_engines(self):
        self._get_payload()
        storage_engines = []
        for storage in self.payload['storages']:
            print(storage)
            storage_engines.append(StorageEngine(storage['id'], storage['ip'], storage['port']))
        return storage_engines

    def _get_payload(self):
        if self.payload is not None:
            return
        self.payload = json.loads(self.session.get_debug_info(b"{'byCache': true}", ttypes.DebugInfoType.GET_META))
