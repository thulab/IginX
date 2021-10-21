from .thrift.rpc.ttypes import DataType

class TimeSeries(object):

    def __init__(self, path, type):
        self.__path = path
        self.__type = type


    def get_path(self):
        return self.__path


    def get_type(self):
        return self.__type


    def __str__(self):
        return self.__path + " " + DataType._VALUES_TO_NAMES[self.__type]