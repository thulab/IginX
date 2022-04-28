from enum import IntEnum


class Status(IntEnum):
    SUCCESS = 0
    FAIL_TO_CREATE_SOCKET = -1
    FAIL_TO_BIND_ADDR = -2
    FAIL_TO_LOAD_CLASS = -3
