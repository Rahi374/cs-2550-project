from enum import Enum

RECORD_SIZE = 32

class ORG(Enum):
    SEQ = 1
    LSM = 2

class ACTION(Enum):
    RETRIEVE_BY_ID = 1
    RETRIEVE_BY_AREA_CODE = 2
    WRITE_RECORD = 3
    DELETE_TABLE = 4
