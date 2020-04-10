from bitstring import BitArray
from enum import Enum
import pprint

pp = pprint.PrettyPrinter(indent=4)

RECORD_SIZE = 32

class SCHED_TYPE(Enum):
    RR = 0
    RAND = 1

class EXEC_TYPE(Enum):
    PROCESS = 0
    TRANSACTION = 1

class ORG(Enum):
    SEQ = 1
    LSM = 2

class ACTION(Enum):
    RETRIEVE_BY_ID = 1
    RETRIEVE_BY_AREA_CODE = 2
    WRITE_RECORD = 3
    DELETE_RECORD = 4
    DELETE_TABLE = 5

ACTION_WORDS = {
    ACTION.RETRIEVE_BY_ID: "Retrieve by ID",
    ACTION.RETRIEVE_BY_AREA_CODE: "Retrieve by area code",
    ACTION.WRITE_RECORD: "Write record",
    ACTION.DELETE_RECORD: "Delete record",
    ACTION.DELETE_TABLE: "Delete table"
}

ACTION_CODES = {
    ACTION.RETRIEVE_BY_ID: "R",
    ACTION.RETRIEVE_BY_AREA_CODE: "M",
    ACTION.WRITE_RECORD: "W",
    ACTION.DELETE_RECORD: "E",
    ACTION.DELETE_TABLE: "D"
}

def pad_bits(ba: BitArray, target_len: int):
    l = len(ba)
    if l % 8 == 0:
        return ba
    target_len *= 8
    amount_to_pad = target_len - l
    return BitArray(amount_to_pad) + ba

def pad_ba(ba: bytearray, length: int):
    l = len(ba)
    for i in range(l, length):
        ba.append(0)
    return ba
