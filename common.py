from bitstring import BitArray
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
