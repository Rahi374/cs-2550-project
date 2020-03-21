from bitstring import BitArray
from common import *
from math import ceil, floor, inf, log, sqrt
import numpy as np
from record import Record

class SlottedPage():
    num_records = 0
    max_num_records = inf
    addr_bit_width = 0
    block_size = 0
    num_bytes_for_pointers = 0

    records = []

    def __init__(self, block: bytearray, block_size: int):
        if not isinstance(block_size, int):
            raise TypeError("block_size must be integer")
        if not isinstance(block, bytearray):
            raise TypeError("block must be bytearray")
        if not len(block) == block_size:
            raise Exception("invalid block size")

        self.block_size = block_size
        self.addr_bit_width = ceil(log(block_size, 2))
        self.max_num_records = floor(block_size/RECORD_SIZE)
        self.num_bytes_for_pointers = ceil((self.addr_bit_width * self.max_num_records)/8)
        num_extra_bytes = block_size % RECORD_SIZE
        num_records_to_remove = ceil((self.num_bytes_for_pointers - num_extra_bytes)/RECORD_SIZE)
        self.max_num_records -= num_records_to_remove

        self.records = [None] * self.max_num_records

        ba = BitArray(block)
        for i in range(0, self.max_num_records):
            bit_index_1 = -self.addr_bit_width * i if i is not 0 else None
            bit_index_2 = -self.addr_bit_width * (i+1)
            off = ba[bit_index_2:bit_index_1].int
            if off >= block_size:
                continue
            id = block[off] + (block[off+1] << 8) + (block[off+2] << 16) + (block[off+3] << 24)
            name = block[off+4:off+20].decode("ascii")
            phone = block[off+20:off+32].decode("ascii")
            if id == 0 and name[0] == "\0" and phone[0] == "\0":
                continue
            self.records[i] = Record(id, name, phone)
            self.num_records += 1


    def __getitem__(self, key):
        if not isinstance(key, int):
            raise TypeError("index must be integer")
        return self.records[key]

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            raise TypeError("index must be integer")
        if key >= self.max_num_records:
            raise Exception("index out of bounds")
        if value is None:
            self.records[key] = None
            self.num_records -= 1
            return
        if not isinstance(value, Record):
            raise TypeError("record to be inserted must be a Record") 
        self.records[key] = value

    def __len__(self):
        return self.num_records

    def max_records(self):
        return self.max_num_records

    def to_bytearray(self):
        ba = bytearray(self.block_size - self.num_bytes_for_pointers)
        pointers = BitArray()
        offset = -1
        for i in range(0, self.max_num_records):
            if self.records[i] is None:
                offset = -1
            else:
                ba[i*RECORD_SIZE:(i+1)*RECORD_SIZE] = self.records[i].to_bytearray()
                offset = i * RECORD_SIZE
            pointers.prepend(BitArray(bin=np.binary_repr(offset, width=self.addr_bit_width)))

        return ba + pad_bits(pointers, self.num_bytes_for_pointers).bytes


