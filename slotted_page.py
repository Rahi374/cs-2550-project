from bitstring import BitArray
from common import *
from math import ceil, floor, inf, log, sqrt
import numpy as np
from record import Record

class SlottedPage():

    def __init__(self, block_size: int, block: bytearray = None):
        if block == None:
            block = bytearray(block_size)

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
        self.num_records = 0

        ba = BitArray(block)
        for i in range(0, self.max_num_records):
            bit_index_1 = -self.addr_bit_width * i if i is not 0 else None
            bit_index_2 = -self.addr_bit_width * (i+1)
            off = ba[bit_index_2:bit_index_1].int
            if off >= block_size:
                continue

            try:
                r = Record(ba=block[off:off+32])
            except Exception:
                continue
            self.records[i] = r
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

    def insert(self, record):
        # insert to first available slot, if available
        for i, rec in enumerate(self.records):
            if self.records[i] == None:
                self.records[i] = record
                self.num_records += 1
                return
        raise Exception("no space to insert record")

    def delete(self, record_id):
        for i, rec in enumerate(self.records):
            if rec is not None and rec.id == record_id:
                self.records[i] = None
                self.num_records -= 1
                return
        raise Exception("record to be removed must exist")

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

        padded_pointers = pad_bits(pointers, self.num_bytes_for_pointers).bytes
        diff = self.block_size - len(ba) - len(padded_pointers)
        return ba + bytearray(diff) + padded_pointers

    def has_space(self):
        return self.num_records < self.max_num_records 

    def __str__(self):
        return str(self.records)

    def __repr__(self):
        return str(self.records)
