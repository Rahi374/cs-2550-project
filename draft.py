# core.py

from enum import Enum
class ORG(Enum):
    SEQ = 1
    LSM = 2

class ACTION(Enum):
    RETRIEVE_BY_ID = 1
    RETRIEVE_BY_AREA_CODE = 2
    WRITE_RECORD = 3
    DELETE_TABLE = 4

class Core():
    mem = None
    disk = None

    def __init__(self, disk_org: ORG, mem_size: int, block_size: int):
        disk = Storage(block_size, disk_org)
        mem = Mem(disk, disk_org)

    def run(self, insts: list):
        for inst in insts:
            self.exec_inst(inst)

    def exec_inst(self, inst: Instruction):
        if inst.action == ACTION.RETRIEVE_BY_ID:
            self.read_id(inst.table_name, inst.record_id)

        elif inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            self.read_area_code(inst.table_name, inst.record_id)

        elif inst.action == ACTION.WRITE_RECORD:
            self.write(inst.table_name, inst.tuple_data)

        elif inst.action == ACTION.DELETE_TABLE:
            self.delete(inst.table_name)

    def read_id(self, table: str, rec_id: int):
        return mem.retrieve_rec(table=table, rec_id=rec_id, field="id", is_primary=True)

    def read_area_code(self, table: str, area_code: int):
        return mem.retrieve_rec(table=table, rec_id=area_code, field="area_code", is_primary=False)

    def write(self, table: str, record: tuple):
        if not self.table_exists(table):
            self.create_table(table)
        mem.write_rec(table, record)

    def delete(self, table: str):
        disk.delete_table(table)

    def create_table(self, table: str):
        disk.create_table(table)

    def table_exists(self, table: str):
        disk.table_exists(table)


# inst.py
import ACTION

class Instruction():
    action = None
    table_name = None
    tuple_data = None
    record_id = None

    def __init__(self, action: ACTION, table_name: str, data=None):
    """
    data: either tuple_data or record_id
        - tuple_data: (int id, string client_name, string phone)
        - record_id: int id
    """
        self.action = action
        self.table_name = table_name

        if self.action == ACTION.RETRIEVE_BY_ID or self.action == ACTION.RETRIEVE_BY_AREA_CODE:
            self.record_id = data
        elif self.action == ACTION.WRITE_RECORD:
            self.tuple_data = data


# main.py
import sys
import 

import ACTION

def print_usage():
    print("python3 main.py <instruction_file> <disk_org> <mem_size> <block_size>")
    print("    instruction_file: instruction file")
    print("    disk_org: SEQ|LSM")
    print("    mem_size: size of memory (in bytes)")
    print("    block_size: size of disk blocks (in bytes)")

if len(sys.argv) != 5:
    print_usage()
    sys.exit()

instr_file = sys.argv[1]
disk_org = (ORG.SEQ if sys.argv[2] == "SEQ" else ORG.LSM)
mem_size = sys.argv[3]
block_size = sys.argv[4]

#insts = parser.parse(instr_file)
insts = [
        Instruction(ACTION.RETRIEVE_BY_ID,        "X", 13),
        Instruction(ACTION.RETRIEVE_BY_ID,        "Y", 7),
        Instruction(ACTION.WRITE_RECORD,          "Y", (5, "John", "412-111-2222")),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "Y", 609),
        Instruction(ACTION.WRITE_RECORD,          "X", (2, "Thalia", "412-656-2212")),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "X", 412),
        Instruction(ACTION.DELETE_TABLE,          "Y"),
        ]

core = Core(disk_org, mem_size, block_size)
core.run(insts)


# Mem (exposed to core):
# constructor(storage storage)
# retrieve_record(string table, int id)
# retrieve_record(string table, int area_code) - some sort of switch
# write_record(string table, tuple record)
# update_record(string table, tuple record)

# Mem (common, private):
# blocks[byte[block_size]]
# max_num_of_blocks
# SequentialMem (private):
# key_index (dict: id -> block_id)
# non_key_index (dict: area_code -> list[block_id])


# mem.py
from storage import Storage
from core import ORG
import heapq as pq

class Mem():
    storage = None
    storage_type = None
    page_table = None
    hash_table = None 



    def __init__(my_storage: Storage, my_storage_type: ORG):
        this.storage = my_storage
        this.storage_type = my_storage_type
        this.page_table = []
        hash_table = collections.defaultdict(int)

    def retrieve_rec(self, table: str, rec_id: int, field: str, is_primary: bool):
        if storage_type == ORG.SEQ:

        else:

    def write_rec(self, table: str, rec: tuple):

    def update_rec(self, table: str, rec: tuple):

    def key_index(self, key: int) -> int:
    """
    return: index
    """

    def search_page(self, hash_key):
    """
    search in page_table, and/or swap pages
    """  


#storage.py  
class Slotted_Page():
    def __init__():













import ACTION  
import sys
class Parser():
    @staticmethod
    def parse(inst_file):
        instructions = []
        f = open(inst_file, 'r')
        lines = f.readlines()
        line_num = 1
        for line in lines:
            split_line - line.split()
            action = split_line[0]
            table_name = split_line[1]
            data = split_line[2]
            inst = None
            if action == 'R':
                inst = Instruction(ACTION.RETRIEVE_BY_ID, table_name, data)
            elif action == 'M':
                inst = Instruction(ACTION.RETRIEVE_BY_AREA_CODE, table_name, data)
            elif action == 'W':
                inst = Instruction(ACTION.WRITE_RECORD, table_name, data)
            elif action == 'D':
                inst = Instruction(ACTION.DELETE_TABLE, table_name, data)
            else:
                print("Malformed Input File on Line "+line_num+": "+line)
              sys.exit()
            instructions.append(inst)
            line_num += 1
        return instructions
