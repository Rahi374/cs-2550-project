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
        exec_inst(inst)
        
    def read_id(self, table: str, rec_id: int):
      return

    def read_area_code(self, table: str, area_code: int):
      return

    def write(self, table: str, record: tuple):
      return
      
    def delete(self, table: str):
      return
    
    def create_table(self, table: str):
      return
      
    def table_exists(self, table: str):
      return
      
    def create_storage(self, disk_org: ORG, block_size: int):
      return
      

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
      self.tuple_data = tuple_data


# main.py
import sys
import 

import ACTION

def print_usage():
	print("python3 main.py <instruction_file> <disk_org> <mem_size> <block_size>")
    print("    instruction_file: instruction file")
    print("    disk_org: SEQ or LSM")
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
      	
		def retrieve_rec(self, table: str, rec_id: int, is_primary: bool):
    	
    def write_rec(self, table: str, rec: tuple):
      
    def update_rec(self, table: str, rec: tuple):
    
    def key_index(self, ):
      
    
    def search_page(self, hash_key):
    """
    search in page_table, and/or swap pages
    """  
