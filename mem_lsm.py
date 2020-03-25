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

from storage import Storage
from SSTable import SSTable as SST
from core import ORG
import heapq as pq


class MemLSM():

    def __init__(my_storage: Storage, my_storage_type: ORG, num_of_memtbls):
        self.storage = my_storagee
        self.page_table = []
        self.page_table_ind = {}
        self.meta = {}
        self.num_of_memtbls = num_of_memtbls

    def retrieve_rec(self, table: str, rec_id: int, field: str, is_primary: bool):
        

    def write_rec(self, tbl_name: str, rec: tuple):
        if not tbl_name in self.meta:
            sst = SST()
            self.meta[tbl_name] = [sst]
        else:
            sst = self.meta[tbl_name][-1]
            
            

    def update_rec(self, tbl_name: str, rec: tuple):

    def key_index(self, key: int) -> int:
    """
    return: index
    """

    def search_page(self, hash_key):
    """
    search in page_table, and/or swap pages
    """  
