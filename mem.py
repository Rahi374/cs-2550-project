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
