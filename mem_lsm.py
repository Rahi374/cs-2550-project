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

from LSMStorage import LSMStorage as LSM
from SSTable import SSTable as SST
from record import Record
from core import ORG


class MemLSM():

    def __init__(my_storage: Storage, my_storage_type: ORG, num_of_memtbls):
        self.storage = my_storagee
        self.page_table = {}
        self.num_of_memtbls = num_of_memtbls
        
        #TODO: create storage   

    def write_rec(self, tbl_name: str, rec: Record):
        '''
        for both update and add record in LSM
        '''
        
        sst = None
        
        if not tbl_name in self.page_table:
            #add an LRU list + an sst for this table name
            sst = SST() #TODO: specify size constraints
            check_flush()
            self.page_table[tbl_name] = [sst]
        else:
            LRU = self.page_table[tbl_name]
            for i in range(LRU):
                sst = LRU[i]
                #write to an exisiting sst
                if not sst.isFull() and not sst.IMMUTABLE:
                    sst.add(rec)
                    

            #add new sst
            check_flush()
            sst = SSTable() #TODO: specify size constraints        
            LRU.insert(0, sst)

    def check_flush():
        '''
        check if memory is full
        flush to L0 if TRUE
        '''
        
        
    def read_rec(self, tbl_name: str, rec: Record):
        """
        search sst in pagetable, evict if necessary 
        """ 

        #iterate through LRU to find that stt, and record
        if tbl_name in self.page_table:
            LRU = self.page_table[tbl_name]
            for i in range(LRU):
                sst = LRU[i]
                rec = sst.search_rec(rec.id)
                if rec:
                    #update the LRU order
                    LRU.pop(i)
                    LRU.append(sst)
                    return rec

        #TODO: get the SSTable from memory
        
        

                

            
