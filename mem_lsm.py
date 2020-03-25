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
import sys


class MemLSM():

    def __init__(self, my_storage: LSM, num_of_memtbls_allowed = 4):
        self.storage = my_storage
        self.page_table = {}
        self.num_of_memtbls_allowed = num_of_memtbls_allowed
        self.num_of_memtbls = 0
        #TODO: create storage   

    def write_rec(self, tbl_name: str, rec: Record):
        '''
        for both update and add record in LSM
        '''
        
        sst = None
        
        if not tbl_name in self.page_table:
            #add an LRU list + an sst for this table name
            self.check_flush()
            sst = SST() #TODO: specify size constraints
            sst.add(rec)
            self.page_table[tbl_name] = [sst]
            self.num_of_memtbls += 1
            
        else:
            LRU = self.page_table[tbl_name]
            for i in range(len(LRU)):
                sst = LRU[i]
                #write to an existing sst
                if not sst.isFull() and not sst.IMMUTABLE:
                    sst.add(rec)
                    return
                    
            #add new sst
            self.check_flush()
            sst = SST() #TODO: specify size constraints 
            sst.add(rec)       
            LRU.append(sst)
            self.num_of_memtbls += 1
            

    def check_flush(self):
        '''
        check if memory is full
        flush to L0 if TRUE
        '''
        if self.num_of_memtbls == self.num_of_memtbls_allowed:
            #TODO: flush to L0
            self.num_of_memtbls = 0
        
        
        
    def read_rec(self, tbl_name: str, rec: Record):
        """
        search for rec in pagetable
        and search for it in disk it necessary
        """ 

        #iterate through LRU to find that stt, and record
        if tbl_name in self.page_table:
            LRU = self.page_table[tbl_name]
            for i in range(len(LRU) - 1, -1, -1):
                sst = LRU[i]
                rec = sst.search_rec(rec.id)
                if rec:
                    return rec

        #TODO: get the recs from disks

    def read_recs(self, tbl_name: str, area: str):
        """
        search for rec in pagetable
        and search for it in disk it necessary
        """ 

        #iterate through LRU to find that stt, and records
        recs = set()
        pks = set() #store the existing primary keys to prevent duplicate reads
        if tbl_name in self.page_table:
            LRU = self.page_table[tbl_name]
            for i in range(len(LRU) - 1, -1, -1):
                sst = LRU[i]
                tmp = set(sst.search_recs(area))
                for x in tmp:
                    if x.id in pks:
                        tmp.remove(x)
                    else:
                        pks.add(x.id)
                recs |= tmp


        #TODO: get the recs from disk
        return list(recs)
        


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        mem = MemLSM(4)

        mem.write_rec('tbl1', Record(123, 'test', '401-222-3142'))
        mem.write_rec('tbl1', Record(123, 'test', '123-222-3142'))
        mem.write_rec('tbl2', Record(123, 'test', '123-222-3142'))
        mem.write_rec('tbl2', Record(111, 'test', '401-222-3142'))
        mem.write_rec('tbl2', Record(333, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(123, 'test', '999-222-3142'))
        mem.write_rec('tbl2', Record(1, 'test', '123-222-3142'))
        mem.write_rec('tbl2', Record(2, 'test', '401-222-3142'))
        mem.write_rec('tbl2', Record(3, 'test', '999-222-3142'))
        mem.write_rec('tbl2', Record(123, 'test', '999-111-0000'))

        mem.write_rec('tbl1', Record(101, 'test', '401-222-3142'))

        for x in mem.page_table.keys():
            print(x)
            for y in mem.page_table[x]:
                print(y)

        print(mem.read_recs('tbl2', '999')) 
        print(mem.read_recs('tbl1', '401'))
        
        

            
