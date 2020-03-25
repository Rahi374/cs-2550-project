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

    def __init__(self, SS_per_LRU = 4, block_size = 64, blocks_per_SS = 4):
        self.SS_per_LRU = SS_per_LRU
        self.num_of_memtbls = 0
        
        self.storage = LSM(block_size, blocks_per_SS)  
        self.memtbls = {}
        self.page_table = {}

    def _bsearch(self, recs: list, key):
        if not recs:
            return -1 
            
        l = 0
        r = len(recs)
        while l < r:
            m = int(l + (r - l) / 2)
            m_key = abs(recs[m].id)
            # print(l, m, r, m_key)
            if m_key < key:
                l = m + 1
            elif m_key > key:
                r = m - 1
            else:
                return recs[m]
            
        return 0

    def write_rec(self, tbl_name: str, rec: Record):
        '''
        for both update and add record in LSM
        '''
        
        memtbl = None
        if tbl_name not in self.memtbls:
            #create a memtbl if necessary 
            memtbl = self.storage.build_memtable(tbl_name)
            self.memtbls[tbl_name] = memtbl
        else:
            memtbl = self.memtbls[tbl_name]

        
        #flush if necessary 
        # print('memtbl', memtbl)
        if memtbl.is_full():
            self.storage.push_memtable(memtbl)
            memtbl = self.storage.build_memtable(tbl_name)
            self.memtbls[tbl_name] = memtbl
    
        memtbl.add_record(rec)
    
    def _level_read_rec(self, level: list, rec_id):
        for i in range(len(level)):
            pass

    def _ba_2_recs(self, ba):
        res = []
        for i in range(8):
            rec = Record(ba = ba[i * 32: (i + 1) * 32])
        res.append(rec)
        return res

        
    def read_rec(self, tbl_name: str, rec_id):
        '''
        search for rec in memtbl (most recent), and then in page_table
        and search for it in disk it necessary
        '''
        #search in memtbl
        if tbl_name in self.memtbls:
            memtbl = self.memtbls[tbl_name]
            recs = memtbl.get_in_order_records()
            # print(recs)
            res = self._bsearch(recs, rec_id)
            if res:
                return res
        
        #search in LRU

        # #create LRU if necessary 
        # if tbl_name not in self.page_table:
        #     self.page_table[tbl_name] = [[], [], []] #L0, L1, L2
        # LRU = self.page_table[tbl_name]

        #search in storage
        rec = self.storage.get_record(rec_id, tbl_name)


        return -1

    def del_tbl(self, tbl_name:str):
        #TODO: implement
        pass    

    def read_recs(self, tbl_name: str, area: str):
        """
        search for rec in pagetable
        and search for it in disk it necessary
        # """ 

        # #iterate through LRU to find that stt, and records
        # recs = set()
        # pks = set() #store the existing primary keys to prevent duplicate reads
        # if tbl_name in self.page_table:
        #     LRU = self.page_table[tbl_name]
        #     for i in range(len(LRU) - 1, -1, -1):
        #         sst = LRU[i]
        #         tmp = set(sst.search_recs(area))
        #         for x in tmp:
        #             if x.id in pks:
        #                 tmp.remove(x)
        #             else:
        #                 pks.add(x.id)
        #         recs |= tmp


        #TODO: get the recs from disk
        return list(recs)
        
    


if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] == 'test':
        mem = MemLSM()

        #test write 
        mem.write_rec('tbl1', Record(123, 'test', '401-222-3142'))
        rec = mem.read_rec('tbl1', 123)
        print(rec)


        #9 tbl2 recs
        mem.write_rec('tbl2', Record(123, 'test', '123-222-3142'))
        mem.write_rec('tbl2', Record(111, 'test', '401-222-3142'))
        mem.write_rec('tbl2', Record(333, 'test', '401-222-3142'))
        mem.write_rec('tbl2', Record(13, 'eeee', '411-222-3142'))
        mem.write_rec('tbl2', Record(1, 'test', '123-222-3142'))
        mem.write_rec('tbl2', Record(2, 'test', '401-222-1111'))
        mem.write_rec('tbl2', Record(3, 'test', '999-222-3142'))
        mem.write_rec('tbl2', Record(122, 'test', '999-111-0000'))
        mem.write_rec('tbl2', Record(444, 'test', '999-111-4444'))
        ss = mem.memtbls['tbl2'].ss_table
        print('client: ', ss)


        #test read rec 
        mem.write_rec('tbl3', Record(123, 'test', '123-222-3142'))
        mem.write_rec('tbl3', Record(111, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(333, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(13, 'eeee', '411-222-3142'))
        mem.write_rec('tbl3', Record(1, 'test', '123-222-3142'))
        mem.write_rec('tbl3', Record(2, 'test', '401-222-1111'))
        mem.write_rec('tbl3', Record(3, 'test', '999-222-3142'))
        mem.write_rec('tbl3', Record(3, 'test2', '999-111-3142'))
        mem.write_rec('tbl3', Record(122, 'test', '999-111-0000'))

        mem.write_rec('tbl3', Record(10, 'test', '999-111-4444'))
        mem.write_rec('tbl3', Record(11, 'test', '123-222-3142'))
        mem.write_rec('tbl3', Record(12, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(13, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(4, 'eeee', '411-222-3142'))
        mem.write_rec('tbl3', Record(5, 'test', '123-222-3142'))
        mem.write_rec('tbl3', Record(13, 'test', '401-222-1111'))
        mem.write_rec('tbl3', Record(6, 'test', '999-222-3142'))
        
        print(mem.read_rec('tbl3', 3))
        



        
        


        # for x in mem.page_table.keys():
        #     print(x)
        #     for y in mem.page_table[x]:
        #         print(y)

        # print(mem.read_recs('tbl2', '999')) 
        # print(mem.read_recs('tbl1', '401'))

        # print(mem.read_rec('tbl1', 123))
        # print(mem.read_rec('tbl2', 3))
        
        

            
