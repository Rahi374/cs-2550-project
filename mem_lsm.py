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

    def __init__(self, SS_per_LRU = 4, block_size = 64, blocks_per_SS = 4, LRU_size = 3):
        self.SS_per_LRU = SS_per_LRU
        self.num_of_memtbls = 0
        
        self.storage = LSM(block_size, blocks_per_SS)  
        self.memtbls = {}
        self.page_table = {}
        self.LRU_size = LRU_size

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
                if recs[m].id < 0:
                    return -1
                # print('recs[m] ', recs[m])
                return recs[m]
            
        return 0

    def _get_area_recs(self, recs:list, area, pks:set, found:list):
        for r in recs:
            if r.id in pks:
                continue

            if int(r.phone[:3]) == int(area):
                pks.add(r.id)
                found.append(r)
            #print(found)

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
            rec = self._bsearch(self._ba_2_recs(level[i]), rec_id)

            # print('_level read: ', rec, '\n')
            if not type(rec) == int:
                #adjust the LRU sequence
                ba = level.pop(i)
                level.append(ba)
                return rec
        return -1

    def _level_read_recs(self, level: list, area, pks, found):
        for i in range(len(level)):
            self._get_area_recs(self._ba_2_recs(level[i]), area, pks, found)
        # print('level_read_recs: ', found)
            


    def _ba_2_recs(self, ba):
        res = []
        for i in range(8):
            rec = Record(ba = ba[i * 32: (i + 1) * 32])
            res.append(rec)
        return res

    def _check_evict(self, tbl_name, level, ba):
        l = self.page_table[tbl_name][level]
        #evict if LRU is full
        if len(l) == self.LRU_size:
            l.pop(0)
        #append to the end
        l.append(ba)
    
    def _check_evicts(self, tbl_name, bas):
        for i in range(len(bas)):
            for ba in bas[i]:
                self._check_evict(tbl_name, i, ba)

        
    def _print_pt(self):
        print()
        for x in self.page_table:
            print(x + ':')
            LRU = self.page_table[x]
            for i in range(len(LRU)):
                print('\tlevel ' + str(i) + ':')
                for ba in LRU[i]:
                    print('\t\t' + str(self._ba_2_recs(ba)))

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
            rec = self._bsearch(recs, rec_id)
            if rec:
                return rec
        
        #search in LRU
        LRU = None
        if tbl_name not in self.page_table:
            #create LRU if necessary 
            self.page_table[tbl_name] = [[], [], []] #L0, L1, L2
            LRU = self.page_table[tbl_name]
        else:
            LRU = self.page_table[tbl_name]
            re = self._level_read_rec(LRU[0], rec_id)
            if not type(rec) == int: 
                return rec
            rec = self._level_read_rec(LRU[1], rec_id)
            if not type(rec) == int: 
                return rec
            rec = self._level_read_rec(LRU[2], rec_id)
            if not type(rec) == int: 
                return rec

        #search in storage
        rec, ba, level = self.storage.get_record(rec_id, tbl_name)
        #update the page table
        self._check_evict(tbl_name, level, ba)
        return rec
    

    def del_tbl(self, tbl_name:str):
        #TODO: implement
        pass    

    def read_recs(self, tbl_name: str, area: str):
        """
        search for rec in pagetable
        and search for it in disk it necessary
        """ 
        found = []
        pks = set()
        #search in memtbl
        if tbl_name in self.memtbls:
            memtbl = self.memtbls[tbl_name]
            recs = memtbl.get_in_order_records()
            self._get_area_recs(recs, area, pks, found)

        #search in LRU
        if tbl_name not in self.page_table:
            #create LRU if necessary 
            self.page_table[tbl_name] = [[], [], []] #L0, L1, L2
            LRU = self.page_table[tbl_name]
        else:
            LRU = self.page_table[tbl_name]
            self._level_read_recs(LRU[0], area, pks, found)
            self._level_read_recs(LRU[1], area, pks, found)
            self._level_read_recs(LRU[2], area, pks, found)
        
        #search in Storage
        bas, found = self.storage.get_records(area, tbl_name, pks, found)
        
        #truncate the blocks returned from storage to fit the memory size
        bas[0] = bas[0][:self.LRU_size]
        bas[1] = bas[1][:self.LRU_size]
        bas[2] = bas[2][:self.LRU_size]
        #update the page table
        self._check_evicts(tbl_name, bas)
        
        return found
        
    


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
        mem.write_rec('tbl3', Record(3, 'test2', '999-111-3333'))
        mem.write_rec('tbl3', Record(122, 'test', '999-111-0000'))

        mem.write_rec('tbl3', Record(3, 'eeee', '101-101-1000'))
        mem.write_rec('tbl3', Record(3, 'eeee', '101-100-1000'))
        mem.write_rec('tbl3', Record(3, 'eeee', '999-110-1000'))
        mem.write_rec('tbl3', Record(3, 'eeee', '101-010-1000'))
        mem.write_rec('tbl3', Record(4, 'eeee', '101-110-1000'))
        mem.write_rec('tbl3', Record(4, 'eeee', '101-100-1000'))
        mem.write_rec('tbl3', Record(4, 'eeee', '101-110-1000'))
        mem.write_rec('tbl3', Record(4, 'eeee', '999-010-1000'))

        mem.write_rec('tbl3', Record(10, 'test', '999-111-4444'))
        mem.write_rec('tbl3', Record(11, 'test', '123-222-3142'))
        mem.write_rec('tbl3', Record(12, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(13, 'test', '401-222-3142'))
        mem.write_rec('tbl3', Record(4, 'eeee', '411-222-3142'))
        mem.write_rec('tbl3', Record(5, 'test', '123-222-3142'))
        mem.write_rec('tbl3', Record(13, 'test', '401-222-1111'))
        mem.write_rec('tbl3', Record(6, 'test', '999-222-3142'))
        
        
        print(mem.read_rec('tbl3', 3))
        mem._print_pt()
        print(mem.read_rec('tbl3', 123))

        #test read_recs
        print('tbl3 memtable: ', mem.memtbls['tbl3'].ss_table)
        #print('result: ', mem.read_recs('tbl3', 999))
        mem._print_pt()

        print("**\n\n**")
        print('result: ', mem.read_recs('tbl2', 401))
        mem._print_pt()
        
        
        

    



        
        
