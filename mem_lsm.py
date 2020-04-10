# Mem (exposed to core):
# constructor(storage storage)
# retrieve_record(string table, int id)
# retrieve_record(string table, int area_code) - some sort of switch
# write_rec(self, tbl_name: str, rec: Record = None, is_del = False, rec_id = None):


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
from logger import Logger 
import common
import sys
import math

class MemLSM():

    def __init__(self, storage, block_size = 64, LRU_size = 3):
        self.num_of_memtbls = 0
        
        self.storage = storage
        self.memtbls = {}
        self.page_table = {}
        self.LRU_size = LRU_size

    # returns 0 for not found, -1 for deleted, or Record for found
    def _bsearch(self, recs: list, key):
        if not recs:
            return -1 
        key = int(key)
        l = 0
        r = len(recs)
        m = int(l + (r - l) / 2)
        # print(recs)
        while l <= r and m < len(recs) and m >= 0:
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
            m = int(l + (r - l) / 2)
            
        return 0

    def _get_area_recs(self, recs: list, area: str, pks: set, found: list):
        for r in recs:
            if r.id in pks:
                continue

            if r.id < 0:
                pks.add(abs(r.id))
                continue
            
            if int(r.phone[:3]) == int(area):
                pks.add(r.id)
                found.append(r)
            #print(found)

    def delete_record(self, tbl_name: str, rec_id):
        self.write_rec(tbl_name = tbl_name, rec = None, is_del = True, rec_id = rec_id)

    def write_rec(self, tbl_name: str, rec: Record = None, is_del = False, rec_id = None):
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
    
        if is_del:
            memtbl.delete_record(rec_id)
        else:
            memtbl.add_record(rec)

    
    def _level_read_rec(self, level: list, rec_id):
        rec = None
        for i in range(len(level)):
            rec = self._bsearch(self._ba_2_recs(level[i]), rec_id)

            # print('_level read: ', rec, '\n')
            if type(rec) == Record and int(rec.phone[:2]) >= 0:
                #adjust the LRU sequence
                ba = level.pop(i)
                level.append(ba)
                return rec
        return rec

    def _level_read_recs(self, level: list, area: str, pks, found):
        for i in range(len(level)):
            self._get_area_recs(self._ba_2_recs(level[i]), area, pks, found)
        # print('level_read_recs: ', found)
            
    def _ba_2_recs(self, ba):
        res = []
        if ba == None:
            return None
        for i in range(math.floor(len(ba) / common.RECORD_SIZE)):
            rec = Record(ba = ba[i * common.RECORD_SIZE: (i + 1) * common.RECORD_SIZE])
            res.append(rec)
        return res

    def _check_evict(self, tbl_name, level, ba):
        l = self.page_table[tbl_name][level]
        out = None
        #evict if LRU is full
        if l is not None and len(l) == self.LRU_size:
            out = l.pop(0)
            
        #append to the end
        l.append(ba)

        if out == None:
            return
        ba = self._ba_2_recs(ba)
        out = self._ba_2_recs(out)
        #Logging
        if type(ba[0]) == Record and type(ba[-1]) == Record:
            Logger.log(f'SWAP IN L-{level} {tbl_name}{abs(ba[0].id)}-{tbl_name}{abs(ba[-1].id)}')
        if type(out[0]) == Record and type(out[-1]) == Record:
            Logger.log(f'SWAP OUT L-{level} {tbl_name}{abs(out[0].id)}-{tbl_name}{abs(out[-1].id)}')
    
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
            rec = self._bsearch(recs, int(rec_id))
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
            rec = self._level_read_rec(LRU[0], rec_id)
            if (isinstance(rec, int) and rec == -1) or isinstance(rec, Record):
                return rec
            rec = self._level_read_rec(LRU[1], rec_id)
            if (isinstance(rec, int) and rec == -1) or isinstance(rec, Record):
                return rec
            rec = self._level_read_rec(LRU[2], rec_id)
            if (isinstance(rec, int) and rec == -1) or isinstance(rec, Record):
                return rec

        #search in storage
        res = self.storage.get_record(rec_id, tbl_name)
        if res == -1:
            return None

        rec, ba, level = res

        #check if not found or deleted
        if ba == -1 or int(rec.phone[:2]) < 0 :
            return None
        #update the page table
        self._check_evict(tbl_name, level, ba)

        return rec

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
        res = self.storage.get_records(area, tbl_name, pks, found)
        if res == -1:
            return res
            
        bas, found = res
        
        #truncate the blocks returned from storage to fit the memory size
        bas[0] = bas[0][:self.LRU_size]
        bas[1] = bas[1][:self.LRU_size]
        bas[2] = bas[2][:self.LRU_size]
        #update the page table
        self._check_evicts(tbl_name, bas)
        

        return found

    def delete_table(self, tbl_name: str):
        # remove from memtbl if exists
        self.memtbls.pop(tbl_name, None)
        # remove from LRU if exists
        self.page_table.pop(tbl_name, None)
        # remove from storage - core already calls storage::remove_table()


    def flush(self):
        for memtable in self.memtbls.values():
            self.storage.push_memtable(memtable)

    def print_cache(self):
        for memtable in self.memtbls.values():
            print(memtable.ss_table)
        self._print_pt()

if __name__ == '__main__':
    #test delete record
    mem = MemLSM(LSM(64, 3))
    mem.write_rec('tbl1', Record(1, 'name1', '111-222-3333'))
    mem.write_rec('tbl1', Record(2, 'name2', '111-222-3333'))
    mem.write_rec('tbl1', Record(3, 'name3', '111-222-3333'))
    mem.write_rec('tbl1', Record(4, 'name1', '111-222-3333'))
    mem.write_rec('tbl1', Record(5, 'name1', '222-222-3333'))
    mem.write_rec('tbl1', Record(6, 'name1', '444-222-3333'))
    mem.write_rec('tbl1', Record(17, 'name1', '333-222-3333'))
    mem.write_rec('tbl1', Record(7, 'name1', '333-222-3333'))
    mem.write_rec('tbl1', Record(8, 'name1', '999-222-3333'))

    print('result', mem.read_recs('tbl1', 333))
    mem.delete_record('tbl1', 7)
    
    mem.write_rec('tbl1', Record(11, 'name1', '111-222-3333'))
    mem.write_rec('tbl1', Record(12, 'name2', '111-222-3333'))
    mem.write_rec('tbl1', Record(13, 'name3', '111-222-3333'))
    mem.write_rec('tbl1', Record(14, 'name1', '111-222-3333'))
    mem.write_rec('tbl1', Record(15, 'name1', '222-222-3333'))
    mem.write_rec('tbl1', Record(16, 'name1', '444-222-3333'))
    mem.write_rec('tbl1', Record(17, 'name1', '333-222-3333'))
    mem.write_rec('tbl1', Record(18, 'name1', '999-222-3333'))


    mem.write_rec('tbl2', Record(1, 'name1', '111-222-3333'))
    mem.write_rec('tbl2', Record(2, 'name2', '111-222-3333'))
    mem.write_rec('tbl2', Record(3, 'name3', '111-222-3333'))
    mem.write_rec('tbl2', Record(4, 'name1', '111-222-3333'))
    mem.write_rec('tbl2', Record(5, 'name1', '222-222-3333'))
    mem.write_rec('tbl2', Record(6, 'name1', '444-222-3333'))
    mem.write_rec('tbl2', Record(7, 'name1', '333-222-3333'))
    mem.write_rec('tbl2', Record(8, 'name1', '999-222-3333'))
    mem.write_rec('tbl2', Record(5, 'name1', '999-222-3333'))

    print(mem.memtbls['tbl1'].ss_table)
    print(mem.read_rec('tbl1', 2))
    print(mem.read_rec('tbl1', 1))
    mem._print_pt()
    # mem.delete_record('tbl2', 4)
    # mem._print_pt()
    # print(mem.read_rec('tbl2', 4))


    # print(mem.memtbls['tbl1'].ss_table)
    # print(mem.read_rec('tbl1', 7))
    # print('result', mem.read_recs('tbl1', 333))


    

    

