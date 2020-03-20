import os
import shutil
import threading

class LSMStorage():
    L0_lock = None
    L1_lock = None
    L2_lock = None

    def __init__(self, block_size):
        
        #delete old directory if exists
        if os.path.isdir('strorage'):
            try:
                shutil.rmtree('storage')
                os.rmdir('storage')
            except Exception as e:
                print(e)
                print("exception in deletion of old storage")

        #make storage directory
        try:
            os.mkdir('storage')
        except Exception as e:
            print(e)
            print("exception in mkdir")

        os.chmod('storage', 0o777)

        init_locks()
        start_compaction_threads()

        return

    def add_record(self, Record: rec):
        table_exists = check_if_table_exists(record.table_name)
        if not table_exists:
            os.mkdir('storage/'+rec.table_name)
        ##wait, actually we shouldnt ever add records inside storage, just the memtable in memory

    def get_record(self, record_id, table_name):
        table_exists = check_if_table_exists(table_name)
        if not table_exists:
            return -1
        else:
            rec, block = check_L0(record_id, table_name)
            if block == -1:
                rec, block = check_L1(record_id, table_name)
                if block == -1:
                    rec, block = check_L2(rec)
                
            else:
                return rec, block
        
        

    def get_records(self, area_code):
        return

    def build_memtable(self, block_size, table_name):
        table_exists = check_if_table_exists(table_name)
        if not table_not_exists:
            create_table_structure(table_name)
        return MemTable(block_size, table_name)

    def push_memtable(self, memtable):
        return

    def delete_table(self, table_name):
        try:
            shutil.rmtree('storage/'+table_name)
            os.rmdir('storage/'+table_name)
        except Exception as e:
            print(e)
            print("exception in deletion of table")
        return


    #private methods
    def check_if_table_exists(self, table_name):
        return os.path.isdir('storage/'+table_name)

    def create_table_structure(self, table_name):
        os.makedir('storage/'+table_name)
        os.makedir('storage/'+table_name+'/L0')
        os.makedir('storage/'+table_name+'/L1')
        os.makedir('storage/'+table_name+'/L2')
        
        return #TODO

    def start_compaction_threads(self):
        start_L0Compaction()
        start_L1Compaction()

    def start_L0Compaction(self):
        return #TODO

    def start_L1Compaction(self):
        return #TODO

    def init_locks(self):
        L0_lock = threading.Lock()
        L1_lock = threading.Lock()
        L2_lock = threading.Lock()

class MemTable(object):
    blk_size = None
    tbl_name = None
    lower_bound = None
    upper_bound = None
    max_records = None

    #TODO need 1 block of data to put records in (sstable)

    def __init__(self, block_size, table_name):
        blk_size = block_size
        tbl_name = table_name
        #need to calculate max records per memtable
        #TODO

    def add_record(self, record):
        #TODO add record to the block of storage (sstable)
        if lower_bound is None:
            lower_bound = record.id
            upper_bound = record.id
        else if record.id < lower_bound:
            lower_bound = record.id
        else if record.id > upper_bound:
            upper_bound = record.id
        




