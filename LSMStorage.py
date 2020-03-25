import os
import shutil
import threading
import time
import math
import record
from SSTable import *

class LSMStorage():
    blk_size = None
    blocks_per_ss = None
    metadata_counts = {}#number or SSTables in a level for a table
    metadata_ranges = {}#ranges of values in an SSTable
    L0_lock_hm = {}
    L1_lock_hm = {}
    L2_lock_hm = {}
    compaction_thread_hm = {}
    

    def __init__(self, block_size, blocks_per_SS):
        self.blk_size = block_size
        self.blocks_per_ss = blocks_per_SS
        #delete old directory if exists
        if os.path.isdir('storage'):
            try:
                shutil.rmtree('storage/')
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


        return


    def get_record(self, record_id, table_name):
        table_exists = self.check_if_table_exists(table_name)
        if not table_exists:
            return -1
        else:
            rec, ss = check_level_for_rec(record_id, table_name, "L0")
            if ss == -1:
                rec, ss = check_level_for_rec(record_id, table_name, "L1")
                if ss == -1:
                    rec, ss = check_level_for_rec(record_id, table_name, "L2")
                else:
                    return rec, ss
            else:
                return rec, ss
            return rec, ss
    def get_records(self, table_name, area_code):
        #TODO
        return

    def build_memtable(self, table_name):
        table_exists = self.check_if_table_exists(table_name)
        if not table_exists:
            self.metadata_counts[table_name+"L0"] = 0
            self.metadata_counts[table_name+"L1"] = 0
            self.metadata_counts[table_name+"L2"] = 0
            self.L0_lock_hm[table_name] = threading.Lock()
            self.L1_lock_hm[table_name] = threading.Lock()
            self.L2_lock_hm[table_name] = threading.Lock()
            self.create_table_structure(table_name)
        return MemTable(self.blk_size, self.blocks_per_ss ,table_name)

    def push_memtable(self, memtable):
        table_name = memtable.tbl_name
        self.L0_lock_hm[table_name].acquire()
        all_records = memtable.get_in_order_records()
        lower, upper = all_records[0], all_records[-1]
        self.write_records_to_level_SST(all_records, memtable.tbl_name, lower, upper, "L0")
        self.metadata_counts[table_name+"L0"] = metadata_counts[table_name+"L0"] + 1 
        self.L0_lock_hm[table_name].release()
        if self.metadata_counts[table_name+"L0"] == 4: #compact L0 if no more room
            if self.metadata_counts[table_name+"L1"] > 6:#if L1 would not be able to take L0, compact it
                L1_lock = self.L1_lock_hm[table_name]
                L2_lock = self.L2_lock_hm[table_name]
                L1_lock.acquire()
                L2_lock.acquire()
                self.compact_L1(table_name)
                L2_lock.release()
                L1_lock.release()

            L0_lock = self.L0_lock_hm[table_name]
            L1_lock = self.L1_lock_hm[table_name]
            L0_lock.acquire()
            L1_lock.acquire()
            self.compact_L0(table_name)
            L1_lock.release()
            L0_lock.release()



    def delete_table(self, table_name):
        try:
            shutil.rmtree('storage/'+table_name)
            os.rmdir('storage/'+table_name)
            l0_thread = compaction_thread_hm["L0"+table_name]
            l0_thread._stop()
            l1_thread = compaction_thread_hm["L1"+table_name]
            l1_thread._stop()
        except Exception as e:
            print(e)
            print("exception in deletion of table")
        return


    #private methods
    def check_if_table_exists(self, table_name):
        return os.path.isdir('storage/'+table_name)

    def create_table_structure(self, table_name):
        os.mkdir('storage/'+table_name)
        os.mkdir('storage/'+table_name+'/L0')
        os.mkdir('storage/'+table_name+'/L1')
        os.mkdir('storage/'+table_name+'/L2') 
        return

    def get_byte_array_of_records(self, records):
        byte_arr = bytearray(0)
        for r in records:
            byte_arr += r.to_bytearray()
        return byte_arr

    def write_records_to_level_SST(self, records, table_name, lower, upper, level):
        remove_duplicate_level_entries(records, table_name, level)
        records_per_block = blk_size / get_size_of_records()
        dir_path = "storage/"+table_name+"/"+level+"/SST"+metadata_counts[table_name+level]
        metadata_ranges[dir_path] = (lower, upper)
        os.mkdir(dir_path)
        records_to_write = len(records)
        c = 0 #file number
        i = 0 #record number
        while records_to_write > 0:
            if records_to_write < records_per_block:
                recs_to_write = records[i:i+records_to_write]
                f = open(dir_path+"/"+str(c)+"_"+str(records_to_write))
                f.write(get_byte_array(recs_to_write))
                f.close()
                records_to_write = 0
            else:
                recs_to_write = records[i:i+records_per_block]
                f = open(dir_path+"/"+str(c)+"_"+str(records_per_block))
                f.write(get_byte_array(recs_to_write))
                f.close()
                records_to_write -= records_per_block
                c += 1
                i += records_per_block

       
    def check_level_for_rec(self, record_id, table_name, level):
        dir_path = "storage/"+table_name+"/"+level
        ss_tables = os.listdir(dir_path)
        rec, ss = None, -1
        for s in ss_tables:
            lower, upper = metadata_ranges[dir_path+"/"+s]
            if lower < record_id and upper > record_id:
                rec, ss = check_sst_for_record(record_id, table_name, level, sst_num)
                if ss != -1:
                    return rec, ss
        return rec, ss

    def check_sst_for_record(self, record_id, table_name, level, sst_num):
        dir_path = "storage/"+table_name+"/"+level+"/SST"+sst_num
        blocks = os.listdir(dir_path)
        for b in blocks:
            f = open(dir_path+"/"+b, "rb")
            b_arr = bytearray(f.read())
            num_recs = b.split("_")[1]
            rec_num = 0
            for i in range(num_recs):
                start_of_rec = i*get_size_of_records()
                rec_id = int(b_arr[start_of_rec:start_of_rec+4])
                if rec_id == record_id:
                    rec_id = int(b_arr[start_of_rec:start_of_rec+4])
                    rec_name = str(b_arr[start_of_rec+4:start_of_rec+20])
                    rec_phone = str(b_arr[start_of_rec+20:start_of_rec+32])
                    return Record(rec_id, rec_name, rec_phone), b_arr

            f.close()
        return None, -1


    def remove_duplicate_level_entries(records, table_name, level):
        rec_hm = {}
        for r in records:
            rec_hm[r.id] = r
        dir_path = "storage/"+table_name+"/"+level+"/"
        ss_tables_in_L0 = os.listdir(dir_path)
        for sst in ss_tables_in_L0:
            remove_duplicates_from_ss_table(sst, rec_hm, "/"+level+"/")

    def remove_duplicates_from_ss_table(sst, rec_hm, level):
        dir_path = "storage/"+table_name+level+sst
        blocks_in_sst = os.listdir(dir_path)
        for b in blocks_in_sst:
            remove_duplicates_from_block(b, rec_hm)

    def remove_duplicates_from_block(b, rec_hm, level):
        num_recs = b.split("_")[1]
        dir_path = "storage/"+table_name+level+sst+"/"+b
        f = open(dir_path, "w+b")
        b_arr = bytearray(f.read())
        rec_num = 0
        for r in range(num_recs):
            start_of_rec = rec_num * get_size_of_records() 
            rec_id = int(b_arr[start_of_rec:start_of_rec+4])
            if rec_hm[rec_id] != None:
                b_arr[start_of_rec:start_of_rec+4] = bytearray([(-2 >> shift) & 0xff for shift in [0, 8, 16, 24]])
            rec_num += 1
        f.write(b_arr)
        f.close()
        

    def start_compaction_threads(self, table_name):
        L0_comp_thread = threading.Thread(target=start_L0Compaction, args=(table_name,))
        L1_comp_thread = threading.Thread(target=start_L1Compaction, args=(table_name,))
        compaction_thread_hm["L0"+table_name] = L0_comp_thread
        compaction_thread_hm["L1"+table_name] = L1_comp_thread
        L0_comp_thread.start()
        L1_comp_thread.start()

    def start_L0Compaction(self, table_name):
        L0_lock = L0_lock_hm[table_name]
        L1_lock = L1_lock_hm[table_name]
        while True:
            L0_lock.acquire()
            L1_lock.acquire()
            compact_L0(table_name)
            L1_lock.release()
            L0_lock.release()
            time.sleep(5)

    def start_L1Compaction(self,table_name):
        L1_lock = L1_lock_hm[table_name]
        L2_lock = L2_lock_hm[table_name]
        while True:
            L1_lock.acquire()
            L2_lock.acquire()
            compact_L1(table_name)
            L2_lock.release()
            L1_lock.release()
            time.sleep(10)


    def compact_L0(table_name):
        if metadata_counts[table_name+"L1"] > 6:#if L1 would not be able to take L0, compact it
            L1_lock = L1_lock_hm[table_name]
            L2_lock = L2_lock_hm[table_name]
            L1_lock.acquire()
            L2_lock.acquire()
            compact_L1(table_name)
            L2_lock.release()
            L1_lock.release()
        list_of_records = []
        dir_of_L0 = "storage/"+table_name+"/L0"
        for sst in os.listdir(dir_of_L0):
            for b in os.listdir(dir_of_L0+"/"+sst):
                f = open(dir_of_L0+"/"+sst+"/"+b, "w+b")
                b_arr = bytearray(f.read())
                num_recs = b.split("_")[1]            
                rec_num = 0
                for r in range(num_recs):
                    start_of_rec = rec_num * get_size_of_records() 
                    rec_id = int(b_arr[start_of_rec:start_of_rec+4])
                    rec_name = str(b_arr[start_of_rec+4:start_of_rec+20])
                    rec_phone = str(b_arr[start_of_rec+20:start_of_rec+32])
                    list_of_records.append(Record(rec_id, rec_name, rec_phone))
                    rec_num += 1
                f.close()
                os.remove(dir_of_L0+"/"+sst+"/"+b)

            os.remove(dir_of_L0+"/"+sst)
        list_of_records = [r for r in list_of_records if r.id != -2]
        list_of_records.sort(key=lambda x: x.id)
        write_L0_records_to_L1(list_of_records, table_name)

        metadata_counts[table_name+"L0"] = 0
        return


    def write_L0_records_to_L1(recs, table_name):
        remove_duplicate_level_entries(recs, table_name, "L1")
        write_records_to_level_SST(recs, table_name, "L1")
        

    def compact_L1(table_name):
        list_of_records = []
        dir_of_L1 = "storage/"+table_name+"/L1"
        for sst in os.listdir(dir_of_L1):
            for b in os.listdir(dir_of_L1+"/"+sst):
                f = open(dir_of_L1+"/"+sst+"/"+b, "w+b")
                b_arr = bytearray(f.read())
                num_recs = b.split("_")[1]            
                rec_num = 0
                for r in range(num_recs):
                    start_of_rec = rec_num * get_size_of_records() 
                    rec_id = int(b_arr[start_of_rec:start_of_rec+4])
                    rec_name = str(b_arr[start_of_rec+4:start_of_rec+20])
                    rec_phone = str(b_arr[start_of_rec+20:start_of_rec+32])
                    list_of_records.append(Record(rec_id, rec_name, rec_phone))
                    rec_num += 1
                f.close()
                os.remove(dir_of_L1+"/"+sst+"/"+b)

            os.remove(dir_of_L1+"/"+sst)
        list_of_records = [r for r in list_of_records if r.id != -2]
        list_of_records.sort(key=lambda x: x.id)
        write_L1_records_to_L2(list_of_records, table_name)

        metadata_counts[table_name+"L1"] = 0
        return
    

        def write_L1_records_to_L2(recs, table_name):
            remove_duplicate_level_entries(recs, table_name, "L2")
            list_of_records = []
            dir_of_L2 = "storage/"+table_name+"/L2"
            for sst in os.listdir(dir_of_L2):
                for b in os.listdir(dir_of_L2+"/"+sst):
                    f = open(dir_of_L2+"/"+sst+"/"+b, "w+b")
                    b_arr = bytearray(f.read())
                    num_recs = b.split("_")[1]            
                    rec_num = 0
                    for r in range(num_recs):
                        start_of_rec = rec_num * get_size_of_records() 
                        rec_id = int(b_arr[start_of_rec:start_of_rec+4])
                        rec_name = str(b_arr[start_of_rec+4:start_of_rec+20])
                        rec_phone = str(b_arr[start_of_rec+20:start_of_rec+32])
                        list_of_records.append(Record(rec_id, rec_name, rec_phone))
                        rec_num += 1
                    f.close()
                    os.remove(dir_of_L2+"/"+sst+"/"+b)

                os.remove(dir_of_L2+"/"+sst)
            list_of_records = [r for r in list_of_records if r.id != -2]
            for rec in recs:
                list_of_records.append(rec)
            list_of_records.sort(key=lambda x: x.id)
            write_records_to_level_SST(list_of_records, table_name, list_of_records[0].id, list_of_records[-1].id, "L2")
            return


def get_size_of_records():
    return 32

class MemTable(object):
    blk_size = None
    tbl_name = None
    blocks_per_ss = None
    lower_bound = None
    upper_bound = None
    max_records = None
    num_records = 0
    ss_table = SSTable()

    def __init__(self, block_size, blocks_per_SS, table_name):
        self.blk_size = block_size
        self.tbl_name = table_name
        self.blocks_per_ss = blocks_per_SS
        self.max_records = self.blocks_per_ss * math.floor(self.blk_size / get_size_of_records())
        print("mem table block size: "+str(block_size))
        print("mem table blocks per ss: "+str(blocks_per_SS))
        print("mem table max number of records: "+str(self.max_records))


    def get_range():
        return lower_bound, upper_bound

    def add_record(self, record):
        self.ss_table.insert(record.id, record)


    def delete_record(self, record_id):
        #make new record and set the name to -1 to indicate it is a delete node
        deleted_record = Record(record_id,"-1","-1")
        self.ss_table.insert(record_id, deleted_record)
        self.num_records = len(AVL_Tree.getInOrder())
        return

    def get_in_order_records(self):
        return ss_table.getInOrderRecords()

    def is_full():
        return ss_table.num_of_record == max_records





