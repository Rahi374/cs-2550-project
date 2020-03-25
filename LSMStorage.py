import os
import shutil
import threading
import time
import math
from record import *
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
            print("no table")
            return -1
        else:
            rec, ss = self.check_level_for_rec(record_id, table_name, "L0")
            if ss == -1:
                rec, ss = self.check_level_for_rec(record_id, table_name, "L1")
                if ss == -1:
                    rec, ss = self.check_level_for_rec(record_id, table_name, "L2")
                else:
                    return rec, ss
            else:
                return rec, ss
            return rec, ss

    def get_records(self, area_code, table_name, hm_keys_found):
        list_of_bytearrays = []
        get_matching_area_code_byte_arrays(list_of_bytearrays, table_name, area_code, "L0")
        get_matching_area_code_byte_arrays(list_of_bytearrays, table_name, area_code, "L1")
        get_matching_area_code_byte_arrays(list_of_bytearrays, table_name, area_code, "L2")
        return list_of_bytearrays

    def get_matching_area_code_byte_arrays(self, list_of_bytearrays, table_name, area_code, level):
        level_dir = "storage/"+level
        for sst in os.listdir(level_dir):
            sst_dir = level_dir + "/" + sst
            if self.sst_contains_record_matching_area_code(sst_dir, area_code):
                list_of_bytearrays.append(self.get_sst_as_b_arr(table_name, level, sst))

    def sst_contains_record_matching_area_code(self, sst_dir, area_code):
        blocks = os.listdir(sst_dir)
        for b in blocks:
            num_recs = int(b.split("_")[1])
            f = open(sst_dir+"/"+b, "rb")
            b_arr = bytearray(f.read())
            for i in range(num_recs):
                start_of_rec = i * get_size_of_records()
                rec_phone_num = b_arr[start_of_rec+20:start_of_rec+32].decode()
                rec_area_code = phone_num.split("-")[0]
                if rec_area_code = area_code:
                    f.close()
                    return True
            f.close()

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
            self.start_compaction_threads(table_name) #TODO re-enable this
        return MemTable(self.blk_size, self.blocks_per_ss ,table_name)

    def push_memtable(self, memtable):
        print("in order records being pushed from memtable")
        print(memtable.get_in_order_records())
        table_name = memtable.tbl_name
        self.L0_lock_hm[table_name].acquire()
        all_records = memtable.get_in_order_records()
        lower, upper = all_records[0], all_records[-1]
        self.write_records_to_level_SST(all_records, memtable.tbl_name, lower, upper, "L0")
        self.metadata_counts[table_name+"L0"] = self.metadata_counts[table_name+"L0"] + 1 
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
        if len(records) == 0:
            return
        self.remove_duplicate_level_entries(records, table_name, level)
        records_per_block = math.floor(self.blk_size / get_size_of_records())
        dir_path = "storage/"+table_name+"/"+level+"/SST"+str(self.metadata_counts[table_name+level])
        self.metadata_counts[table_name+level] = self.metadata_counts[table_name+level] + 1
        self.metadata_ranges[dir_path] = (lower, upper)
        os.mkdir(dir_path)
        records_to_write = len(records)
        c = 0 #file number
        i = 0 #record number
        while records_to_write > 0:
            if records_to_write < records_per_block:
                recs_to_write = records[i:i+records_to_write]
                print("writing records partial:")
                print(recs_to_write)
                f = open(dir_path+"/"+str(c)+"_"+str(records_to_write), "wb+")
                f.write(self.get_byte_array_of_records(recs_to_write))
                f.close()
                records_to_write = 0
            else:
                recs_to_write = records[i:i+records_per_block]
                #print("writing records in level sst:")
                #print(recs_to_write)
                #print("\n\n\n")
                #print(self.get_byte_array_of_records(recs_to_write))
                f = open(dir_path+"/"+str(c)+"_"+str(records_per_block), "wb+")
                f.write(self.get_byte_array_of_records(recs_to_write))
                f.close()
                records_to_write -= records_per_block
                c += 1
                i += records_per_block

       
    def check_level_for_rec(self, record_id, table_name, level):
        dir_path = "storage/"+table_name+"/"+level
        ss_tables = os.listdir(dir_path)
        rec, ss = None, -1
        for s in ss_tables:
            lower, upper = self.metadata_ranges[dir_path+"/"+s]
            print("lower: "+str(lower))
            print("upper: "+str(upper))
            if lower <= record_id and upper >= record_id:
                rec, ss = self.check_sst_for_record(record_id, table_name, level, s)
                if ss != -1:
                    return rec, ss
        return rec, ss

    def check_sst_for_record(self, record_id, table_name, level, sst):
        dir_path = "storage/"+table_name+"/"+level+"/"+sst
        blocks = os.listdir(dir_path)
        for b in blocks:
            f = open(dir_path+"/"+b, "rb")
            b_arr = bytearray(f.read())
            num_recs = int(b.split("_")[1])
            rec_num = 0
            for i in range(num_recs):
                start_of_rec = i*get_size_of_records()
                rec_id = int.from_bytes(b_arr[start_of_rec:start_of_rec+4], byteorder="little", signed=True)
                if rec_id == record_id:
                    rec_name = b_arr[start_of_rec+4:start_of_rec+20].decode()
                    rec_phone = b_arr[start_of_rec+20:start_of_rec+32].decode()
                    f.close()
                    return Record(rec_id, rec_name, rec_phone), self.get_sst_as_b_arr(table_name, level, sst)

            f.close()
        return None, -1

    def get_sst_as_b_arr(self, table_name, level, sst):
        b_arr = bytearray()
        sst_dir = "storage/"+table_name+"/"+level+"/"+sst
        dirs = os.listdir(sst_dir)
        dirs.sort()
        for b in dirs:
            f = open(sst_dir+"/"+b, "rb")
            b_arr += bytearray(f.read())
            f.close()
        return b_arr

    def remove_duplicate_level_entries(self, records, table_name, level):
        rec_hm = {}
        for r in records:
            rec_hm[r.id] = r
        dir_path = "storage/"+table_name+"/"+level+"/"
        ss_tables_in_L0 = os.listdir(dir_path)
        for sst in ss_tables_in_L0:
            self.remove_duplicates_from_ss_table(sst, rec_hm, table_name,"/"+level+"/")

    def remove_duplicates_from_ss_table(self, sst, rec_hm, table_name, level):
        dir_path = "storage/"+table_name+level+sst
        blocks_in_sst = os.listdir(dir_path)
        for b in blocks_in_sst:
            self.remove_duplicates_from_block(b, rec_hm, table_name, sst,level)

    def remove_duplicates_from_block(self, b, rec_hm, table_name, sst,level):
        num_recs = int(b.split("_")[1])
        dir_path = "storage/"+table_name+level+sst+"/"+b
        #time.sleep(20)
        f = open(dir_path, "rb")
        b_arr = bytearray(f.read())
        for r in range(num_recs):
            start_of_rec = r * get_size_of_records() 
            rec_id = int.from_bytes(b_arr[start_of_rec:start_of_rec+4], byteorder="little", signed=True)
            if rec_id in rec_hm:
                b_arr = b_arr[0:start_of_rec] + Record(-2,"-1","-1").to_bytearray() + b_arr[start_of_rec+32:]
        f.close()
        f = open(dir_path, "wb+")
        f.write(b_arr)
        f.close()
        

    def start_compaction_threads(self, table_name):
        L0_comp_thread = threading.Thread(target=self.start_L0Compaction, args=(table_name,))
        L1_comp_thread = threading.Thread(target=self.start_L1Compaction, args=(table_name,))
        self.compaction_thread_hm["L0"+table_name] = L0_comp_thread
        self.compaction_thread_hm["L1"+table_name] = L1_comp_thread
        L0_comp_thread.start()
        L1_comp_thread.start() #TODO re-enable this

    def start_L0Compaction(self, table_name):
        L0_lock = self.L0_lock_hm[table_name]
        L1_lock = self.L1_lock_hm[table_name]
        while True:
            L0_lock.acquire()
            L1_lock.acquire()
            self.compact_L0(table_name)
            L1_lock.release()
            L0_lock.release()
            time.sleep(5)

    def start_L1Compaction(self,table_name):
        L1_lock = self.L1_lock_hm[table_name]
        L2_lock = self.L2_lock_hm[table_name]
        while True:
            L1_lock.acquire()
            L2_lock.acquire()
            self.compact_L1(table_name)
            L2_lock.release()
            L1_lock.release()
            time.sleep(10)


    def compact_L0(self, table_name):
        if self.metadata_counts[table_name+"L1"] > 6:#if L1 would not be able to take L0, compact it
            L1_lock = self.L1_lock_hm[table_name]
            L2_lock = self.L2_lock_hm[table_name]
            L1_lock.acquire()
            L2_lock.acquire()
            self.compact_L1(table_name)
            L2_lock.release()
            L1_lock.release()
        list_of_records = []
        dir_of_L0 = "storage/"+table_name+"/L0"
        for sst in os.listdir(dir_of_L0):
            for b in os.listdir(dir_of_L0+"/"+sst):
                file_name = dir_of_L0+"/"+sst+"/"+b
                f = open(file_name, "rb")
                b_arr = bytearray(f.read())
                num_recs = int(b.split("_")[1])
                rec_num = 0
                for r in range(num_recs):
                    start_of_rec = rec_num * get_size_of_records() 
                    rec_id = int.from_bytes(b_arr[start_of_rec:start_of_rec+4], byteorder="little", signed=True)
                    rec_name = b_arr[start_of_rec+4:start_of_rec+20].decode()
                    rec_phone = b_arr[start_of_rec+20:start_of_rec+32].decode()
                    list_of_records.append(Record(rec_id, rec_name, rec_phone))
                    rec_num += 1
                f.close()
                os.remove(dir_of_L0+"/"+sst+"/"+b)

            os.rmdir(dir_of_L0+"/"+sst)
        list_of_records = [r for r in list_of_records if r.id != -2]
        list_of_records.sort(key=lambda x: x.id)
        self.write_L0_records_to_L1(list_of_records, table_name)
        self.metadata_counts[table_name+"L0"] = 0
        return


    def write_L0_records_to_L1(self, recs, table_name):
        if len(recs) == 0:
            return
        self.remove_duplicate_level_entries(recs, table_name, "L1")
        lower, upper = recs[0].id, recs[-1].id
        self.write_records_to_level_SST(recs, table_name, lower, upper, "L1")
        

    def compact_L1(self, table_name):
        list_of_records = []
        dir_of_L1 = "storage/"+table_name+"/L1"
        for sst in os.listdir(dir_of_L1):
            for b in os.listdir(dir_of_L1+"/"+sst):
                f = open(dir_of_L1+"/"+sst+"/"+b, "rb")
                b_arr = bytearray(f.read())
                num_recs = int(b.split("_")[1])
                rec_num = 0
                for r in range(num_recs):
                    start_of_rec = rec_num * get_size_of_records() 
                    rec_id = int.from_bytes(b_arr[start_of_rec:start_of_rec+4], byteorder="little", signed=True)
                    rec_name = b_arr[start_of_rec+4:start_of_rec+20].decode()
                    rec_phone = b_arr[start_of_rec+20:start_of_rec+32].decode()
                    list_of_records.append(Record(rec_id, rec_name, rec_phone))
                    rec_num += 1
                f.close()
                os.remove(dir_of_L1+"/"+sst+"/"+b)

            os.rmdir(dir_of_L1+"/"+sst)
        list_of_records = [r for r in list_of_records if r.id != -2]
        list_of_records.sort(key=lambda x: x.id)
        self.write_L1_records_to_L2(list_of_records, table_name)
        self.metadata_counts[table_name+"L1"] = 0
        return
    

    def write_L1_records_to_L2(self, recs, table_name):
        if len(recs) == 0:
            return
        self.remove_duplicate_level_entries(recs, table_name, "L2")
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
                    rec_id = int.from_bytes(b_arr[start_of_rec:start_of_rec+4], byterder="little", signed=True)
                    rec_name = b_arr[start_of_rec+4:start_of_rec+20].decode()
                    rec_phone = b_arr[start_of_rec+20:start_of_rec+32].decode()
                    list_of_records.append(Record(rec_id, rec_name, rec_phone))
                    rec_num += 1
                f.close()
                os.remove(dir_of_L2+"/"+sst+"/"+b)

            os.remove(dir_of_L2+"/"+sst)
        list_of_records = [r for r in list_of_records if r.id != -2]
        for rec in recs:
            list_of_records.append(rec)
        list_of_records.sort(key=lambda x: x.id)
        self.write_records_to_level_SST(list_of_records, table_name, list_of_records[0].id, list_of_records[-1].id, "L2")
        return


def get_size_of_records():
    return 32

class MemTable(object):
    blk_size = None
    tbl_name = None
    blocks_per_ss = None
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

    def add_record(self, record):
        self.ss_table.add(record)

    def delete_record(self, record_id):
        #make new record and set the name to -1 to indicate it is a delete node
        deleted_record = Record(record_id,"-1","-1")
        self.ss_table.add(deleted_record)
        return

    def get_in_order_records(self):
        return self.ss_table.getInOrder()

    def is_full(self):
        print(self.ss_table.get_num_records)
        return self.ss_table.get_num_records() == self.max_records





