import os
import shutil
import threading
import time
import math
from record import *
from SSTable import *
from logger import Logger

class LSMStorage():
    blk_size = None
    blocks_per_ss = None
    metadata_counts = {}#number or SSTables in a level for a table
    metadata_ranges = {}#ranges of values in an SSTable
    L0_lock_hm = {}
    L1_lock_hm = {}
    L2_lock_hm = {}
    level_to_lock_hm = {}
    compaction_thread_hm = {}
    thread_should_run = {}


    def __init__(self, block_size, blocks_per_SS):
        self.blk_size = block_size
        self.blocks_per_ss = blocks_per_SS
        self.level_to_lock_hm["L0"] = self.L0_lock_hm
        self.level_to_lock_hm["L1"] = self.L1_lock_hm
        self.level_to_lock_hm["L2"] = self.L2_lock_hm

        # delete old directory if exists
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

    def kill_all_compaction_threads(self):
        for thread_key in self.thread_should_run:
            self.thread_should_run[thread_key] = False

    def save_table(self, table_name):
        tmp_table_save_path = f"python_is_garbage_{table_name}_backup.tar.gz"
        os.system(f"tar -czf {tmp_table_save_path} storage/{table_name}")
        f = open(tmp_table_save_path, "rb")
        blob = f.read()
        f.close()
        os.remove(tmp_table_save_path)
        return blob

    def restore_table(self, table_storage):
        tmp_table_save_path = f"python_is_a_crapbag_{table_storage.table_name}_backup.tar.gz"
        f = open(tmp_table_save_path, "wb")
        f.write(table_storage.blob)
        f.close()
        os.system(f"tar -xzf {tmp_table_save_path} storage/{table_storage.table_name}")
        os.remove(tmp_table_save_path)

        for e in table_storage.copy_metadata_ranges.keys():
            self.metadata_ranges[e] = table_storage.copy_metadata_ranges[e]
        for e in table_storage.copy_metadata_counts.keys():
            self.metadata_counts[e] = table_storage.copy_metadata_counts[e]
        
    def write_storage_levels(self, table_name, level, list_of_files):
        level_dir = "storage/"+table_name+"/"+level
        os.mkdir(level_dir)
        for f_entry in list_of_files:
            fname = f_entry[0]
            fdata = f_entry[1]
            f = open(fname, "wb+")
            f.write(fdata)
            f.close()

    def get_table_storage(self, table_name):
        table_exists = self.table_exists(table_name)
        if not table_exists:
            return None
        else:
            blob = self.save_table(table_name)
            ranges = self.get_ranges(table_name)
            counts = self.get_counts(table_name)
            return TableStorage(table_name, ranges, counts, blob)

    #return 3 lists, one for each level
    #each list is a list of lists where entries are: ["filename":file_data]
    def get_level_storage(self, table_name): 
        table_dir = "storage/"+table_name
        l0_dir = table_dir+"/L0"
        l1_dir = table_dir+"/L1"
        l2_dir = table_dir+"/L2"
        l0_entries = self.get_entries(l0_dir)
        l1_entries = self.get_entries(l1_dir)
        l2_entries = self.get_entries(l2_dir)
        return l0_entries, l1_entries, l2_entries

    def get_entries(self, path):
        all_files = []
        for fname in os.listdir(path):
            f = open(path+'/'+fname, "rb")
            data = bytearray(f.read())
            f.close()
            all_files.append([fname, data])
        return all_files


    def get_ranges(self, table_name):
        partial_hm = {}
        keys = list(self.metadata_ranges.keys())
        for e in keys:
            if table_name in e:
                partial_hm[e] = self.metadata_ranges[e]
                del self.metadata_ranges[e]
        return partial_hm

    def get_counts(self, table_name):
        partial_hm = {}
        keys = list(self.metadata_counts.keys())
        for e in keys:
            if table_name in e:
                partial_hm[e] = self.metadata_counts[e]
                del self.metadata_counts[e]
        return partial_hm

    def get_record(self, record_id, table_name):
        table_exists = self.table_exists(table_name)
        level = 0

        if not table_exists:
            print("no table")
            return -1
        else:
            record_id = record_id.strip()
            rec, ss = self.check_level_for_rec(record_id, table_name, "L0")
            if ss == -1:
                rec, ss = self.check_level_for_rec(record_id, table_name, "L1")
                if ss == -1:
                    rec, ss = self.check_level_for_rec(record_id, table_name, "L2")
                else:
                    return rec, ss, 1
            else:
                return rec, ss, 0
            return rec, ss, 2

    def get_records(self, area_code, table_name, hm_keys_found, rec_list):
        table_exists = self.table_exists(table_name)
        if not table_exists:
            print("no table")
            return -1

        l0_list = []
        l1_list = []
        l2_list = []
        self.get_matching_area_code_byte_arrays(l0_list, rec_list, table_name, area_code, "L0", hm_keys_found)
        self.get_matching_area_code_byte_arrays(l1_list, rec_list, table_name, area_code, "L1", hm_keys_found)
        self.get_matching_area_code_byte_arrays(l2_list, rec_list, table_name, area_code, "L2", hm_keys_found)
        list_of_bytearrays_lists = [l0_list, l1_list, l2_list]
        return list_of_bytearrays_lists, rec_list

    def get_matching_area_code_byte_arrays(self, list_of_bytearrays, rec_list, table_name, area_code, level, hm_keys_found):
        level_dir = "storage/"+table_name+"/"+level
        for sst in os.listdir(level_dir):
            sst_dir = level_dir + "/" + sst
            if self.sst_contains_record_matching_area_code(sst_dir, area_code, rec_list, hm_keys_found):
                list_of_bytearrays.append(self.get_sst_as_b_arr(table_name, level, sst))

    def sst_contains_record_matching_area_code(self, sst_dir, area_code, rec_list, hm_keys_found):
        blocks = os.listdir(sst_dir)
        match_found = False
        for b in blocks:
            num_recs = int(b.split("_")[1])
            f = open(sst_dir+"/"+b, "rb")
            b_arr = bytearray(f.read())
            for i in range(num_recs):
                start_of_rec = i * get_size_of_records()
                record = Record(ba=b_arr[start_of_rec:start_of_rec+RECORD_SIZE])
                rec_id = record.id
                rec_name = record.client_name
                rec_phone_num = record.phone
                rec_area_code = rec_phone_num.split("-")[0]

                if rec_area_code == str(area_code):
                    match_found = True
                    if rec_id not in hm_keys_found:
                        hm_keys_found.add(rec_id)
                        ret_rec = Record(rec_id, rec_name, rec_phone_num)
                        rec_list.append(ret_rec)


            f.close()
        return match_found

    def build_memtable(self, table_name):
        table_exists = self.table_exists(table_name)
        if not table_exists:
            self.metadata_counts[table_name+"L0"] = 0
            self.metadata_counts[table_name+"L1"] = 0
            self.metadata_counts[table_name+"L2"] = 0
            self.L0_lock_hm[table_name] = threading.Lock()
            self.L1_lock_hm[table_name] = threading.Lock()
            self.L2_lock_hm[table_name] = threading.Lock()
            self.create_table_structure(table_name)
            self.thread_should_run[table_name] = True
            self.start_compaction_threads(table_name) #TODO re-enable this
        return MemTable(self.blk_size, self.blocks_per_ss ,table_name)


    def push_memtable(self, memtable):
        #print("Pushing memtable")
        table_name = memtable.tbl_name
        #print("got l0 lock for push_memtable")
        all_records = memtable.get_in_order_records()
        lower, upper = all_records[0].id, all_records[-1].id
        self.write_records_to_level_SST(all_records, memtable.tbl_name, lower, upper, "L0")
        #print("count of tables is: "+str(self.metadata_counts[table_name+"L0"]))
        if self.metadata_counts[table_name+"L0"] >= 4: #compact L0 if no more room
            if self.metadata_counts[table_name+"L1"] >= 6:#if L1 would not be able to take L0, compact it
                #print("need to compact L1")
                self.compact_L1(table_name)
            self.compact_L0(table_name)



    def delete_table(self, table_name):
        if not self.table_exists(table_name):
            return

        delete_log_str = "DELETED: "+table_name
        Logger.log(delete_log_str)
        try:
            L0_lock = self.L0_lock_hm[table_name]
            L1_lock = self.L1_lock_hm[table_name]
            L2_lock = self.L2_lock_hm[table_name]
            #L0_lock.acquire()
            #print("l0 lock acquired 2")
            #L1_lock.acquire()
            #print("l1 lock acquired 2")
            #L2_lock.acquire()
            #print("l2 lock acquired 2")
            #print("done acquiring locks for 2")
            self.thread_should_run[table_name] = False
            shutil.rmtree('storage/'+table_name)
            #L2_lock.release()
            #print("l2 lock released 2")
            #L1_lock.release()
            #print("l1 lock released 2")
            #L0_lock.release()
        except Exception as e:
            print(e)
            print("exception in deletion of table")
        return


    #private methods
    def table_exists(self, table_name):
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
        create_log_str = "CREATE "+level+" K-"+table_name+str(lower)+"-"+table_name+str(upper)
        Logger.log(create_log_str)
        while records_to_write > 0:
            if records_to_write < records_per_block:
                recs_to_write = records[i:i+records_to_write]
                f = open(dir_path+"/"+str(c)+"_"+str(records_to_write), "wb+")
                f.write(self.get_byte_array_of_records(recs_to_write))
                f.close()
                records_to_write = 0
            else:
                recs_to_write = records[i:i+records_per_block]
                f = open(dir_path+"/"+str(c)+"_"+str(records_per_block), "wb+")
                f.write(self.get_byte_array_of_records(recs_to_write))
                f.close()
                records_to_write -= records_per_block
                c += 1
                i += records_per_block


    def check_level_for_rec(self, record_id, table_name, level):
        #level_lock = self.level_to_lock_hm[level][table_name]
        #level_lock.acquire()
        #print("level: "+level+" lock acquired 333")
        #print("done acquiring locks for 333")
        dir_path = "storage/"+table_name+"/"+level
        ss_tables = os.listdir(dir_path)
        rec, ss = None, -1
        for s in ss_tables:
            lower, upper = self.metadata_ranges[dir_path+"/"+s]
            if lower <= int(record_id) and upper >= int(record_id):
                rec, ss = self.check_sst_for_record(record_id, table_name, level, s)
                if ss != -1:
                    #level_lock.release()
                    return rec, ss
        #level_lock.release()
        #print("level: "+level+" lock released 333")
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
                record = Record(ba=b_arr[start_of_rec:start_of_rec+RECORD_SIZE])
                if record.id == int(record_id):
                    f.close()
                    return record, self.get_sst_as_b_arr(table_name, level, sst)

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
        f = open(dir_path, "rb")
        b_arr = bytearray(f.read())
        for r in range(num_recs):
            start_of_rec = r * get_size_of_records() 
            rec_id = int.from_bytes(b_arr[start_of_rec:start_of_rec+4], byteorder="little", signed=True)
            if rec_id in rec_hm or -1*rec_id in rec_hm:
                b_arr = b_arr[0:start_of_rec] + Record(-1*rec_id,"1","1").to_bytearray() + b_arr[start_of_rec+32:]
        f.close()
        f = open(dir_path, "wb+")
        f.write(b_arr)
        f.close()


    def start_compaction_threads(self, table_name):
        L0_comp_thread = threading.Thread(target=self.start_L0Compaction, args=(table_name,))
        L1_comp_thread = threading.Thread(target=self.start_L1Compaction, args=(table_name,))
        self.compaction_thread_hm["L0"+table_name] = L0_comp_thread
        self.compaction_thread_hm["L1"+table_name] = L1_comp_thread
        #L0_comp_thread.start()
        #L1_comp_thread.start()

    def start_L0Compaction(self, table_name):
        while True:
            should_still_run = self.thread_should_run[table_name]
            if not should_still_run:
                break
            self.compact_L0(table_name)
            time.sleep(5)

    def start_L1Compaction(self,table_name):
        while True:
            should_still_run = self.thread_should_run[table_name]
            if not should_still_run:
                break
            self.compact_L1(table_name)
            time.sleep(10)


    def compact_L0(self, table_name):
        L0_lock = self.L0_lock_hm[table_name]
        L1_lock = self.L1_lock_hm[table_name]
        if self.metadata_counts[table_name+"L1"] >= 6:#if L1 would not be able to take L0, compact it
            self.compact_L1(table_name)
        #L0_lock.acquire()
        #L1_lock.acquire()
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
        list_of_records = [r for r in list_of_records if r.id > 0 or r.phone == "-1"]
        list_of_records.sort(key=lambda x: x.id)
        #L1_lock.release()
        #L0_lock.release()
        self.write_L0_records_to_L1(list_of_records, table_name)
        self.metadata_counts[table_name+"L0"] = 0
        return


    def write_L0_records_to_L1(self, recs, table_name):
        if len(recs) == 0:
            return
        self.remove_duplicate_level_entries(recs, table_name, "L1")
        

        list_of_records = []
        dir_of_L1 = "storage/"+table_name+"/L1"
        for sst in os.listdir(dir_of_L1):
            for b in os.listdir(dir_of_L1+"/"+sst):
                f = open(dir_of_L1+"/"+sst+"/"+b, "w+b")
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
        list_of_records = [r for r in list_of_records if r.id > 0 or r.phone == "-1"]
        for rec in recs:
            list_of_records.append(rec)
        list_of_records.sort(key=lambda x: x.id)
        #L2_lock.release()
        #L1_lock.release()
        num_ssts_needed = math.ceil(len(list_of_records)/self.get_records_per_sst())
        total_records_needed_written = len(list_of_records)
        recs_written = 0
        for i in range(num_ssts_needed):
            if total_records_needed_written >= self.get_records_per_sst():
                recs_to_write = list_of_records[i*self.get_records_per_sst(): (i+1)*self.get_records_per_sst()]
                self.write_records_to_level_SST(recs_to_write, table_name, recs_to_write[0].id, recs_to_write[-1].id, "L1")
                total_records_needed_written -= self.get_records_per_sst()
            else:
                recs_to_write = list_of_records[i*self.get_records_per_sst():i*self.get_records_per_sst()+total_records_needed_written]
                self.write_records_to_level_SST(recs_to_write, table_name, recs_to_write[0].id, recs_to_write[-1].id, "L1")
                total_records_needed_written = 0


    def compact_L1(self, table_name):
        #L1_lock = self.L0_lock_hm[table_name]
        #L2_lock = self.L1_lock_hm[table_name]
        #L1_lock.acquire()
        #L2_lock.acquire()
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
        list_of_records = [r for r in list_of_records if r.id > 0 or r.phone == "-1"]
        list_of_records.sort(key=lambda x: x.id)
        #L2_lock.release()
        #L1_lock.release()
        self.write_L1_records_to_L2(list_of_records, table_name)
        self.metadata_counts[table_name+"L1"] = 0
        return


    def write_L1_records_to_L2(self, recs, table_name):
        if len(recs) == 0:
            return
        L1_lock = self.L0_lock_hm[table_name]
        L2_lock = self.L1_lock_hm[table_name]
        #L1_lock.acquire()
        #L2_lock.acquire()
        self.remove_duplicate_level_entries(recs, table_name, "L2")
        list_of_records = []
        dir_of_L2 = "storage/"+table_name+"/L2"
        for sst in os.listdir(dir_of_L2):
            for b in os.listdir(dir_of_L2+"/"+sst):
                f = open(dir_of_L2+"/"+sst+"/"+b, "w+b")
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
                os.remove(dir_of_L2+"/"+sst+"/"+b)

            os.rmdir(dir_of_L2+"/"+sst)
        list_of_records = [r for r in list_of_records if r.id > 0 or r.phone == "-1"]
        for rec in recs:
            list_of_records.append(rec)
        list_of_records.sort(key=lambda x: x.id)
        #L2_lock.release()
        #L1_lock.release()
        num_ssts_needed = math.ceil(len(list_of_records)/self.get_records_per_sst())
        total_records_needed_written = len(list_of_records)
        recs_written = 0
        for i in range(num_ssts_needed):
            if total_records_needed_written >= self.get_records_per_sst():
                recs_to_write = list_of_records[i*self.get_records_per_sst(): (i+1)*self.get_records_per_sst()]
                self.write_records_to_level_SST(recs_to_write, table_name, recs_to_write[0].id, recs_to_write[-1].id, "L2")
                total_records_needed_written -= self.get_records_per_sst()
            else:
                recs_to_write = list_of_records[i*self.get_records_per_sst():i*self.get_records_per_sst()+total_records_needed_written]
                self.write_records_to_level_SST(recs_to_write, table_name, recs_to_write[0].id, recs_to_write[-1].id, "L2")
                total_records_needed_written = 0


        return


    def get_records_per_sst(self):
        return self.blocks_per_ss * math.floor(self.blk_size / get_size_of_records())

def get_size_of_records():
    return 32

class MemTable(object):
    blk_size = None
    tbl_name = None
    blocks_per_ss = None
    max_records = None
    num_records = 0


    def __init__(self, block_size, blocks_per_SS, table_name):
        self.blk_size = block_size
        self.tbl_name = table_name
        self.blocks_per_ss = blocks_per_SS
        self.max_records = self.blocks_per_ss * math.floor(self.blk_size / get_size_of_records())
        self.ss_table = SSTable(self.max_records)

    def add_record(self, record):
        self.ss_table.add(record)

    def delete_record(self, record_id):
        #make new record and set the name to -1 to indicate it is a delete node
        deleted_record = Record(-1*record_id,"-1","-1")
        self.ss_table.add(deleted_record)
        return

    def get_in_order_records(self):
        return self.ss_table.getInOrder()

    def is_full(self):
        # print('isfull test: ', self.ss_table)
        return self.ss_table.isFull()

    def __str__(self):
        return str(self.get_in_order_records()) + ' is_full: ' + str(self.is_full())



class TableStorage(object):
    def __init__(self, table_name, ranges, counts, blob):
        self.table_name = table_name
        self.copy_metadata_ranges = ranges
        self.copy_metadata_counts = counts
        self.blob = blob
