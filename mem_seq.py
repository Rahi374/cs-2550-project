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
from common import ORG
import heapq as pq
import os
from datetime import datetime
from slotted_page_cache import *

class Mem():
    def __init__(self, mem_size, block_size, storage, storage_type):
        self.storage = storage
        self.storage_type = storage_type
        self.block_size = block_size

        if storage_type == ORG.SEQ:
            self.cache = SlottedPageCache(mem_size, block_size, storage) 


    def retrieve_rec(self, table: str, rec_id: int, field: str, is_primary: bool):
        if storage_type == ORG.SEQ:
            records = None
            if is_primary:
                records = self.cache.search_by_id(table, rec_id)
            else:
                records = self.cache.search_by_area_code(table, field)

            if records == None: # Miss
                records = self.__walk_storage_for_records(table, rec_id, field, is_primary)

            return records
        else:
            pass

    def write_rec(self, table: str, rec: tuple):
        # Find a slotted_page on cache to write into
        key = self.cache.available_slotted_page(table)
        if key is not None:
            self.cache[key]['slotted_page'].insert(rec)
            self.cache[key]['last_used'] = datetime.now() 
            return
        
        # Find a slotted_page on disk to write into if mem's slotted page is ALL full
        # Cache eviction will happen if cache is full regardless of if an slotted_page block is found or not
        block_id = self.__find_available_slotted_page(table)
        if block_id is not None:
            self.cache[(table, int(block_id))]['slotted_page'].insert(rec)
            self.cache[(table, int(block_id))]['last_used'] = datetime.now()
            return

        # Create a new slotted page
        if not self.cache.can_add():
            raise Exception("Eviction should've already occured in previous code block. Something is off")

        new_block_id = self.__find_next_block_id()
        new_slotted_page = SlottedPage(block_size = self.block_size)
        new_slotted_page.insert(rec)
        self.cache.cache(table, new_block_id, new_slotted_page) 
        

    def update_rec(self, table: str, rec: tuple):
        # Check if rec is actually in mem. 
        # There is a chance that it is brought in without its block being cached
        record = self.cache.search_by_id(table, rec.id)
        if record is not None:
            # Should be good to update the tuple in core on the spot since its pass by reference
            return

        # Record's block is missing in cache. Search for it in storage and bring it to cache
        record = self.__walk_storage_for_records(table, rec.id, None, true)
        if record is not None:
            record.overwrite_values(rec)

        # If record is None, then something is wrong with our code. 
        # Core shouldnt be able to update a record if it doesnt exist
        raise Exception("Trying to update an record that doesnt exist")

    def delete_rec(self, table: str, rec: tuple):
        # Check if rec is actually in mem. 
        # There is a chance that it is brought in without its block being cached
        slotted_page = self.search_slotted_page_by_rec_id(table, rec.id)
        if slotted_page is not None:
            slotted_page.delete(rec)
            return

        # Record's block is missing in cache. Search for it in storage and bring it to cache
        slotted_page = self.__walk_storage_for_slotted_page(table, rec.id, None, true)
        if slotted_page is not None:
            slotted_page.delete(rec)
            return 

        # If slotted_page is None, then something is wrong with our code. 
        # Core shouldnt be able to delete a record that is not found anywhere
        raise Exception("Trying to delete a record that isnt found anywhere")

    def __walk_storage_for_records(self, table_name, rec_id, field, is_primary):
        if not self.cache.can_add():
            self.cache.evict()

        if not self.cache.can_add():
            raise Exception("At one point in time, cache exceeded by more than 1")

        all_records = []
        slotted_page_to_cache = None
        disk_block_id_to_cache = None

        directory_path = self.storage.mnt_path + table_name + '/'
        for root, dirs, files in os.walk(directory_path):
            for disk_block in files:
                block_id = int(disk_block.split("_")[1].split(".")[0])
                ba = self.storage.read_blk(table_name, block_id)

                slotted_page = SlottedPage(self.block_size, ba)
                for record in slotted_page.records:
                    if is_primary and record is not None and record.id == rec_id: # Searching on primary key
                        self.cache.cache(table_name, block_id, slotted_page)
                        return record
                    elif not is_primary and record is not None and record.phone.split("-")[0] == field: # Searching by area code
                        all_records.append(record)
                        slotted_page_to_cache = slotted_page
                        disk_block_id_to_cache = block_id
        
        if len(all_records) > 0 and slotted_page_to_cache is not None and disk_block_id_to_cache is not None:
            self.cache.cache(table_name, disk_block_id_to_cache, slotted_page_to_cache)
            return all_records

        # Not in Storage
        return None

    def __walk_storage_for_slotted_page(self, table_name, rec_id):
        if not self.cache.can_add():
            self.cache.evict()

        if not self.cache.can_add():
            raise Exception("At one point in time, cache exceeded by more than 1")

        all_records = []
        slotted_page_to_cache = None
        disk_block_id_to_cache = None

        directory_path = self.storage.mnt_path + table_name + '/'
        for root, dirs, files in os.walk(directory_path):
            for disk_block in files:
                block_id = int(disk_block.split("_")[1].split(".")[0])
                ba = self.storage.read_blk(table_name, block_id)

                slotted_page = SlottedPage(ba, self.block_size)
                for record in slotted_page.records:
                    if record is not None and record.id == rec_id: # Searching on primary key
                        self.cache.cache(table_name, block_id, slotted_page)
                        return self.cache[(table_name, block_id)]['slotted_page']
        
        # Not in Storage
        return None



    def __find_available_slotted_page(self, table_name):
        if not self.cache.can_add():
            self.cache.evict()

        if not self.cache.can_add():
            raise Exception("At one point in time, cache exceeded by more than 1")

        directory_path = self.storage.mnt_path + table_name + '/'
        for root, dirs, files in os.walk(directory_path):
            for disk_block in files:
                block_id = int(disk_block.split("_")[1].split(".")[0])
                ba = self.storage.read_blk(table_name, block_id)

                slotted_page = SlottedPage(ba, self.block_size)
                if slotted_page.has_space():
                    self.cache.cache(table_name, block_id, slotted_page)
                    return block_id

        return None

    def __find_next_block_id(self):
        highest_block_id = 0

        for root, dirs, files in os.walk(directory_path):
            for disk_block in files:
                block_id = int(disk_block.split("_")[1].split(".")[0])
                if block_id > highest_block_id:
                    highest_block_id = block_id 
        
        return highest_block_id + 1