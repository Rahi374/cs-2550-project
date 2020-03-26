from datetime import datetime
from common import *
from storage import *
from slotted_page import *
from record import *

class SlottedPageCache():

    def __init__(self, cache_size: int, block_size: int, storage: Storage):
        self.CACHE_SIZE = cache_size
        self.MAX_NUM_BLOCKS = cache_size / block_size
    
        self.storage = storage
        
        # Dict will be mapping COMPOSITE KEY (table_name, block_id) -> {slotted_page = slotted_page Object, last_used = timestamp}
        self.cache_dic = {}
        

    def cache(self, table_name: str, block_id: int, slotted_page: SlottedPage):
        if not isinstance(table_name, str):
            raise TypeError("Table name must be str")

        if not isinstance(block_id, int):
            raise TypeError("block_id must be an int") 

        if not isinstance(slotted_page, SlottedPage):
            raise TypeError("SP must be a SlottedPage class")

        if (table_name, block_id) in self.cache_dic:
            raise KeyError(f"Cannot cache ({table_name},{block_id}) since it is already in cache")

        if not self.can_add():
            self.evict()
        
        self.cache_dic[(table_name, block_id)] = {
                'slotted_page': slotted_page,
                'last_used': datetime.now()
        }

    def can_add(self):
        return len(self.cache_dic) + 1 <= self.MAX_NUM_BLOCKS

    def evict(self):
        print("evicting!")
        least_recently_used_id = min(self.cache_dic, key=lambda x: self.cache_dic[x]['last_used'])
        table_name = least_recently_used_id[0]
        block_id = int(least_recently_used_id[1])
        evicting_bytearray = self.cache_dic[least_recently_used_id]['slotted_page'].to_bytearray()

        self.storage.write_blk(table_name, block_id, evicting_bytearray)

        del self.cache_dic[(table_name, block_id)]

    def search_by_id(self, table_name: str, record_id: int):
        if not isinstance(table_name, str):
            raise TypeError("Table name must be str")
       
        if not isinstance(record_id, int):
            raise TypeError("Record_id must be int")

        keys = self.cache_dic.keys()

        for key in keys:
            if key[0] == table_name:
                records = self.cache_dic[key]['slotted_page'].records
                for record in records:
                    if record is not None and record.id == record_id:
                        self.cache_dic[key]['last_used'] = datetime.now()
                        return [record]

        return None

    def search_slotted_page_by_rec_id(self, table_name: str, record_id: int):
        if not isinstance(table_name, str):
            raise TypeError("Table name must be str")
        if not isinstance(record_id, int):
            raise TypeError("Record_id must be int")

        keys = self.cache_dic.keys()

        for key in keys:
            if key[0] == table_name:
                records = self.cache_dic[key]['slotted_page'].records
                for record in records:
                    if record is not None and record.id == record_id:
                        return self.cache_dic[key]['slotted_page']

        return None

    def search_by_area_code(self, table_name: str, area_code: int, field_name: str):
        if not isinstance(table_name, str):
            raise TypeError("Table name must be str")
       
        if not isinstance(area_code, int):
            raise TypeError("area_code must be int")

        keys = self.cache_dic.keys()
        all_records = []

        for key in keys:
            if key[0] == table_name:
                records = self.cache_dic[key]['slotted_page'].records
                for record in records:
                    if record is not None and field_name == "area_code" and int(record.phone.split("-")[0]) == area_code:
                        all_records.append(record)
                        self.cache_dic[key]['last_used'] = datetime.now()

        return all_records if len(all_records) > 0 else None

    def available_slotted_page(self, table_name: str):
        if not isinstance(table_name, str):
            raise TypeError("Table name must be str")

        keys = self.cache_dic.keys()

        for key in keys:
            if key[0] == table_name:
                if self.cache_dic[key]['slotted_page'].has_space():
                    return key


        return None

    def delete_table(self, table_name: str):
        keys = list(self.cache_dic.keys())

        for key in keys:
            if key[0] == table_name:
                print(f"deleting {key}")
                del self.cache_dic[key]

    def flush_cache(self):
        keys = self.cache_dic.keys()

        for key in keys:
            self.storage.write_blk(key[0], key[1], self.cache_dic[key]['slotted_page'].to_bytearray())

    def print_cache(self):
        pp.pprint(self.cache_dic)
