from LSMStorage import *
from record import *
import time


def main():
    test_L1_search()


def test_L1_search():
    table_name = "test_table"
    lsm = LSMStorage(72, 4)
    lsm.build_memtable(table_name)
    records = get_8_records(True)
    lower, upper = records[0].id, records[-1].id
    lsm.write_records_to_level_SST(records, table_name, lower, upper, "L0")

    recs2 = get_8_records(True)[0:4]
    recs2[0].client_name = "newname1"
    recs2[1].client_name = "newname2"
    lower, upper = recs2[0].id, recs2[-1].id
    lsm.write_records_to_level_SST(recs2, table_name, lower, upper, "L0")
    lsm.compact_L0(table_name)
    rec, ss = lsm.get_record(1, table_name)
    rec2, ss2 = lsm.get_record(5, table_name)
    print(rec)
    print(ss)
    print(rec2)
    print(ss2)

def test_L0_compaction():
    table_name = "test_table"
    lsm = LSMStorage(72, 4)
    lsm.build_memtable(table_name)
    records = get_8_records(True)
    lower, upper = records[0].id, records[-1].id
    lsm.write_records_to_level_SST(records, table_name, lower, upper, "L0")

    recs2 = get_8_records(True)[0:4]
    recs2[0].client_name = "newname1"
    recs2[1].client_name = "newname2"
    lower, upper = recs2[0].id, recs2[-1].id
    lsm.write_records_to_level_SST(recs2, table_name, lower, upper, "L0")
    lsm.compact_L0(table_name)



def test_retrieve_in_L0():
    table_name = "test_table"
    lsm = LSMStorage(72, 4)
    lsm.build_memtable(table_name)
    records = get_8_records(True)
    lower, upper = records[0].id, records[-1].id
    lsm.write_records_to_level_SST(records, table_name, lower, upper, "L0")

    recs2 = get_8_records(True)[0:4]
    recs2[0].client_name = "newname1"
    recs2[1].client_name = "newname2"
    lower, upper = recs2[0].id, recs2[-1].id
    lsm.write_records_to_level_SST(recs2, table_name, lower, upper, "L0")

    rec, ss = lsm.get_record(1, table_name)
    print(rec)
    rec2, ss2 = lsm.get_record(4, table_name)
    print(rec2)

def test_second_L0_write():
    table_name = "test_table"
    lsm = LSMStorage(72, 4)
    lsm.build_memtable(table_name)
    records = get_8_records(True)
    lower, upper = records[0].id, records[-1].id
    lsm.write_records_to_level_SST(records, table_name, lower, upper, "L0")

    recs2 = get_8_records(True)[0:4]
    recs2[0].client_name = "newname1"
    recs2[1].client_name = "newname2"
    lower, upper = recs2[0].id, recs2[-1].id
    lsm.write_records_to_level_SST(recs2, table_name, lower, upper, "L0")

    

def test_uneven_L0_write():
    table_name = "test_table"
    lsm = LSMStorage(72, 4)
    lsm.build_memtable(table_name)
    records = get_8_records(True)[0:-1]
    lower, upper = records[0].id, records[-1].id
    print("lower: "+str(lower))
    print("upper: "+str(upper))
    lsm.write_records_to_level_SST(records, table_name, lower, upper, "L0")
    f = open("storage/test_table/L0/SST0/3_1", "rb+")
    b_arr2 = bytearray(f.read())
    rec_id = int.from_bytes(b_arr2[0:4], byteorder="little", signed=True)
    print(rec_id)
    name = b_arr2[4:20].decode()
    print(name)
    phone = b_arr2[20:32].decode()
    print(phone)

def test_write_and_read_records_from_file():
    recs = get_8_records(True)
    b_arr = bytearray()
    for r in recs:
        print(r)
        b_arr += r.to_bytearray()
    print(b_arr)
    f = open("test_file", "wb+")
    f.write(b_arr)
    f.close()
    
    f2 = open("test_file", "rb")
    b_arr2 = bytearray(f2.read())
    print(b_arr2)
    print("\n")
    print("b_arr's equal: "+str(b_arr == b_arr2))
    rec_id = int.from_bytes(b_arr2[0:4], byteorder="little", signed=True)
    print(rec_id)
    name = b_arr2[4:20].decode()
    print(name)
    phone = b_arr2[20:32].decode()
    print(phone)

def test_write_to_L0():
    table_name = "test_table"
    lsm = LSMStorage(72, 4)
    lsm.build_memtable(table_name)
    records = get_8_records(True)
    lower, upper = records[0].id, records[-1].id
    print("lower: "+str(lower))
    print("upper: "+str(upper))
    lsm.write_records_to_level_SST(records, table_name, lower, upper, "L0")

def test_lambda_sort():
    records = get_8_records(False)
    records.sort(key=lambda x: x.id)
    print("sorted records:\n")
    for sr in records:
        print(sr)


def test_build_push_memtable():
    lsm = LSMStorage(72, 4)
    mem_table = lsm.build_memtable("table1")
    rec1 = Record(1, "name1", "412-760-0285")
    rec2 = Record(1, "name2", "412-760-0285") #should overwrite rec1
    rec3 = Record(2, "name3", "412-760-0285")
    rec4 = Record(3, "name4", "412-760-0285")
    rec5 = Record(4, "name5", "412-760-0285")
    rec6 = Record(5, "name6sd", "413-760-0285")
    rec7 = Record(6, "name7sdf", "413-760-0285")
    rec8 = Record(7, "name1", "413-760-0285")
    rec9 = Record(8, "name1", "412-760-0285")
    mem_table.add_record(rec1)
    mem_table.add_record(rec2)
    mem_table.add_record(rec3)
    mem_table.add_record(rec4)
    mem_table.add_record(rec5)
    mem_table.add_record(rec6)
    mem_table.add_record(rec7)
    mem_table.add_record(rec8)
    print("mem_table.is_full: "+str(mem_table.is_full())+" (should be false)")
    mem_table.add_record(rec9)
    print("mem_table.is_full: "+str(mem_table.is_full())+" (should be true)")
    lsm.push_memtable(mem_table)
    mem_table = lsm.build_memtable("table1")
    rec1 = Record(1, "name1", "412-760-0285")
    rec2 = Record(2, "name222", "412-760-0285")
    rec3 = Record(3, "name122", "412-760-0285")
    mem_table.add_record(rec1)
    mem_table.add_record(rec2)
    mem_table.add_record(rec3)
    lsm.push_memtable(mem_table)




def get_8_records(sorted):
    records = [
        Record(1, "name1", "412-760-0285"),
        Record(8, "name8", "412-760-0285"),
        Record(3, "name3", "412-760-0285"),
        Record(6, "name6", "413-760-0285"),
        Record(4, "name44444", "412-760-0285"),
        Record(5, "name555", "413-760-0285"),
        Record(7, "name7", "413-760-0285"),
        Record(2, "name2", "412-760-0285")
    ]
    if sorted:
        records.sort(key=lambda x: x.id)
    return records







if __name__ == "__main__":
    main()
