from LSMStorage import *


def main():
    lsm = LSMStorage(72, 4)
    mem_table = lsm.build_memtable("table1")
    mem_table.add_record

if __name__ == "__main__":
    main()
