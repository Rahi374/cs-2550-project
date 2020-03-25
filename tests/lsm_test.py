import unittest

from LSMStorage import *

class TestLSM(unittest.TestCase):
    def test_lsm(self):
        lsm = LSMStorage(72, 4)
        mem_table = lsm.build_memtable("table1")
        rec1 = Record(1, "name1", "412-760-0285")
        rec2 = Record(1, "name2", "412-760-0285") #should overwrite rec1
        rec3 = Record(2, "name1", "412-760-0285")
        rec4 = Record(3, "name1", "412-760-0285")
        rec5 = Record(4, "name1", "412-760-0285")
        rec6 = Record(5, "name1", "413-760-0285")
        rec7 = Record(6, "name1", "413-760-0285")
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
        self.assertFalse(mem_table.is_full())
        mem_table.add_record(rec9)
        self.assertTrue(mem_table.is_full())

if __name__ == '__main__':
    unittest.main()
