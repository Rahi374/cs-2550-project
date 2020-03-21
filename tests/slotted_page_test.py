import unittest

from record import Record
from slotted_page import SlottedPage

class TestSlottedPage(unittest.TestCase):
    def test_regular(self):
        cname1 = "bob"
        phone1 = "1234567890"
        r1 = Record(0, cname1, phone1)
 
        blk = bytearray(64)
        sp = SlottedPage(blk, 64)

        self.assertEqual(len(sp), 0)
        self.assertEqual(sp.max_records(), 1)

        self.assertEqual(sp[0], None)

        sp[0] = r1
        self.assertEqual(sp[0].client_name, cname1)
        self.assertEqual(sp[0].phone, phone1)

        ba = sp.to_bytearray()
        self.assertEqual(len(ba), 64)

    def test_no_waste(self):
        cname1 = "bob"
        cname2 = "alice"
        phone1 = "1234567890"
        phone2 = "4352769867"
        r1 = Record(0, cname1, phone1)
        r2 = Record(1, cname2, phone2)
 
        blk = bytearray(66)
        sp = SlottedPage(blk, 66)

        self.assertEqual(len(sp), 0)
        self.assertEqual(sp.max_records(), 2)

        self.assertEqual(sp[0], None)
        self.assertEqual(sp[1], None)

        sp[0] = r1
        sp[1] = r2
        self.assertEqual(sp[0].client_name, cname1)
        self.assertEqual(sp[0].phone, phone1)
        self.assertEqual(sp[1].client_name, cname2)
        self.assertEqual(sp[1].phone, phone2)

        ba = sp.to_bytearray()
        self.assertEqual(len(ba), 66)

if __name__ == '__main__':
    unittest.main()
