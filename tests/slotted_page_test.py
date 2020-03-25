import unittest

from record import Record
from slotted_page import SlottedPage

cname1 = "bob"
cname2 = "alice"
phone1 = "1234567890"
phone2 = "4352769867"

record1 = Record(0, cname1, phone1)
record2 = Record(1, cname2, phone2)

test_record = Record(5, "abcdefgh", "012-345-6789")
test_ba = bytearray([
    5, 0, 0, 0,
    97, 98, 99, 100, 101, 102, 103, 104, 0, 0, 0, 0, 0, 0, 0, 0,
    48, 49, 50, 45, 51, 52, 53, 45, 54, 55, 56, 57,
    5, 0, 0, 0,
    97, 98, 99, 100, 101, 102, 103, 104, 0, 0, 0, 0, 0, 0, 0, 0,
    48, 49, 50, 45, 51, 52, 53, 45, 54, 55, 0, 0])

class TestSlottedPage(unittest.TestCase):
    def test_regular(self):
        r1 = Record(0, cname1, phone1)
 
        sp = SlottedPage(64)

        self.assertEqual(len(sp), 0)
        self.assertEqual(sp.max_records(), 1)

        self.assertEqual(sp[0], None)

        sp[0] = r1
        self.assertEqual(sp[0].client_name, cname1)
        self.assertEqual(sp[0].phone, phone1)

        ba = sp.to_bytearray()
        self.assertEqual(len(ba), 64)

    def test_no_waste(self):
        r1 = Record(0, cname1, phone1)
        r2 = Record(1, cname2, phone2)
 
        sp = SlottedPage(66)

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

    def test_conversion(self):
        sp = SlottedPage(64)

        sp[0] = test_record

        sp2 = SlottedPage(64, test_ba)

        self.assertEqual(sp[0], sp2[0])
        self.assertEqual(type(sp2[0]), Record)

    def test_insert(self):
        sp = SlottedPage(66)
        sp[1] = Record(0, cname1, phone1)
        sp.insert(test_record)

        self.assertEqual(sp[0], test_record)

        sp = SlottedPage(66)
        sp[0] = Record(0, cname1, phone1)
        sp.insert(test_record)

        self.assertEqual(sp[1], test_record)

        self.assertEqual(type(sp[1]), Record)

    def test_delete(self):
        sp = SlottedPage(66)
        sp.insert(record1)
        sp.insert(record2)

        sp.delete(record1.id)
        self.assertIsNone(sp[0])
        self.assertEqual(sp[1], record2)

        sp = SlottedPage(66)
        sp.insert(record1)
        sp.insert(record2)

        sp.delete(record2.id)
        self.assertIsNone(sp[1])
        self.assertEqual(sp[0], record1)

        sp = SlottedPage(66)
        sp[1] = record2

        sp.delete(record2.id)
        self.assertIsNone(sp[0])
        self.assertIsNone(sp[1])

if __name__ == '__main__':
    unittest.main()
