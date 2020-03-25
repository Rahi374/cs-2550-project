import unittest

from record import Record

id1 = 5
cname1 = "abcdefgh"
phone1 = "012-345-6789"

test_record = Record(id1, cname1, phone1)
test_ba = bytearray([
    5, 0, 0, 0,
    97, 98, 99, 100, 101, 102, 103, 104, 0, 0, 0, 0, 0, 0, 0, 0,
    48, 49, 50, 45, 51, 52, 53, 45, 54, 55, 56, 57])

empty_ba = bytearray([
    0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])

class TestRecord(unittest.TestCase):
    def test_bytearray_conversion(self):
        self.assertEqual(test_record, Record(ba=test_ba))
        self.assertEqual(test_record, Record(id1, cname1, phone1))

        self.assertEqual(test_ba, test_record.to_bytearray())
        self.assertEqual(test_ba, Record(id1, cname1, phone1).to_bytearray())

    def test_bytearray_invalid_conversion(self):
        self.assertRaises(Exception, Record, ba=empty_ba)

        self.assertRaises(Exception, Record, ba=test_ba[0:31])
        self.assertRaises(Exception, Record, ba=test_ba+bytearray([0]))

if __name__ == '__main__':
    unittest.main()
