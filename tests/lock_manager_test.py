import unittest

from lock_manager import *

class TestLockManager(unittest.TestCase):
    def test_lock_manager(self):
        lock_man = lock_manager()

        # test availability checking and queueing
        self.assertTrue(lock_man.is_read_lock_available(0, "key0"))
        self.assertTrue(lock_man.is_read_lock_available(0, "key0"))
        self.assertFalse(lock_man.is_read_lock_available(1, "key0"))
        self.assertFalse(lock_man.is_write_lock_available(2, "key0"))

        # test read lock acquiring
        lock_man.read_lock(0, "key0")
        lock_man.read_lock(1, "key0")
        self.assertFalse(lock_man.is_write_lock_available(2, "key0"))

        # test unlocking read lock
        lock_man.unlock_all_locks_for_transaction(0)
        lock_man.unlock_all_locks_for_transaction(1)
        self.assertTrue(lock_man.is_write_lock_available(2, "key0"))

        # test write lock acquiring
        lock_man.write_lock(2, "key0")
        self.assertFalse(lock_man.is_write_lock_available(1, "key0"))
        self.assertFalse(lock_man.is_read_lock_available(0, "key0"))

        # test unlocking write lock
        lock_man.unlock_all_locks_for_transaction(2)
        self.assertTrue(lock_man.is_write_lock_available(1,"key0"))

        # test write lock acquiring again
        lock_man.write_lock(1, "key0")
        self.assertFalse(lock_man.is_write_lock_available(0,"key0"))
    
if __name__ == "__main__":
    unittest.main()
