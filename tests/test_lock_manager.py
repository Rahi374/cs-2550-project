from lock_manager import *

def main():
    print("Running test of lock manager")
    lock_man = lock_manager()
    ret = lock_man.is_read_lock_available(0,"key0")
    print(f"ret should be true: {ret}")
    ret = lock_man.is_read_lock_available(0,"key0")
    print(f"ret should be true: {ret}")
    ret = lock_man.is_read_lock_available(1,"key0")
    print(f"ret should be false: {ret}")
    ret = lock_man.is_write_lock_available(2,"key0")
    print(f"ret should be false: {ret}")
    lock_man.read_lock(0,"key0")
    lock_man.read_lock(1,"key0")
    ret = lock_man.is_write_lock_available(2,"key0")
    print(f"ret should be false: {ret}")
    lock_man.unlock_all_locks_for_transaction(0)
    lock_man.unlock_all_locks_for_transaction(1)
    ret = lock_man.is_write_lock_available(2,"key0")
    print(f"ret should be true: {ret}")
    lock_man.write_lock(2, "key0")
    ret = lock_man.is_write_lock_available(1,"key0")
    print(f"ret should be false: {ret}")
    ret = lock_man.is_read_lock_available(0,"key0")
    print(f"ret should be false: {ret}")
    lock_man.unlock_all_locks_for_transaction(2)
    ret = lock_man.is_write_lock_available(1,"key0")
    print(f"ret should be true: {ret}")
    lock_man.write_lock(1, "key0")
    ret = lock_man.is_write_lock_available(0,"key0")
    print(f"ret should be false: {ret}")


    
if __name__ == "__main__":
    main()
