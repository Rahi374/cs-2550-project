import networkx as nx
import sys

class lock_manager(object):
    locks = {}
    trans_id_to_locks = {}#trans_id -> Set of locks
    

    def __init__(self):
        return


    def print_locks(self):
        print(self.locks.keys())
        for l in self.locks.keys():
            print(self.locks[l])
    
    #table-level locking
    def is_table_read_lock_available(self, trans_id, table_name):
        return self.is_read_lock_available(trans_id, table_name)

    def is_table_write_lock_available(self, trans_id, table_name):
        return self.is_write_lock_available(trans_id, table_name)

    def table_read_lock(self, trans_id, table_name):
        self.read_lock(trans_id, table_name)

    def table_write_lock(self, trans_id, table_name):
        self.write_lock(trans_id, table_name)

    #tuple-level locking 
    def is_tuple_read_lock_available(self, trans_id, tuple_id, table_name):
        return self.is_read_lock_available(trans_id, table_name+"@"+str(tuple_id))


    def is_tuple_write_lock_available(self, trans_id, tuple_id, table_name):
        return self.is_write_lock_available(trans_id, table_name+"@"+str(tuple_id))


    def tuple_read_lock(self, trans_id, tuple_id, table_name):
        self.read_lock(trans_id, table_name+"@"+str(tuple_id))


    def tuple_write_lock(self, trans_id, tuple_id, table_name):
        self.write_lock(trans_id, table_name+"@"+str(tuple_id))


    #generic methods used by tables and tuples for locking
    def is_read_lock_available(self, trans_id, key):
        lock = None
        if key not in self.locks:
            lock = ReadWriteLock(key)
            self.locks[key] = lock
        else:
            lock = self.locks[key]
        

        if trans_id not in self.trans_id_to_locks:
            self.trans_id_to_locks[trans_id] = set()
        self.trans_id_to_locks[trans_id].add(lock)

        if lock.is_trans_id_in_current_owners(trans_id):
            return True
        
        if lock.has_write_owner:
            if not lock.is_trans_id_in_queue(trans_id):
                lock.enqueue_trans(trans_id, "r")
            return False
        else:
            next_in_line = lock.peek_queue()
            if next_in_line is not None and next_in_line[0] == trans_id:
                return True
            else:
                if not lock.is_trans_id_in_queue(trans_id):
                    lock.enqueue_trans(trans_id, "r")

                next_in_line = lock.peek_queue()
                if not lock.has_write_owner and next_in_line is not None and next_in_line[0] == trans_id:
                    return True
                return False 
    

    def is_write_lock_available(self, trans_id, key):
        lock = None
        if key not in self.locks:
            lock = ReadWriteLock(key)
            self.locks[key] = lock
        else:
            lock = self.locks[key]

        if trans_id not in self.trans_id_to_locks:
            self.trans_id_to_locks[trans_id] = set()
        self.trans_id_to_locks[trans_id].add(lock)

        if lock.is_trans_id_write_owner(trans_id):
            return True
        
        # check if nobody already owns the lock
        if len(lock.lock_owners) == 0:
            next_in_line = lock.peek_queue()
            if next_in_line is not None and next_in_line[0] == trans_id:
                return True

        #check if you are the only read owner and nobody is before you in queue
        if len(lock.lock_owners) == 1 and lock.lock_owners[0][0] == trans_id and len(lock.waiting_on_locks) == 0:
                lock.enqueue_trans(trans_id, "w")
                return True
        # you are the only read owner and you are at the front of the queue
        elif len(lock.lock_owners) == 1 and lock.lock_owners[0][0] == trans_id and lock.waiting_on_locks[0][0] == trans_id:
                return True
        # join the queue for the lock
        elif not lock.is_trans_id_in_queue(trans_id):
            lock.enqueue_trans(trans_id, "w")
            next_in_line = lock.peek_queue()
            if len(lock.lock_owners) == 0 and next_in_line is not None and next_in_line[0] == trans_id:
                return True

        return False
   

    def read_lock(self, trans_id, key):
        if key not in self.locks:
            print("error: tried to acquire lock when it didn't exist")
            raise Exception
        
        lock = self.locks[key]
        if lock.is_trans_id_in_current_owners(trans_id):
            return

        next_in_line = lock.peek_queue()
        if next_in_line[0] != trans_id:
            print("error: tried to acquire lock when you were not next in line")
            raise Exception
    
        lock.acquire_lock_by_next_in_line()


    def write_lock(self, trans_id, key):
        if key not in self.locks:
            print("error: tried to acquire lock when it didn't exist")
            raise Exception

        lock = self.locks[key]
        if lock.is_trans_id_write_owner(trans_id):
            return
        lock.release_acquired_read_lock_by_transaction(trans_id)

        next_in_line = lock.peek_queue()
        if next_in_line[0] != trans_id:
            print("error: tried to acquire lock when you were not next in line")
            raise Exception
        lock.acquire_lock_by_next_in_line()


   #transaction management
    def unlock_all_locks_for_transaction(self, trans_id):
        if trans_id not in self.trans_id_to_locks:
            return
        set_of_locks = self.trans_id_to_locks[trans_id]
        for l in set_of_locks:
            l.release_lock_by_transaction(trans_id)
        del self.trans_id_to_locks[trans_id]


    def print_wait_graph(self):
        return


    def detect_deadlock(self):
        graph = nx.DiGraph()
        for trans_id, lock_list in self.trans_id_to_locks.items():
            for lock in lock_list:

                if (lock.does_trans_id_want_lock(trans_id, 'r') and lock.is_locked('w')) or \
                   (lock.does_trans_id_want_lock(trans_id, 'w') and lock.is_locked('w')):
                       if lock.lock_owners[0][0] != trans_id:
                           graph.add_edge(trans_id, lock.lock_owners[0][0])

                elif lock.does_trans_id_want_lock(trans_id, 'r') and lock.is_locked('r'):
                    if "@" not in lock.lock_key and lock.lock_owners[0][0] != trans_id:
                        graph.add_edge(trans_id, lock.lock_owners[0][0])

                elif lock.does_trans_id_want_lock(trans_id, 'w') and lock.is_locked('r'):
                    for target_owner in lock.lock_owners:
                        if target_owner[0] != trans_id:
                            graph.add_edge(trans_id, target_owner[0])
                    # if i locked as read, first queued is someone else for write, and i want write
                    if trans_id in [x[0] for x in lock.lock_owners] and lock.waiting_on_locks[0][0] != trans_id:
                        graph.add_edge(trans_id, trans_id)

        cycles = list(nx.algorithms.cycles.simple_cycles(graph))
        if len(cycles) != 0:
            print(f"DEADLOCK DETECTED: {cycles}")
            return True
        return False


class ReadWriteLock(object):
    def __init__(self, key):
        self.lock_key = key
        self.lock_owners = [] #can potentially be a list of multiple read owners for one write owner
        self.waiting_on_locks = [] #entries are [trans_id, "r"/"w"]
        self.has_write_owner = False
        return

    def trans_id_position_in_queue(self, trans_id, type):
        i = 0
        for lock in self.waiting_on_locks:
            if lock[0] == trans_id and lock[1] == type:
                return i
            i += 1
        return -1

    # type = 'r'/'w'
    def does_trans_id_want_lock(self, trans_id, type):
        for lock in self.waiting_on_locks:
            if lock[0] == trans_id and lock[1] == type:
                return True
        return False

    # type = 'r'/'w'
    def is_locked(self, type):
        if len(self.lock_owners) == 0:
            return False
        if type == 'w' and self.has_write_owner:
            return True
        if type == 'r':
            return True
        return False

    def is_trans_id_in_current_owners(self, trans_id):
        for o in self.lock_owners:
            if o[0] == trans_id:
                return True
        return False


    def is_trans_id_write_owner(self, trans_id):
        if len(self.lock_owners) == 1:
            if self.lock_owners[0][0] == trans_id and self.lock_owners[0][1] == "w":
                return True
        return False


    def is_trans_id_in_queue(self, trans_id):
        for o in self.waiting_on_locks:
            if o[0] == trans_id:
                return True
        return False
        

    def enqueue_trans(self, trans_id, read_or_write):
        self.waiting_on_locks.append([trans_id, read_or_write])


    def peek_queue(self):
        if len(self.waiting_on_locks) > 0:
            return self.waiting_on_locks[0]
        else:
            return None

    def acquire_lock_by_next_in_line(self):
        if self.waiting_on_locks[0][1] == "w":
            self.has_write_owner = True
        self.lock_owners.append(self.waiting_on_locks[0])
        del self.waiting_on_locks[0]
    
    def release_lock_by_transaction(self, trans_id):
        if len(self.lock_owners) == 1 and self.lock_owners[0][0] == trans_id and self.lock_owners[0][1] == "w":
            self.has_write_owner = False
        self.lock_owners = [t for t in self.lock_owners if t[0] != trans_id]
        self.waiting_on_locks = [t for t in self.waiting_on_locks if t[0] != trans_id]

    def release_acquired_read_lock_by_transaction(self, trans_id):
        self.lock_owners = [t for t in self.lock_owners if t[0] != trans_id]

