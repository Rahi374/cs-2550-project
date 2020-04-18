from common import *
from core import Core
from inst import Instruction
from instruction_sequence import InstructionSequence
from lock_manager import *
from logger import Logger
from parser import Parser
import random

class Scheduler:

    instruction_sequence_sequences = []
    sched_type = None
    seq_pc = 0
    core = None
    lock_manager = None
    log = []

    def load_sequences_from_files(self, instruction_files):
        ret = []
        for filename in instruction_files:
            seqs = Parser.parse(filename, True)
            ret.append(seqs)

        return ret

    def __init__(self, sched_type, instruction_files, mem_size, block_size, blocks_per_ss):
        self.instruction_sequence_sequences = self.load_sequences_from_files(instruction_files)
        self.sched_type = sched_type
        self.seq_pc = 0
        self.mem_size = mem_size
        self.block_size = block_size
        self.blocks_per_ss = blocks_per_ss
        self.lock_manager = lock_manager()
        self.log = []
        return

    def run_inst(self, inst, t_id):
        Logger.log(inst.to_log())
        print(inst)
        log_entry, ret = self.core.exec_inst_phase2(inst, t_id)
        if log_entry is None:
            return
        self.core.print_result(inst, ret)
        self.log.append(log_entry)

    def next_inst_seq(self):
        return self.instruction_sequence_sequences[self.seq_pc][0]

    def remove_current_seq(self):
        del self.instruction_sequence_sequences[self.seq_pc][0]
        if len(self.instruction_sequence_sequences[self.seq_pc]) == 0:
            del self.instruction_sequence_sequences[self.seq_pc]

    def incr_seq_pc(self):
        if self.sched_type == SCHED_TYPE.RR:
            try:
                self.seq_pc = (self.seq_pc + 1) % len(self.instruction_sequence_sequences)
            except ZeroDivisionError:
                return
        elif self.sched_type == SCHED_TYPE.RAND:
            self.seq_pc = random.randrange(len(self.instruction_sequence_sequences))

    def reset_seq_pc(self):
        if self.seq_pc >= len(self.instruction_sequence_sequences):
            self.seq_pc = len(self.instruction_sequence_sequences) - 1

    def do_undos(self, t_id: int):
        for log_entry in reversed(self.log):
            inst = log_entry.inst
            if log_entry.t_id == t_id:

                if inst.action == ACTION.WRITE_RECORD:
                    if len(log_entry.before_image) == 0:
                         self.core.delete_record(log_entry.inst.table_name, log_entry.inst.tuple_data.id)
                    else:
                         self.core.write(log_entry.inst.table_name, log_entry.before_image[0])

                elif inst.action == ACTION.DELETE_RECORD:
                    if len(log_entry.before_image) == 0:
                        continue
                    self.core.write(log_entry.inst.table_name, log_entry.before_image[0])

                elif inst.action == ACTION.DELETE_TABLE:
                    if log_entry.before_image == None:
                        continue
                    self.core.disk.restore_table(log_entry.before_image)

    def can_acquire_locks(self, t_id, inst):
        if inst.action == ACTION.RETRIEVE_BY_ID:
            b = self.lock_manager.is_tuple_read_lock_available(t_id, inst.record_id, inst.table_name)
            if b is False:
                return b
            a = self.lock_manager.is_table_read_lock_available(t_id, inst.table_name)
            return a and b
        if inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            return self.lock_manager.is_table_write_lock_available(t_id, inst.table_name)
        if inst.action == ACTION.WRITE_RECORD:
            b = self.lock_manager.is_tuple_write_lock_available(t_id, inst.tuple_data.id, inst.table_name)
            if b is False:
                return b
            a = self.lock_manager.is_table_read_lock_available(t_id, inst.table_name)
            return a and b
        if inst.action == ACTION.DELETE_RECORD:
            b = self.lock_manager.is_tuple_write_lock_available(t_id, inst.record_id, inst.table_name)
            if b is False:
                return b
            a = self.lock_manager.is_table_read_lock_available(t_id, inst.table_name)
            return a and b
        if inst.action == ACTION.DELETE_TABLE:
            return self.lock_manager.is_table_write_lock_available(t_id, inst.table_name)
        if inst.action == ACTION.ABORT:
            return True
        if inst.action == ACTION.COMMIT:
            return True
        return None

    def do_lock_stuff(self, t_id, inst):
        if inst.action == ACTION.RETRIEVE_BY_ID:
            self.lock_manager.table_read_lock(t_id, inst.table_name)
            self.lock_manager.tuple_read_lock(t_id, inst.record_id, inst.table_name)
        elif inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            self.lock_manager.table_write_lock(t_id, inst.table_name)
        elif inst.action == ACTION.WRITE_RECORD:
            self.lock_manager.table_read_lock(t_id, inst.table_name)
            self.lock_manager.tuple_write_lock(t_id, inst.tuple_data.id, inst.table_name)
        elif inst.action == ACTION.DELETE_RECORD:
            self.lock_manager.table_read_lock(t_id, inst.table_name)
            self.lock_manager.tuple_write_lock(t_id, inst.record_id, inst.table_name)
        elif inst.action == ACTION.DELETE_TABLE:
            self.lock_manager.table_write_lock(t_id, inst.table_name)
        elif inst.action == ACTION.ABORT:
            self.lock_manager.unlock_all_locks_for_transaction(t_id)
            self.do_undos(t_id)
        elif inst.action == ACTION.COMMIT:
            self.lock_manager.unlock_all_locks_for_transaction(t_id)

    def run(self):
        self.core = Core(ORG.LSM, self.mem_size, self.block_size, self.blocks_per_ss)
        for seq_seq in self.instruction_sequence_sequences:
            print([id(seq) for seq in seq_seq])

        while len(self.instruction_sequence_sequences) != 0:
            next_inst_seq = self.next_inst_seq()
            inst = next_inst_seq.fetch()
            Logger.trans_id =id(next_inst_seq)

            if next_inst_seq.exec_type == EXEC_TYPE.TRANSACTION:

                can_acquire_lock = self.can_acquire_locks(id(next_inst_seq), inst)
                if (self.lock_manager.detect_deadlock()):
                    print(f"terminating {id(next_inst_seq)}")
                    inst = Instruction(ACTION.ABORT)
                    self.do_lock_stuff(id(next_inst_seq), inst)
                    self.remove_current_seq()
                    self.reset_seq_pc()
                    continue

                if can_acquire_lock:
                    self.do_lock_stuff(id(next_inst_seq), inst)
                else:
                    self.incr_seq_pc()
                    continue

            self.run_inst(inst, id(next_inst_seq))
            next_inst_seq.exec()

            # remove completed instruction sequences
            if next_inst_seq.completed():
                self.remove_current_seq()
                self.reset_seq_pc()
                continue

            self.incr_seq_pc()

        self.core.mem.print_cache()
        self.core.mem.flush()
        self.core.disk.kill_all_compaction_threads()
        Logger.write_log()
