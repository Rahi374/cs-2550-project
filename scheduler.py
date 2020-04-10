from common import *
from core import Core
from inst import Instruction
from instruction_sequence import InstructionSequence
from logger import Logger
from parser import Parser
import random

class Scheduler:

    instruction_sequence_sequences = []
    sched_type = None
    seq_pc = 0
    core = None

    # TODO use the log
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
        return

    def run_inst(self, inst):
       Logger.log(inst.to_log())
       try:
           ret = self.core.exec_inst(inst)
           self.core.print_result(inst, ret)
       except Exception as e:
           ret = e

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

    def can_acquire_locks(self, t_id, inst):
        if inst.action == ACTION.RETRIEVE_BY_ID:
            return True
        if inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            return True
        if inst.action == ACTION.WRITE_RECORD:
            return True
        if inst.action == ACTION.DELETE_RECORD:
            return True
        if inst.action == ACTION.DELETE_TABLE:
            return True
        if inst.action == ACTION.ABORT:
            return False
        if inst.action == ACTION.COMMIT:
            return True
        return None

    def do_acquire_lock(self, t_id, inst):
        if inst.action == ACTION.RETRIEVE_BY_ID:
            return True
        if inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            return True
        if inst.action == ACTION.WRITE_RECORD:
            return True
        if inst.action == ACTION.DELETE_RECORD:
            return True
        if inst.action == ACTION.DELETE_TABLE:
            return True
        if inst.action == ACTION.ABORT:
            return False
        if inst.action == ACTION.COMMIT:
            return True
        return None

    def run(self):
        self.core = Core(ORG.LSM, self.mem_size, self.block_size, self.blocks_per_ss)

        while len(self.instruction_sequence_sequences) != 0:
            next_inst_seq = self.next_inst_seq()
            inst = next_inst_seq.fetch()

            if next_inst_seq.exec_type == EXEC_TYPE.TRANSACTION:
                if self.can_acquire_locks(self.seq_pc, inst):
                    self.do_acquire_lock(self.seq_pc, inst)
                else:
                    self.incr_seq_pc()
                    continue

            self.run_inst(inst)
            next_inst_seq.exec()

            # remove completed instruction sequences
            if next_inst_seq.completed():
                self.remove_current_seq()

            self.incr_seq_pc()

        self.core.disk.kill_all_compaction_threads()
        Logger.write_log()
