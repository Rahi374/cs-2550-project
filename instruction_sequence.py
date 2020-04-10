from common import *
from inst import Instruction

class InstructionSequence():

    instruction_sequence = []
    pc = 0
    exec_type = None

    def __init__(self, instruction_array, exec_type):
        self.instruction_sequence = instruction_array
        self.pc = 0
        self.exec_type = exec_type

    def __len__(self):
        return len(self.instruction_sequence)

    def fetch(self):
        return self.instruction_sequence[self.pc]

    def exec(self):
        self.pc += 1

    def completed(self):
        return self.pc >= len(self)
