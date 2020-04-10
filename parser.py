from common import *
from inst import Instruction
from instruction_sequence import InstructionSequence
from record import Record
import sys

class Parser():
    @staticmethod
    def parse(inst_file, create_transaction=False):
        instructions = []
        if create_transaction:
            transactions = []
            cur_transaction_mode = EXEC_TYPE.PROCESS

        f = open(inst_file, 'r')
        lines = f.readlines()
        f.close()
        line_num = 1
        for line in lines:
            if line[0] == "#" or line[0] == '\n':
                continue
            split_line = line.split()
            action = split_line[0]
            if line[0] != "B" and line[0] != "C" and line[0] != "A":
                table_name = split_line[1]
                data = split_line[2] if len(split_line) > 2 else None
            inst = None
            if create_transaction and action == 'B':
                if int(split_line[1]) == 0:
                    cur_transaction_mode = EXEC_TYPE.PROCESS
                elif int(split_line[1]) == 1:
                    cur_transaction_mode = EXEC_TYPE.TRANSACTION
                else:
                    print(f"B can only take 0 or 1, in file {inst_file} on line {line_num}")
                    sys.exit(1)
                instructions = []
            elif create_transaction and action == 'A':
                instructions.append(Instruction(ACTION.ABORT))
                transactions.append(InstructionSequence(instructions, cur_transaction_mode))
                instructions = []
            elif create_transaction and action == 'C':
                instructions.append(Instruction(ACTION.COMMIT))
                transactions.append(InstructionSequence(instructions, cur_transaction_mode))
                instructions = []
            elif action == 'R':
                inst = Instruction(ACTION.RETRIEVE_BY_ID, table_name, int(data))
            elif action == 'M':
                inst = Instruction(ACTION.RETRIEVE_BY_AREA_CODE, table_name, int(data))
            elif action == 'W':
                data = " ".join(split_line[2:]).strip('()').split(",")
                inst = Instruction(ACTION.WRITE_RECORD, table_name, Record(int(data[0]), data[1].strip(), data[2].strip()))
            elif action == 'E':
                inst = Instruction(ACTION.DELETE_RECORD, table_name, int(data))
            elif action == 'D':
                inst = Instruction(ACTION.DELETE_TABLE, table_name)
            else:
                print(f"Malformed Input File on Line {line_num}: {line}")
                sys.exit(1)
            if inst is not None:
                instructions.append(inst)
            line_num += 1

        if create_transaction:
            return transactions
        return instructions
