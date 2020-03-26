from common import ACTION
from inst import Instruction
from record import Record
import sys

class Parser():
    @staticmethod
    def parse(inst_file):
        instructions = []
        f = open(inst_file, 'r')
        lines = f.readlines()
        line_num = 1
        for line in lines:
            if line[0] == "#":
                instructions.append(line.strip())
                line_num +=1
                continue
            split_line = line.split()
            action = split_line[0]
            table_name = split_line[1]
            data = split_line[2] if len(split_line) > 2 else None
            inst = None
            if action == 'R':
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
                print("Malformed Input File on Line "+line_num+": "+line)
                sys.exit()
            instructions.append(inst)
            line_num += 1
        return instructions
