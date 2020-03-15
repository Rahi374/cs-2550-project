import ACTION  
import sys
class Parser():
    @staticmethod
    def parse(inst_file):
        instructions = []
        f = open(inst_file, 'r')
        lines = f.readlines()
        line_num = 1
        for line in lines:
            split_line - line.split()
            action = split_line[0]
            table_name = split_line[1]
            data = split_line[2]
            inst = None
            if action == 'R':
                inst = Instruction(ACTION.RETRIEVE_BY_ID, table_name, data)
            elif action == 'M':
                inst = Instruction(ACTION.RETRIEVE_BY_AREA_CODE, table_name, data)
            elif action == 'W':
                inst = Instruction(ACTION.WRITE_RECORD, table_name, data)
            elif action == 'D':
                inst = Instruction(ACTION.DELETE_TABLE, table_name, data)
            else:
                print("Malformed Input File on Line "+line_num+": "+line)
              sys.exit()
            instructions.append(inst)
            line_num += 1
        return instructions
