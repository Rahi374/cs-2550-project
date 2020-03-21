# main.py
from common import *
import sys

def print_usage():
    print("python3 main.py <instruction_file> <disk_org> <mem_size> <block_size>")
    print("    instruction_file: instruction file")
    print("    disk_org: SEQ|LSM")
    print("    mem_size: size of memory (in bytes)")
    print("    block_size: size of disk blocks (in bytes)")

if len(sys.argv) != 5:
    print_usage()
    sys.exit()

instr_file = sys.argv[1]
disk_org = (ORG.SEQ if sys.argv[2] == "SEQ" else ORG.LSM)
mem_size = sys.argv[3]
block_size = sys.argv[4]

#insts = parser.parse(instr_file)
insts = [
        Instruction(ACTION.RETRIEVE_BY_ID,        "X", 13),
        Instruction(ACTION.RETRIEVE_BY_ID,        "Y", 7),
        Instruction(ACTION.WRITE_RECORD,          "Y", (5, "John", "412-111-2222")),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "Y", 609),
        Instruction(ACTION.WRITE_RECORD,          "X", (2, "Thalia", "412-656-2212")),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "X", 412),
        Instruction(ACTION.DELETE_TABLE,          "Y"),
        ]

core = Core(disk_org, mem_size, block_size)
core.run(insts)
