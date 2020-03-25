# main.py
from common import *
from core import Core
from inst import Instruction
from parser import Parser
from record import Record
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

real_insts = Parser.parse(instr_file)
insts = [
        Instruction(ACTION.RETRIEVE_BY_ID,        "X", 13),
        Instruction(ACTION.RETRIEVE_BY_ID,        "Y", 7),
        Instruction(ACTION.WRITE_RECORD,          "Y", Record(5, "John", "412-111-2222")),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "Y", 609),
        Instruction(ACTION.WRITE_RECORD,          "X", Record(2, "Thalia", "412-656-2212")),
        Instruction(ACTION.WRITE_RECORD,          "X", Record(3, "Bin", "412-555-2121")),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "X", 412),
        #Instruction(ACTION.DELETE_TABLE,          "Y"),
        ]

insts2 = [
        Instruction(ACTION.WRITE_RECORD,          "X", Record(1, "Alex", "412-111-1111")),
        Instruction(ACTION.WRITE_RECORD,          "X", Record(2, "Bob", "412-222-2222")),
        Instruction(ACTION.RETRIEVE_BY_ID,        "X", 1),
        #Instruction(ACTION.DELETE_RECORD,         "Y", 5),
        Instruction(ACTION.WRITE_RECORD,          "Y", Record(3, "Bin", "412-555-2121")),
        Instruction(ACTION.DELETE_TABLE,          "Y"),
        Instruction(ACTION.RETRIEVE_BY_AREA_CODE, "X", 412),
]

core = Core(disk_org, int(mem_size), int(block_size))
core.run(real_insts)
