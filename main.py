# main.py
from common import *
from core import Core
from inst import Instruction
from parser import Parser
from record import Record
import sys

def print_usage():
    print("python3 main.py <instruction_file> <disk_org> <mem_size> <block_size> <blocks_per_ss>")
    print("    instruction_file: instruction file")
    print("    disk_org: SEQ|LSM")
    print("    mem_size: size of memory (in bytes)")
    print("    block_size: size of disk blocks (in bytes)")
    print("    blocks_per_ss: number of blocks in each SST")

if len(sys.argv) < 5:
    print_usage()
    sys.exit()

instr_file = sys.argv[1]
disk_org = (ORG.SEQ if sys.argv[2] == "SEQ" else ORG.LSM)
mem_size = sys.argv[3]
block_size = sys.argv[4]
blocks_per_ss = sys.argv[5] if disk_org == ORG.LSM else None

if disk_org == ORG.SEQ and (int(mem_size) < int(block_size) or int(mem_size) % int(block_size) != 0):
    print(f"mem_size must be multiple of block_size")
    sys.exit()

if disk_org == ORG.LSM and int(mem_size) % (int(block_size) * int(blocks_per_ss)) != 0:
    print(f"mem_size must be multiple of {int(block_size) * int(blocks_per_ss)}")
    sys.exit()

real_insts = Parser.parse(instr_file)

core = Core(disk_org, int(mem_size), int(block_size), None if blocks_per_ss is None else int(blocks_per_ss))
core.run(real_insts)
