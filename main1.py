# main1.py
import argparse
from common import *
from core import Core
from inst import Instruction
from logger import Logger
from parser import Parser
from record import Record
import sys

parser = argparse.ArgumentParser()
parser.add_argument("instruction_file", help="path to instruction file", type=str)
parser.add_argument("disk_org", help="disk organization: SEQ or LSM", type=str)
parser.add_argument("mem_size",
                    help="size of memory in bytes.\n"
                         "If disk_org is LSM, mem_size must be a multiple of block_size * blocks_per_ss.\n"
                         "If disk_org is ORG, mem_size must be a multiple of block_size",
                    type=int)
parser.add_argument("block_size", help="size of disk blocks in bytes", type=int)
parser.add_argument("-bps", "--blocks_per_ss", help="number of blocks in each SST. Must be specified if disk_org is LSM", type=int)
parser.add_argument("-f", "--log_file", help="path to log", type=str, default="log.log")
args = parser.parse_args()

if args.disk_org == "SEQ":
    disk_org = ORG.SEQ
elif args.disk_org == "LSM":
    disk_org = ORG.LSM
else:
    print("invalid disk_org")
    sys.exit()

if disk_org == ORG.LSM and args.blocks_per_ss is None:
    print("block_per_ss must be specified if disk_org is LSM")
    sys.exit()

if disk_org == ORG.SEQ and (args.mem_size < args.block_size or args.mem_size % args.block_size != 0):
    print(f"mem_size must be multiple of block_size")
    sys.exit()

if disk_org == ORG.LSM and args.mem_size % (args.block_size * args.blocks_per_ss) != 0:
    print(f"mem_size must be multiple of {args.block_size * args.blocks_per_ss}")
    sys.exit()

Logger.initialize(args.log_file)

real_insts = Parser.parse(args.instruction_file)

core = Core(disk_org, args.mem_size, args.block_size, None if args.blocks_per_ss is None else args.blocks_per_ss)
core.run(real_insts)
