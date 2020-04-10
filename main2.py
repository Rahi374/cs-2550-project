# main2.py
import argparse
from common import *
from logger import Logger
from scheduler import Scheduler
import sys

parser = argparse.ArgumentParser()
parser.add_argument("scheduling_type", help="rr or random", type=str)
parser.add_argument("instruction_file", help="path to instruction file", type=str, nargs='+')
parser.add_argument("-f", "--log_file", help="path to log", type=str, default="log.log")
parser.add_argument("-m", "--mem_size", help="size of memory (in bytes), must be multiple of block_size and blocks_per_ss, default = 544", type=int, default=544)
parser.add_argument("-b", "--block_size", help="size of blocks, default = 68", type=int, default=68)
parser.add_argument("-s", "--blocks_per_ss", help="number of blocks per SS, default = 2", type=int, default=2)
args = parser.parse_args()

Logger.initialize(args.log_file)

if args.scheduling_type == "rr":
    sched_type = SCHED_TYPE.RR
elif args.scheduling_type == "random":
    sched_type = SCHED_TYPE.RAND
else:
    print("scheduling_type must be rr or random")
    sys.exit(1)

# instruction_file is a list of filenames
scheduler = Scheduler(args.instruction_file)
#scheduler.run()
