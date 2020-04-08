# main2.py
import argparse
from common import *
from logger import Logger
from scheduler import Scheduler
import sys

parser = argparse.ArgumentParser()
parser.add_argument("instruction_file", help="path to instruction file", type=str, nargs='+')
parser.add_argument("-f", "--log_file", help="path to log", type=str, default="log.log")
args = parser.parse_args()

Logger.initialize(args.log_file)

# instruction_file is a list of filenames
scheduler = Scheduler(args.instruction_file)
#scheduler.run()
