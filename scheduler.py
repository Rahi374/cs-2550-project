from common import *
from core import Core

class Scheduler:

    def __init__(self, instruction_files):
        # load/read instructions from instruction files
        self.instruction_sequences = self.load_sequences_from_files(instruction_files)
        return

    def run(self):
        # TODO set these parameters for LSM
        self.core = Core(ORG.LSM, args.mem_size, args.block_size, None if args.blocks_per_ss is None else args.blocks_per_ss)

        # main loop
        start = time.time()
        while has_instructions:
            inst = self.get_next_instruction()

            if self.can_run(inst):
                Logger.log(inst.to_log())
                try:
                    ret = core.exec_inst(inst)
                    core.print_result(inst, ret)
                except Exception as e:
                    ret = e

        done = time.time()
        core.disk.kill_all_compaction_threads()
        Logger.write_log()

        return
