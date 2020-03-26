from common import *
from enum import Enum
from inst import Instruction
import math
from mem_lsm import MemLSM
from mem_seq import MemSeq
from record import Record
from storage import Storage
from LSMStorage import LSMStorage

class Core():

    def __init__(self, disk_org: ORG, mem_size: int, block_size: int, blocks_per_ss: int):
        self.disk = self.create_storage(block_size, blocks_per_ss, disk_org)
        self.mem = self.create_memory(mem_size, block_size, blocks_per_ss, self.disk, disk_org)
        self.disk_org = disk_org

    def create_storage(self, block_size: int, blocks_per_ss: int, disk_org: ORG):
        if disk_org == ORG.SEQ:
            return Storage(disk_org, block_size)
        elif disk_org == ORG.LSM:
            return LSMStorage(block_size, blocks_per_ss)

    def create_memory(self, mem_size: int, block_size: int, blocks_per_ss: int, disk: Storage, disk_org: ORG):
        if disk_org == ORG.SEQ:
            return MemSeq(mem_size, block_size, disk, disk_org)
        elif disk_org == ORG.LSM:
            # mem_size = blocks_per_ss * lru_size * block_size
            return MemLSM(disk, block_size, blocks_per_ss, math.floor(mem_size/(blocks_per_ss * block_size)))


    def run(self, insts: list):
        for inst in insts:
            print(f"Executing: {str(inst)}")
            try:
                ret = self.exec_inst(inst)
            except Exception as e:
                ret = e
            print(f"=> {ret}")
        self.mem.print_cache()
        self.mem.flush()
        if disk_org == ORG.LSM:
            self.storage.kill_all_compaction_threads()

    def exec_inst(self, inst: Instruction):
        if isinstance(inst, str):
            print(inst)
            return

        if inst.action == ACTION.RETRIEVE_BY_ID:
            return self.read_id(inst.table_name, inst.record_id)

        if inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            return self.read_area_code(inst.table_name, inst.record_id)

        if inst.action == ACTION.WRITE_RECORD:
            return self.write(inst.table_name, inst.tuple_data)

        if inst.action == ACTION.DELETE_RECORD:
            return self.delete_record(inst.table_name, inst.record_id)

        if inst.action == ACTION.DELETE_TABLE:
            return self.delete_table(inst.table_name)

    def read_id(self, table: str, rec_id: int):
        if self.disk_org == ORG.SEQ:
            ret = self.mem.retrieve_rec(table=table, rec_id=rec_id, field_name="id", is_primary=True)
        elif self.disk_org == ORG.LSM:
            ret = self.mem.read_rec(table, str(rec_id))
            ret = [] if ret is None else [ret]
        return ret

    def read_area_code(self, table: str, area_code: int):
        if self.disk_org == ORG.SEQ:
            ret = self.mem.retrieve_rec(table=table, rec_id=area_code, field_name="area_code", is_primary=False)
        elif self.disk_org == ORG.LSM:
            ret = self.mem.read_recs(table, str(area_code))
        return ret

    def write(self, table: str, record: Record):
        if not self.table_exists(table) and self.disk_org == ORG.SEQ:
            self.create_table(table)

        if self.disk_org == ORG.LSM:
            self.mem.write_rec(table, record)
        elif self.disk_org == ORG.SEQ:
            res = self.read_id(table, record.id)
            if len(res) == 0:
                self.mem.write_rec(table, record)
            elif len(res) == 1:
                self.mem.update_rec(table, record)
            else:
                raise Exception("multiple records with same id")

    def delete_record(self, table: str, rec_id: int):
        self.mem.delete_rec(table, rec_id)

    def delete_table(self, table: str):
        self.mem.delete_table(table)
        self.disk.delete_table(table)

    def create_table(self, table: str):
        self.disk.create_table(table)

    def table_exists(self, table: str):
        return self.disk.table_exists(table)
