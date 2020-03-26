from common import *
from enum import Enum
from inst import Instruction
from mem_seq import MemSeq
from record import Record
from storage import Storage

class Core():

    def __init__(self, disk_org: ORG, mem_size: int, block_size: int):
        self.disk = self.create_storage(block_size, disk_org)
        self.mem = self.create_memory(mem_size, block_size, self.disk, disk_org)

    def create_storage(self, block_size: int, disk_org: ORG):
        if disk_org == ORG.SEQ:
            return Storage(disk_org, block_size)
        elif disk_org == ORG.LSM:
            return Storage(disk_org, block_size)

    def create_memory(self, mem_size: int, block_size: int, disk: Storage, disk_org: ORG):
        if disk_org == ORG.SEQ:
            return MemSeq(mem_size, block_size, disk, disk_org)
        elif disk_org == ORG.LSM:
            return Mem(disk, mem_size, block_size)


    def run(self, insts: list):
        for inst in insts:
            print(f"Executing: {str(inst)}")
            try:
                ret = self.exec_inst(inst)
            except Exception as e:
                print(e)
            print(f"Returned: {ret}")
        self.mem.print_cache()
        self.mem.flush()

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
        return self.mem.retrieve_rec(table=table, rec_id=rec_id, field_name="id", is_primary=True)

    def read_area_code(self, table: str, area_code: int):
        return self.mem.retrieve_rec(table=table, rec_id=area_code, field_name="area_code", is_primary=False)

    def write(self, table: str, record: Record):
        if not self.table_exists(table):
            self.create_table(table)
        self.mem.write_rec(table, record)

    def delete_record(self, table: str, rec_id: int):
        self.mem.delete_rec(table, rec_id)

    def delete_table(self, table: str):
        self.mem.delete_table_in_mem(table)
        self.disk.delete_table(table)

    def create_table(self, table: str):
        self.disk.create_table(table)

    def table_exists(self, table: str):
        return self.disk.table_exists(table)
