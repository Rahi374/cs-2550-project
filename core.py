from common import *
from enum import Enum

class Core():
    mem = None
    disk = None

    def __init__(self, disk_org: ORG, mem_size: int, block_size: int):
        disk = create_storage(block_size, disk_org)
        mem = create_memory(disk, mem_size, block_size, disk_org)

    def create_storage(self, block_size: int, disk_org: ORG):
        if disk_org == ORG.SEQ:
            return Storage(block_size)
        elif disk_org == ORG.LSM:
            return Storage(block_size)

    def create_memory(self, disk, mem_size: int, block_size: int, disk_org: ORG):
        if disk_org == ORG.SEQ:
            return Mem(disk, mem_size, block_size)
        elif disk_org == ORG.LSM:
            return Mem(disk, mem_size, block_size)


    def run(self, insts: list):
        for inst in insts:
            self.exec_inst(inst)

    def exec_inst(self, inst: Instruction):
        if inst.action == ACTION.RETRIEVE_BY_ID:
            self.read_id(inst.table_name, inst.record_id)

        elif inst.action == ACTION.RETRIEVE_BY_AREA_CODE:
            self.read_area_code(inst.table_name, inst.record_id)

        elif inst.action == ACTION.WRITE_RECORD:
            self.write(inst.table_name, inst.tuple_data)

        elif inst.action == ACTION.DELETE_TABLE:
            self.delete(inst.table_name)

    def read_id(self, table: str, rec_id: int):
        return mem.retrieve_rec(table=table, rec_id=rec_id, field="id", is_primary=True)

    def read_area_code(self, table: str, area_code: int):
        return mem.retrieve_rec(table=table, rec_id=area_code, field="area_code", is_primary=False)

    def write(self, table: str, record: tuple):
        if not self.table_exists(table):
            self.create_table(table)
        mem.write_rec(table, record)

    def delete(self, table: str):
        disk.delete_table(table)

    def create_table(self, table: str):
        disk.create_table(table)

    def table_exists(self, table: str):
        disk.table_exists(table)
