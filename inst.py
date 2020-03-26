# inst.py
from common import *

class Instruction():

    def __init__(self, action: ACTION, table_name: str, data=None):
        """
        data: either tuple_data or record_id
            - tuple_data: (int id, string client_name, string phone)
            - record_id: int id
        """
        self.action = action
        self.table_name = table_name

        if self.action == ACTION.RETRIEVE_BY_ID or \
           self.action == ACTION.RETRIEVE_BY_AREA_CODE or \
           self.action == ACTION.DELETE_RECORD:
            self.record_id = data
        elif self.action == ACTION.WRITE_RECORD:
            self.tuple_data = data

    def __str__(self):
        data = self.get_data_for_str()
        return f"{ACTION_WORDS[self.action]}, {self.table_name}, {data}"

    def to_log(self):
        data = self.get_data_for_str()
        if self.action == ACTION.WRITE_RECORD:
            return f"{ACTION_CODES[self.action]} {self.table_name} {data.to_log()}"

        if data is not None:
            return f"{ACTION_CODES[self.action]} {self.table_name} {data}"

        return f"{ACTION_CODES[self.action]} {self.table_name}"

    def get_data_for_str(self):
        if self.action == ACTION.RETRIEVE_BY_ID or \
           self.action == ACTION.RETRIEVE_BY_AREA_CODE or \
           self.action == ACTION.DELETE_RECORD:
            data = self.record_id
        elif self.action == ACTION.WRITE_RECORD:
            data = self.tuple_data
        elif self.action == ACTION.DELETE_TABLE:
            data = None
        return data
