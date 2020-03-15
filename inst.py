# inst.py
import ACTION

class Instruction():
    action = None
    table_name = None
    tuple_data = None
    record_id = None

    def __init__(self, action: ACTION, table_name: str, data=None):
    """
    data: either tuple_data or record_id
        - tuple_data: (int id, string client_name, string phone)
        - record_id: int id
    """
        self.action = action
        self.table_name = table_name

        if self.action == ACTION.RETRIEVE_BY_ID or self.action == ACTION.RETRIEVE_BY_AREA_CODE:
            self.record_id = data
        elif self.action == ACTION.WRITE_RECORD:
            self.tuple_data = data
