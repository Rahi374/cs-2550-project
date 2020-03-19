from common import *

class Record():
    def __init__(self, id: int, client_name: str, phone: str):
        if not isinstance(id, int):
            raise TypeError("record id must be int") 
        if not isinstance(client_name, str):
            raise TypeError("record client_name must be str")
        if not isinstance(phone, str):
            raise TypeError("record phone must be str")
        if len(client_name) > 16:
            raise Exception("client name is too long")
        if len(phone) > 12:
            raise Exception("phone is too long")

        self.id = id
        self.client_name = client_name
        self.phone = phone

    def convert_to_byte_array(self):
        # TODO for Paul
        pass

    def __repr__(self):
        return f"Record({self.id}, {self.client_name}, {self.phone})"

    def __str__(self):
        return f"id: {self.id}, client_name: {self.client_name}, phone: {self.phone}"


