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

    def overwrite_values(self, rec):
        self.id = rec.id
        self.client_name = rec.client_name
        self.phone = rec.phone

    def to_bytearray(self):
        id_ba = bytearray([(self.id >> shift) & 0xff for shift in [0, 8, 16, 24]])
        name_ba = pad_ba(bytearray(self.client_name, "ascii"), 16)
        phone_ba = pad_ba(bytearray(self.phone, "ascii"), 12)
        return id_ba + name_ba + phone_ba

    def __repr__(self):
        return f"Record({self.id}, {self.client_name}, {self.phone})"

    def __str__(self):
        return f"id: {self.id}, client_name: {self.client_name}, phone: {self.phone}"

    def __hash__(self):
        return self.id

    def __eq__(self, other):
        return other is not None and \
               self.id == other.id and \
               self.client_name == other.client_name and \
               self.phone == other.phone
