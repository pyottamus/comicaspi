from datetime import date

def serialize_date(date_val: date) ->int:...

def deserialize_date(val: int)->date:...

def is_leap(year: int) -> bool:...

def gen_key(date_val: date, secondary:int)->int:...

def gen_key_serialize(date_val: date, secondary:int) -> tuple[int, int]:...