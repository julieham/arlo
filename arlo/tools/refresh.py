from arlo.operations.date_operations import string_to_datetime, time_since, now
from arlo.read_write.fileManager import get_last_update_string, change_last_update_to_this_date


def minutes_since_last_update():
    last_update = string_to_datetime(get_last_update_string())
    return time_since(last_update).total_seconds()//60


def change_last_update_to_now():
    change_last_update_to_this_date(now())