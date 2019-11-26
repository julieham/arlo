from parameters.param import directory
from tools.logging import info


def get_classbot_last():
    filename = directory[:-10] + 'classbot/output.txt'
    info(filename)
    with open(filename, 'r') as f:
        return f.read()
