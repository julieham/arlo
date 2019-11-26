from parameters.param import directory


def get_classbot_last():
    filename = directory[:-5] + 'classbot/output.txt'
    with open(filename, 'r') as f:
        return f.read()
