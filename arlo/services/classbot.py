from tools.logging import info


def get_classbot_last():
    filename = '/home/arlo/classbot/output.txt'
    info(filename)
    with open(filename, 'r') as f:
        return '\n'.join(f.readlines())
