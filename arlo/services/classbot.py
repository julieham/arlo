from parameters.param import classbot_directory


def get_classbot_last():
    filename = classbot_directory + 'output.txt'
    text = '<br>'.join(open(filename, 'r').readlines())
    return text
