import os


def print_permissions(filename):
    st = os.stat(filename)
    print(oct(st.st_mode))


def set_file_as_read_only(filename):
    os.chmod(filename, 0o444)


def set_file_as_read_write(filename):
    os.chmod(filename, 0o777)
