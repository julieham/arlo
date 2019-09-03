from arlo.read_write.permissions import set_file_as_read_write, set_file_as_read_only


def write_df_to_csv(df, filename, sep=';', header=True, index=True):
    try:
        set_file_as_read_write(filename)
    except FileNotFoundError:
        pass
    df.to_csv(filename, sep=sep, header=header, index=index)
    set_file_as_read_only(filename)
