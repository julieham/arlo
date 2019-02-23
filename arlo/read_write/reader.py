from pandas import read_csv


def read_df_file(filename, na_values=None, sep=', ',index_col=None, squeeze=False):
    return read_csv(filename, na_values=na_values, sep=sep, index_col=index_col, squeeze=squeeze, encoding='utf-8')
