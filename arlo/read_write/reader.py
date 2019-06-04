from pandas import read_csv, DataFrame, Series

from arlo.parameters.param import data_columns, typed_columns


def read_df_file(filename, na_values=None, sep=';', index_col=None, squeeze=False, parse_dates=False, date_parser=None):
    return read_csv(filename, na_values=na_values, sep=sep, index_col=index_col,
                    squeeze=squeeze, encoding='utf-8',
                    parse_dates=parse_dates, date_parser=date_parser
                    )


def empty_data_dataframe():
    empty_df = DataFrame(columns=data_columns)
    for column_name in typed_columns:
        empty_df[column_name] = Series(dtype=typed_columns[column_name])
    return empty_df
