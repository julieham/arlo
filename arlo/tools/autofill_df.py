from arlo.operations.df_operations import assign_content_to_existing_column, assign_new_column
from arlo.tools.autofillManager import autofill_series_with_series, make_dictioname, _autofill_series


def fill_missing_with_autofill_dict(data, column_from, dictionary):
    column_content = autofill_series_with_series(data[column_from], dictionary)
    assign_content_to_existing_column(data, dictionary.name, column_content)


def add_new_column_autofilled(data, column_from, column_to, star_fill=False):
    dictioname = make_dictioname(column_from, column_to)
    column_content = _autofill_series(data[column_from], dictioname, star_fill)
    assign_new_column(data, column_to, column_content)


def fill_existing_column_with_autofill(data, column_from, column_to, star_fill=False, overrule=False):
    dictioname = make_dictioname(column_from, column_to)
    column_content = _autofill_series(data[column_from], dictioname, star_fill)
    assign_content_to_existing_column(data, column_to, column_content, overrule=overrule)
