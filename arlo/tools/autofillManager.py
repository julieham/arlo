from arlo.format.df_operations import assign_new_column, assign_content_to_existing_column
from arlo.parameters.param import directory
from arlo.read_write.fileManager import read_series, write_dictionary_to_file


def autofill_directory(filename):
    return directory + 'autofill/' + filename + '.csv'


def series_dictioname(dictionary):
    source = dictionary.index.name
    destination = dictionary.name
    return source + '-to-' + destination


def read_autofill_dictionary(dictioname):
    return read_series(autofill_directory(dictioname))


def autofill_series_with_series(source, dictionary, star_fill=False):
    dictionary.index = dictionary.index.str.upper()
    default_fill = '**' + source.str.title() + '**' if star_fill else '-'
    return source.str.upper().map(dictionary).fillna(default_fill)


def _autofill_series(series, dictioname, star_fill=False):
    dictionary = read_autofill_dictionary(dictioname)
    return autofill_series_with_series(series, dictionary, star_fill=star_fill)


def fill_missing_with_autofill_dict(data, column_from, dictionary):
    column_content = autofill_series_with_series(data[column_from], dictionary)
    assign_content_to_existing_column(data, dictionary.name, column_content)


def add_new_column_autofilled(data, column_from, column_to, star_fill=False):
    dictioname = column_from + '-to-' + column_to
    column_content = _autofill_series(data[column_from], dictioname, star_fill)
    assign_new_column(data, column_to, column_content)


def fill_existing_column_with_autofill(data, column_from, column_to, star_fill=False, overrule=False):
    dictioname = column_from + '-to-' + column_to
    column_content = _autofill_series(data[column_from], dictioname, star_fill)
    assign_content_to_existing_column(data, column_to, column_content, overrule=overrule)


def clean_dictionary(dictionary):
    dictionary = dictionary.reset_index().drop_duplicates()
    dictionary = dictionary.set_index(dictionary.columns.tolist()[0]).squeeze()
    return dictionary.sort_index()


def write_autofill_dictionary(dictionary):
    dictioname = series_dictioname(dictionary)
    dictionary = clean_dictionary(dictionary)
    write_dictionary_to_file(dictionary, autofill_directory(dictioname))
