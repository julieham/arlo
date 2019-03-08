from arlo.parameters.param import directory
from arlo.read_write.fileManager import read_series, write_dictionary_to_file


def make_dictioname(source, destination):
    return source + '-to-' + destination


def autofill_directory(filename):
    return directory + 'autofill/' + filename + '.csv'


def series_dictioname(dictionary):
    source = dictionary.index.name
    destination = dictionary.name
    return make_dictioname(source, destination)


def read_autofill_dictionary(dictioname):
    return read_series(autofill_directory(dictioname))


def autofill_series_with_series(source, dictionary, star_fill=False):
    dictionary.index = dictionary.index.str.upper()
    default_fill = '**' + source.str.title() + '**' if star_fill else '-'
    return source.str.upper().map(dictionary).fillna(default_fill)


def _autofill_series(series, dictioname, star_fill=False):
    dictionary = read_autofill_dictionary(dictioname)
    return autofill_series_with_series(series, dictionary, star_fill=star_fill)


def clean_dictionary(dictionary):
    dictionary = dictionary.reset_index().drop_duplicates()
    dictionary = dictionary.set_index(dictionary.columns.tolist()[0]).squeeze()
    return dictionary.sort_index()


def write_autofill_dictionary(dictionary):
    dictioname = series_dictioname(dictionary)
    dictionary = clean_dictionary(dictionary)
    write_dictionary_to_file(dictionary, autofill_directory(dictioname))


def autofill_single_value(value, source, destination):
    dictioname = make_dictioname(source, destination)
    formatted_value = value.strftime('%Y-%m-%d') if source == 'date' else value
    dico = read_autofill_dictionary(dictioname)
    result = dico[formatted_value] if formatted_value in dico else '-'
    return result