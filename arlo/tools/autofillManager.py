from arlo.parameters.param import directory
from arlo.read_write.fileManager import read_series, write_dictionary_to_file
from web.status import my_response, is_fail

star_fill_characters = '**'
nb_char_star_fill = len(star_fill_characters)


def make_dictioname(source, destination):
    return source + '-to-' + destination


def remove_star_fill(name):
    if (name[:nb_char_star_fill] == name[-nb_char_star_fill:] == star_fill_characters) and len(name) > 2*nb_char_star_fill:
        return name[nb_char_star_fill:-nb_char_star_fill]
    return name


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
    default_fill = '**' + source.str.title() + star_fill_characters if star_fill else '-'
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


def add_reference(name_source, name_destination, value_source, value_destination):
    dictioname = make_dictioname(name_source, name_destination)
    dictionary = read_autofill_dictionary(dictioname)
    value_source = remove_star_fill(value_source)
    value_destination = remove_star_fill(value_destination)
    if value_source.upper() not in dictionary.str.upper():
        dictionary[value_source] = value_destination
        write_autofill_dictionary(dictionary)
        return my_response(True)
    else:
        return my_response(False, value_source + ' already present')


def _not_possible_to_add_name_references(bank_name, name, category):
    return name is None or (bank_name, category) is (None, None)


def _add_name_references(bank_name, name, category):
    response = my_response(True)
    if bank_name is not None:
        response = add_reference('bank_name', 'name', bank_name, name)
        if is_fail(response):
            return response
    if category is not None:
        response = add_reference('name', 'category', name, category)
    return response
