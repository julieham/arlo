from arlo.parameters.param import directory
from arlo.read_write.file_manager import read_series, write_dictionary_to_file
from web.status import is_fail, success_response, is_successful, failure_response

star_fill_characters = '**'
nb_char_star_fill = len(star_fill_characters)


def make_dictioname(source, destination):
    return source + '-to-' + destination


def remove_star_fill(name):
    if (name[:nb_char_star_fill] == name[-nb_char_star_fill:] == star_fill_characters) and len(name) > 2*nb_char_star_fill:
        return name[nb_char_star_fill:-nb_char_star_fill]
    return name


def _autofill_directory(filename):
    return directory + 'autofill/' + filename + '.csv'


def series_dictioname(dictionary):
    source = dictionary.index.name
    destination = dictionary.name
    return make_dictioname(source, destination)


def read_autofill_dictionary(dictioname):
    return read_series(_autofill_directory(dictioname))


def autofill_series_with_series(source, dictionary, star_fill=False):
    dictionary.index = dictionary.index.str.upper()
    default_fill = '**' + source.str.title() + star_fill_characters if star_fill else '-'
    return source.str.upper().map(dictionary).fillna(default_fill)


def _autofill_series(series, dictioname, star_fill=False):
    dictionary = read_autofill_dictionary(dictioname)
    return autofill_series_with_series(series, dictionary, star_fill=star_fill)


def _clean_dictionary(dictionary):
    dictionary = dictionary.reset_index().drop_duplicates()
    dictionary = dictionary.set_index(dictionary.columns.tolist()[0]).squeeze()
    return dictionary.sort_index()


def _write_autofill_dictionary(dictionary):
    dictioname = series_dictioname(dictionary)
    dictionary = _clean_dictionary(dictionary)
    write_dictionary_to_file(dictionary, _autofill_directory(dictioname))


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

    response = reference_is_valid(value_source, value_destination, dictionary)
    if is_successful(response):
        dictionary[value_source] = value_destination
        _write_autofill_dictionary(dictionary)
    return response


def reference_is_valid(source, destination, dictionary):
    if len(source) * len(destination) == 0:
        return failure_response("impossible to add null reference")
    if source.upper() in dictionary:
        return failure_response(source + 'already present in dictionary')
    return success_response()


def _add_name_references(bank_name, name, category):
    response = success_response()
    if category is not None:
        response = add_reference('name', 'category', name, category)
        if is_fail(response):
            return response
    if bank_name is not None:
        response = add_reference('bank_name', 'name', bank_name, name)
    return response
