import re

from arlo.parameters.param import directory
from arlo.read_write.crud import read_dico, read_dictionary


def find_from_pattern(name, patterns_dict):
    for pattern, translation in patterns_dict.items():
        regex_pattern = '.*'+pattern+'.*'
        if re.match(regex_pattern.lower(), name.lower()):
            return translation
    return False


def autofill_name(s):
    translation = find_from_pattern(s, read_dictionary(directory + 'autofill_name.csv'))
    if translation:
        return translation
    return '**' + s.title() + '**'


def autofill_cat(name):
    dico_categories = read_dico('./arlo/data/' + 'autofill_category.csv')
    if name in dico_categories:
        return dico_categories[name]
    return '-'
