import re

from crud import read_dico


def find_from_pattern(name, patterns_dict):
    for pattern, translation in patterns_dict.items():
        regex_pattern = '.*'+pattern+'.*'
        if re.match(regex_pattern.lower(), name.lower()):
            return translation
    return False


def autofill_name(s):
    translation = find_from_pattern(s, read_dico('data/autofill_name.csv'))
    if translation:
        return translation
    return '** ' + s + ' **'


def autofill_cat(name):
    dico_categories = read_dico('data/autofill_category.csv')
    if name in dico_categories:
        return dico_categories[name]
    return '-'
