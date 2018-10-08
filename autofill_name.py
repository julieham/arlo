import re


def read_dico(filename):
    dictionary = dict()
    f = open(filename, 'r')
    for line in f:
        try:
            line = line.replace('\n', '')
            a, *b = line.split(',')
            dictionary[a] = b
        except ValueError:
            pass
    f.close()
    return dictionary


def sort_and_rewrite_dico(filename):
    dictionary = read_dico(filename)
    f = open(filename, 'w')
    for d in sorted(dictionary, key=lambda k: dictionary[k][1].lower() + dictionary[k][0].lower()):
        f.write(d + "," + ','.join(dictionary[d]) + '\n')
    f.close()


def find_from_pattern(name, patterns_dict):
    for pattern, translation in patterns_dict.items():
        regex_pattern = '.*'+pattern+'.*'
        if re.match(regex_pattern, name):
            return translation
    return False


def make_readable_name(s):
    translation = find_from_pattern(s, read_dico('data/dico_sorted.csv'))
    if translation:
        return translation
    return '** ' + s + ' **'
