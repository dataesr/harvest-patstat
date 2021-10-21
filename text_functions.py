import re
import string as st
import unicodedata

from fuzzywuzzy import fuzz


def remove_punctuations(string, list_punct_except=None):
    to_remove = st.punctuation
    if list_punct_except:
        for exception in list_punct_except:
            to_remove = to_remove.replace(exception, '')
    list_replacement = " " * len(to_remove)
    trantab = str.maketrans(to_remove, list_replacement)
    string_clean = remove_first_end_spaces(remove_multiple_spaces(string.translate(trantab)))
    return string_clean


def isascii(string):
    """Check if the characters in string s are in ASCII, U+0-U+7F."""
    return len(string) == len(string.encode())


def remove_multiple_spaces(string):
    string_clean = re.sub(' +', ' ', string)
    return string_clean


# remove end spaces
def remove_end_spaces(string):
    return "".join(string.rstrip())


# remove first and end spaces
def remove_first_end_spaces(string):
    return "".join(string.rstrip().lstrip())


# remove all spaces
def remove_all_spaces(string):
    return "".join(string.split())


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])


def remove_words(string, pattern_to_remove):
    p = re.compile(pattern_to_remove)
    s = p.sub('', string).strip()
    return s


def put_title_format(chaine):
    try:
        titre = chaine.title()
    except:
        print('problème avec la chaine ')
        print(chaine)
        titre = ''
    return titre


def put_lower_format(chaine):
    try:
        lower_str = chaine.lower()
    except:
        print('problème avec la chaine ')
        print(chaine)
        lower_str = ''
    return lower_str


def get_clean_name(string, list_pattern_to_remove=None):
    """   This function gets rid of punctuations, accents, puts in lower format then remove all words matching the
    pattern 'pattern_to_remove' in input

    param string:
    type string: string
    param pattern_to_remove: pattern for wich all words matching it must be removed
    type pattern_to_remove: string

    return: The string in input cleaned

    """

    string_clean = remove_punctuations(remove_accents(string.lower()))
    if list_pattern_to_remove:
        for pat in list_pattern_to_remove:
            string_clean = remove_words(string_clean, pat)

    return string_clean


def put_words_in_order(string):
    """   This function puts all the words of a string in alphabetical order

    param string: a sentence to be ordered
    type string: string

    return: the string in alphabetical order

    """

    words = string.split(' ')
    words.sort()
    order_list = []
    for word in words:
        order_list.append(word)
        order_string = ' '.join(order_list)

    return order_string


def test_string_within_string(string1, string2):
    """   This function gets 2 strings in input and tests if one of them is entirely comprised in the other one

    param string1: first string to be tested
    type string1: string
    param string2: second string to be tested
    type string2: string

    return:  a score (integer) of 100 if the condition is TRUE, 0 otherwise

    """

    (reduced_string1, reduced_string2) = remove_identic_words(string1, string2)
    if (reduced_string1 == '') or (reduced_string2 == ''):
        is_substring = True
    else:
        is_substring = False

    return is_substring


def remove_identic_words(string1, string2, len_min_of_word=1):
    """   This function gets 2 strings in input, test if the 2 strings have words in common and erases these words in
    each of them

    param string1: first string to be tested
    type string1: string
    param string2: second string to be tested
    type string2: string

    return: a tuple with the 2 initial strings with all identic words erased and the boolean result of the
    identic word test

    """

    setwords1 = set(string1.split(' '))
    setwords2 = set(string2.split(' '))
    new_setwords1_with_ini = setwords1.copy()
    new_setwords2_with_ini = setwords2.copy()
    # on ne va considérer des noms identiques que s'ils ont une certaine taille - par défaut la taille est 1
    setwords1 = {word for word in setwords1 if len(word) >= len_min_of_word}
    setwords2 = {word for word in setwords2 if len(word) >= len_min_of_word}
    test_mot_identique = False
    for word in setwords2:
        for mot in setwords1:
            if fuzz.ratio(word, mot) > 95:
                test_mot_identique = True
                try:
                    new_setwords1_with_ini.remove(mot)
                    new_setwords2_with_ini.remove(word)
                except:
                    print('erreur : noms : ' + string1 + ' et nom ' + string2)

    return (' '.join(new_setwords1_with_ini), ' '.join(new_setwords2_with_ini)), test_mot_identique


def get_list_initials(string):
    """   This function take a string in input and gets the initials of each word

    param string: a string for which we need to get initials
    type string: string

    return:  A list of all initials of each word of the string

    """

    if string != '':
        linit = [init[0] for init in (string.split(' '))]
    else:
        linit = ''

    return linit
