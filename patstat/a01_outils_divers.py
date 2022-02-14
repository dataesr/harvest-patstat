#!/usr/bin/env python
# coding: utf-8


"""

Autre fonction 'get_score_initials' utilisée auparavant

"""

import json
import pickle
import re
import time

import pandas as pd
import requests
from fuzzywuzzy import fuzz

from patstat import config
import text_functions as tf


def save_object(python_object, path):
    """A function to pickle an object

    :param python_object: the object one wants to pickle
    :param path: the path to save to pickle file
    :type path:str
    :return: None

    .. seealso:: load_object()
    """

    with open(path, "wb") as path_to_save:
        pickle.dump(python_object, path_to_save)


def load_object(path):
    """A function to unpickle an object

    :param path: the path where the pickle file is
    :type path:str
    :return: the pickled object

    .. seealso:: save_object()
    """

    with open(path, 'rb') as path_to_load:
        tmp = pickle.load(path_to_load)
    return tmp


def get_score_initials(name1, name2):
    setwords1 = name1.split(' ')
    setwords2 = name2.split(' ')
    identik1 = []
    identik2 = []
    nb_matchs = 0
    new_word1 = setwords1.copy()
    new_word2 = setwords2.copy()
    for word in setwords2:
        for mot in setwords1:
            if fuzz.ratio(word, mot) > 95:
                identik1.append(mot)
                identik2.append(word)
                new_word1.remove(mot)
                new_word2.remove(word)
                nb_matchs += 1
    if new_word1 != '':
        linit1 = [init[0] for init in new_word1]
    else:
        linit1 = ''
    if new_word2 != '':
        linit2 = [init[0] for init in new_word2]
    else:
        linit2 = ''
    print(linit1)
    print(linit2)

    score = 0

    if nb_matchs == 1:
        if len(identik1[0]) > 3:
            if set(linit1) == set(linit2):
                score = 95
    elif nb_matchs > 1:
        if set(linit1) == set(linit2):
            score = 95
    else:
        score = 0
    return score


"""   fonction pour récupérer le label des entreprises (que celles qui sont dans scanR) avec le SIREN
"""


def get_label_organization_siren(siren):
    headers = config.A01_LAB_ORG_SIREN
    url_address_request = 'http://185.161.45.213/organizations/organizations/'
    r = requests.get(url_address_request + str(siren), headers=headers)
    if r.status_code != 200:
        print("Error in getting label of {}".format(siren))
        return None
    elif json.loads(r.text.replace("'", ''))['names'][0]:
        premier_nom = json.loads(r.text.replace("'", ''))['names'][0]
        nom_fr = tf.put_title_format(premier_nom['name_fr'])
        return nom_fr


"""    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   FONCTIONS GEOLOCALISATION

"""


def get_geocod_from_address(adresse):
    headers = config.A01_GEOCOD_ADDRESS
    url_address_request = 'http://185.161.45.213/geocoder/address/'
    r = requests.get(url_address_request + adresse, headers=headers)
    if r.status_code != 200:
        print("Error in getting adresse {}".format(adresse))
        return None
    return r.text


def get_list_geocod_adresses(list_of_adresses):
    liste_adresses_completed = []
    count = 0
    start_time = time.time()
    for adresse in list_of_adresses:
        count += 1
        liste_adresses_completed.append(get_geocod_from_address(adresse))
        if count % 200 == 0:
            print("Nombre de requêtes d'adresse effectuées : " + str(count) + " --  ")
            print("--- %s seconds ---" % (time.time() - start_time))
        if count % 3000 == 0:
            save_object(liste_adresses_completed, 'liste_adresses_completed.p')
    return liste_adresses_completed


def transform_address(address):
    adr_trans = re.sub(r'cedex(\s)?(\d)*', ' ', address)
    adr_trans = re.sub(r'c(\s)?s(\s)?(\d)+', ' ', adr_trans)
    adr_trans = re.sub(r'b(\s)?p\s(\d)+', ' ', adr_trans)
    adr_trans = re.sub(r'\sfr\s', ' ', adr_trans)
    adr_trans = re.sub(r'\sfr$', ' ', adr_trans)
    adr_trans = re.sub(r'^fr$', ' ', adr_trans)
    adr_trans = re.sub(r'\sbp(\s)?(\d)+', ' ', adr_trans)
    adr_trans = re.sub(r'\sf\s', ' ', adr_trans)
    adr_trans = tf.remove_first_end_spaces(tf.remove_multiple_spaces(adr_trans))
    return adr_trans


def get_dep_from_adresse(adresse):
    if re.findall(r'\d{5}', adresse):
        cp = tf.remove_first_end_spaces(tf.remove_multiple_spaces(re.findall(r'\d{5}', adresse)[-1]))
        return cp[0:2]
    else:
        return ''


def get_cpville_from_adresse(adresse):
    match = re.search(r"(?s:.*)(\d{5})", adresse)
    if match:
        cpville = adresse[match.end() - 5:]
        return tf.remove_first_end_spaces(tf.remove_multiple_spaces(cpville))
    else:
        return ''


def replace_saut_ligne(string_to_replace):
    string_cor = string_to_replace.replace('\n', ' ').replace('\t', ' ')

    return string_cor


def get_departement(cp):
    if cp:
        dep = cp[0:2]
        return dep
    else:
        return None


def get_df_from_list_adresses(list_adresses):
    # transformation de la string de listes en listes
    list_adr = [json.loads(idx.replace("'", '')) for idx in list_adresses if not (idx is None)]
    # transformation de la liste en tableau
    geocod_address = pd.DataFrame(list_adr)
    return geocod_address


def extract_coordinates(coordinates_from_api):
    if coordinates_from_api:
        if coordinates_from_api['type'] == 'Point':
            coord = tuple(coordinates_from_api['coordinates'])
            return coord
    else:
        return None
