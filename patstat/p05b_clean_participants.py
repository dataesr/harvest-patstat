#!/usr/bin/env python
# coding: utf-8

import operator
import os
from datetime import datetime as dt

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

from utils import swift

from patstat import dtypes_patstat_declaration as types
from patstat import text_functions as tf

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


# Par famille élargie, on rapproche les noms des participants (individus).
# Pour cela, on commence par sélectionner les participants à grouper :
# ceux appartenant aux familles dont au moins un nouveau participant a été ajouté à la dernière version.

def get_part_to_deduplicate_by_fam(part_table: pd.DataFrame, famtype: str, name_or: str, name_cor: str) -> pd.DataFrame:
    """   This function gets from a participant table a table ready to group

    param part_table: table with participants information - must have 'isascii' and columns corresponding to the input
    variables famtype, name_or and name_cor
    type part_table: dataframe
    param famtype: name of the variable containing the type of family in which we want to assimilate the individuals
    (can be simple family 'docdb_family_id'
    or large family 'inpadoc_family_id')
    type famtype: string
    param name_or: name of the variable with new name labels from the actual patstat version
    type name_or: string
    param name_cor: name of the variable with the old name corrected (after deduplicating) from the last patstat version
    type name_cor: string

    :return: a dataframe ready for deduplication with the original name and his family

    """

    part = part_table[[famtype, "doc_std_name", name_or, name_cor, 'islatin']].copy()
    for col in part.columns:
        part[col] = part[col].replace(np.nan, '')
    #  on ne dédoublonne que les noms en caractères latins
    part2 = part[part['islatin']].copy()
    # on sélectionne les familles qui ont des nouveaux participants
    # (ceux dont le nom corrigé de la version précédente est nul)
    fam_to_deduplicate = part2[part2[name_cor] == ''][[famtype, "doc_std_name"]].drop_duplicates().copy()
    # on sélectionne tous les participants qui appartiennent à ces familles :
    # on regroupe sur toutes les familles où il y a des nouveaux participants
    # depuis la version précédente
    part_to_deduplicate_by_fam = pd.merge(part2[[famtype, "doc_std_name", name_or]], fam_to_deduplicate,
                                          on=[famtype, "doc_std_name"],
                                          how='inner').drop_duplicates()

    return part_to_deduplicate_by_fam


# On groupe

def get_initial_info(text: str) -> bool:
    """ This function tests if the string contains initials : names of persons with initials alone like 'P. Andrews'

    param text: the string to be tested for initials
    type text: string

    :return: a boolean that returns True if there is at least one initial in the string, False otherwise

    """

    list_words = text.split(' ')
    list_init = [mot for mot in list_words if len(mot) == 1]
    init = False
    if list_init:
        init = True

    return init


def get_multinames_info(name: str) -> bool:
    """   This function tests the number of words separated by the separator in input

    param name: the string to be tested for number of words
    type name: string
    param separator: the character that is meant to separate the words in the string
    type separator: string

    :return: a boolean that returns True if there are more than two words separated, False otherwise

    """
    separator = [",", ";", "/", ",,"]
    multin = False
    for sep in separator:
        if len(name.split(sep)) > 2:
            multin = True

    return multin


def get_dict_cluster_name(fam_table: pd.DataFrame, name_var: str, function_dict_score) -> dict:
    """   This function groups all participants of the input table (corresponding to one family)

    param fam_table: table with participant's names of the same family
    type fam_table: dataframe
    param name_var: name of the variable containing the names to deduplicate
    type name_var: string
    param function_dict_score: the function to be used for grouping
    type function_dict_score: function

    :return: a dictionary where the key is the index of table and the value is the new deduplicated name

    """

    family_table = fam_table.copy()
    dict_clusters = {}
    # get_clean_name est défini dans le fichier 'text_functions.py'
    # on crée les variables 'init' et 'multin' qui sont des indicateurs utilisés dans les comparaisons entre noms
    family_table['name_clean'] = family_table[name_var].apply(tf.get_clean_name,
                                                              list_pattern_to_remove=['^dr ', '^mme ', ' mme ',
                                                                                      r'\d\)'])
    family_table['init'] = family_table['name_clean'].apply(get_initial_info)
    family_table['multin'] = family_table[name_var].apply(get_multinames_info)
    family_table['len_name'] = family_table['name_clean'].apply(len)
    # Les premières chaînes vont être sélectionnées en premier pour servir de référent du groupe.
    # il faut donc que ces premiers référents se rapprochent le plus possible du nom 'commun' de la personne :
    # on trie donc pour avoir la plus courte chaîne, hors cas d'initiales et de multiples noms qu'on traite en dernier
    # en effet, si on garde un nom long en premier dans le groupe, avec plusieurs prénoms, les autres ne vont pas
    # matcher avec.
    family_table = family_table.sort_values(['init', 'multin', 'len_name'])
    for index, row in family_table.iterrows():
        name1 = row['name_clean']
        namesource = row[name_var]
        dict_score = function_dict_score(dict_clusters, namesource)
        test_cluster = True
        if len(dict_score):
            best_occur = max(dict_score.items(), key=operator.itemgetter(1))[0]
            max_score = dict_score[best_occur]
            best_name = [name for name in dict_clusters.keys() if best_occur in dict_clusters[name]['occurences']][0]
            if max_score >= 90:
                dict_clusters[best_name]['index'].add(index)
                dict_clusters[best_name]['occurences'].add(name1)
                test_cluster = False
        if test_cluster:
            dict_clusters[name1] = {'index': {index}, 'occurences': {name1}}
    dict_clusters_inv = {}
    for index in family_table.index:
        for key, values in dict_clusters.items():
            if index in values['index']:
                dict_clusters_inv[index] = key

    return dict_clusters_inv


def deduplicate_part_one_family(part_table: pd.DataFrame, famtype: str, name_var: str, family: pd.Int64Dtype(),
                                function_dict_score) -> pd.DataFrame:
    """   This function gets from a participant table an extract table with deduplicated names
    for the family 'family' specified in input

    param part_table: table with participants information - must have the columns corresponding
    to the input variables famtype and name_var
    type part_table: dataframe
    param famtype: name of the variable containing the type of family in which we want to assimilate
    the individuals (can be simple family 'docdb_family_id' or large family 'inpadoc_family_id')
    type famtype: string
    param name_var: name of the variable containing the names to deduplicate
    type name_var: string
    param family: the current family identifier on which the deduplication is being done
    type family: string
    param function_dict_score: the function to be used for grouping
    type function_dict_score: function

    :return: an extract of the dataframe in input, filtered on the family 'family' in input,
    with a variable 'new_name' added, corresponding to the
    deduplicated name

    """

    # on extrait dans un tableau juste les enregistrements de participants correspondant à la famille en cours
    family_table = part_table[part_table[famtype] == family].copy()
    # on réinitialise l'index
    family_table.set_index(famtype).reset_index()
    # on utilise une fonction qui crée un dictionnaire : pour un index donné, on aura la correspondance
    # du nom corrigé après dédoublonnage
    dict_clusters_inv = get_dict_cluster_name(family_table, name_var, function_dict_score)
    # pour dédoublonner, on donne le même nom corrigé (new_name ci-dessous) à tous les participants
    # de la famille repérés comme identiques
    family_table['new_name'] = [dict_clusters_inv[index] for index in family_table.index]

    return family_table


def deduplicate_part_by_fam(part_table: pd.DataFrame, famtype: str, name_var: str, function_dict_score) -> pd.DataFrame:
    """   This function gets from a participant table a table with deduplicated names

    param part_table: table with participants information - must have the columns corresponding
    to the input variables famtype and name_var
    type part_table: dataframe
    param famtype: name of the variable containing the type of family in which we want to assimilate
    the individuals (can be simple family 'docdb_family_id'
    or large family 'inpadoc_family_id')
    type famtype: string
    param name_var: name of the variable containing the names to deduplicate
    type name_var: string
    param function_dict_score: the function to be used for grouping
    type function_dict_score: function

    :return: a dataframe with all deduplicated names by family

    """

    list_families = set(part_table[famtype])
    deduplicated_table_list = []
    count = 0
    print(dt.now())
    # on va grouper par famille, donc on boucle sur les familles
    # la variable famtype permet de grouper au choix sur les familles simples
    # 'docdb_family_id' ou sur les familles élargies 'inpadoc_family_id'
    for family in list_families:
        count += 1
        # cette fonction groupe sur une famille
        family_table = deduplicate_part_one_family(part_table, famtype, name_var, family, function_dict_score)
        # on enregistre les résultats dans une liste contentant toutes les tables dédoublonnées
        deduplicated_table_list.append(family_table)
        # pour savoir où on en est, tous les 3000 familles, on affiche un message
        if count % 3000 == 0:
            print('Traitement de la famille numéro ' + str(count))
    print(dt.now())
    # a la fin on concatène tous les tableaux résultats correspondant à toutes les familles
    deduplicated_ind = pd.concat(deduplicated_table_list)

    return deduplicated_ind


def get_dict_score_individus(dict_clusters: dict, name: str) -> dict:
    """   This function tries different methods to match a person name from the input with the different occurences
    of names loaded in
    'dict_clusters' dictionary and gets the best score

    param dict_clusters: a dictionary where the key is a name and the value is another dictionary where keys are the
    index of original person's table and
    the occurences of group name referent
    type dict_clusters: dictionary
    param name: name to match
    type name: string

    :return: a dictionary where the key is the name to deduplicate and the value is the best score

    """

    # on initialise 'dict_score', qui est le dictionnaire répertoriant toutes les notes pour chaque comparaison du
    # nom actuel avec une occurence du groupe
    dict_score = {}
    # 'init' est la variable qui indique si name1 comprend une ou des initiales
    # 'multin' indique si name1 comprend plus de 2 noms (càd qu'il y a les 2ème et éventuellement 3èmes prénoms)
    name1 = tf.get_clean_name(name, list_pattern_to_remove=['^dr ', '^mme ', ' mme '])
    init = get_initial_info(name1)
    multin = get_multinames_info(name)
    # on parcourt 'dict_clusters', qui est le dictionnaire de groupes (noms rapprochés) déjà identifiés
    for nom, dico in dict_clusters.items():
        occurences = dico['occurences']
        # pour un même groupe, on parcourt ses occurences déjà rencontrées
        for occur in occurences:
            name2 = occur
            # name1 est le nom actuel qu'on cherche à rapprocher à un groupe
            # name2 est l'occurence d'un groupe
            if name1 == name2:
                dict_score[name2] = 100
            else:
                if init == 0 & multin == 0:
                    # fuzz.token_sort_ratio match lorsque les mots sont dans un ordre différent
                    # (cas étendu de la distance de Levenshtein)
                    # gère par exemple le cas "Jacque stanton" et "Stanton jacques"
                    score1 = fuzz.token_sort_ratio(name1, name2)
                    # fuzz.ratio est plus exigent sur l'ordre des mots, mais moins sur les fautes de frappe
                    # fuzz.ration donne 85 pour "dambach claus" vs "dambakh klaus" alors que fuzz.token_sort_ratio
                    # ne donne que 46
                    # pour un meilleur résultat, on les range quand même dans l'ordre alphabétique
                    score2 = fuzz.ratio(tf.put_words_in_order(name1), tf.get_clean_name(name2))
                    # cette fonction considère le cas où l'un des 2 noms comprend entièrement l'autre :
                    # il rapproche par exemple "'Ing. Jacques stanton'"
                    # et "Jacques stanton"
                    if tf.test_string_within_string(name1, name2) is True:
                        score3 = 100
                    else:
                        score3 = 0
                    dict_score[name2] = max(score1, score2, score3)
                if multin == 1:
                    # lorsqu'il y a plusieurs prénoms (on considère qu'il y a plusieurs prénoms car séparés par une
                    # virgule) et dans ce cas uniquement on utilise
                    # une fonction spéciale qui rapproche des chaînes de longueurs très différentes
                    dict_score[name2] = fuzz.token_set_ratio(name1, name2)
                if init == 1:
                    # dans le cas des initiales, on va vraiment chercher à ce que les initiales correspondent aux
                    # initiales d'un autre nom complet
                    # pour ne pas rapprocher des pers de même famille comme 'Michel Blanc' et 'A Blanc'
                    # mais seulement avec 'M Blanc' : il y a pas mal de cas de
                    # personnes différentes avec même nom de famille
                    dict_score[name2] = get_score_initials(name1, name2)

    return dict_score


def get_score_initials(name1: str, name2: str) -> float:
    """   This function gets 2 strings in input, with name1 a person name with at least one initial like
    ('O. Wilson') and gives a score for the comparison with name2

    param name1: first string to be tested, with initials
    type name1: string
    param name2: second string to be tested
    type name2: string

    :return:  a score (integer) result of matching between the 2 strings of names

    """

    # On teste s'il existe des mots identiques (d'au moins 2 lettres) entre les deux noms,
    # et on récupère les noms réduits (sans les mots identiques)
    (reduced_name1, reduced_name2), test_mot_identique = tf.remove_identic_words(name1, name2, 3)
    # On en prend la liste d'initiales
    linit1 = tf.get_list_initials(reduced_name1)
    linit2 = tf.get_list_initials(reduced_name2)

    # s'il y a bien
    if test_mot_identique and set(linit1) == set(linit2):
        score = 100
    else:
        score = 0
    return score


def get_dict_score_entp_identicals(dict_clusters: dict, name: str) -> dict:
    """   This function tries different methods to match a person name from the input with the different occurences of
    names loaded in
    'dict_clusters' dictionary and gets the best score - This function is adapted to the entreprises, not the persons

    param dict_clusters: a dictionary where the key is a name and the value is another dictionary where keys are the
    index of original person's table and
    the occurences of group name referent
    type dict_clusters: dictionary
    param name: name to match
    type name: string

    :return: a dictionary where the key is the name to deduplicate and the value is the best score

    """

    dict_score = {}
    name1 = tf.get_clean_name(name)
    for nom, dico in dict_clusters.items():
        occurences = dico['occurences']
        for occur in occurences:
            score2 = fuzz.ratio(name1, occur)
            score3 = fuzz.token_sort_ratio(name1, occur)
            dict_score[occur] = max(score2, score3)

    return dict_score


def deduplication(part_df: pd.DataFrame, type_person: str, family: str, get_dict) -> pd.DataFrame:
    part_indiv = part_df[part_df['type'] == type_person].copy()

    part_ind_to_deduplicate = get_part_to_deduplicate_by_fam(part_indiv, family, 'name',
                                                             'old_name')

    deduplicated_ind = deduplicate_part_by_fam(part_ind_to_deduplicate, family, 'name',
                                               get_dict)

    # On recompose ensuite la table individus avec les nouveaux groupes

    part_ind = part_indiv.merge(deduplicated_ind[[family, 'doc_std_name', 'name', 'new_name']].drop_duplicates(),
                                on=[family, 'doc_std_name', 'name'], how='left')

    for col in part_ind.columns:
        part_ind[col] = part_ind[col].replace(np.nan, '')

    return part_ind


def get_clean_part():
    # set working directory
    os.chdir(DATA_PATH)
    part = pd.read_csv('part_p05.csv', sep='|', dtype=types.part_init_types, engine="python")

    part.loc[(part["name_corrected"].notna()) & (part["old_name"].notna()), "old_name"] = part.loc[
        (part["name_corrected"].notna()) & (part["old_name"].notna()), "name_corrected"]

    part.loc[part["name_corrected"].notna(), "name"] = part.loc[part["name_corrected"].notna(), "name_corrected"]
    part.loc[part["name_corrected"].isna(), "name"] = part.loc[part["name_corrected"].isna(), "name_clean2"]

    for col in part.columns:
        part[col] = part[col].replace(np.nan, '')

    part_ind = deduplication(part, "pp", "docdb_family_id", get_dict_score_individus)
    part_ind['name_corrige'] = part_ind['new_name'].copy()
    part_ind.loc[part_ind['new_name'] == '', 'name_corrige'] = part_ind.loc[part_ind['new_name'] == '', 'old_name']
    part_ind.loc[part_ind['new_name'] == '', 'name_corrige'] = part_ind.loc[part_ind['new_name'] == '', 'old_name']
    part_ind['name_corrige'] = part_ind['name_corrige'].apply(tf.put_title_format)

    part_ind.to_csv("part_ind.csv", sep="|", index=False, encoding="utf-8")
    swift.upload_object('patstat', 'part_ind.csv')

    part_entp = deduplication(part, "pm", "docdb_family_id", get_dict_score_entp_identicals)
    for col in part_entp.columns:
        part_entp[col] = part_entp[col].fillna('')

    part_entp['name_corrige'] = part_entp['new_name'].copy()
    part_entp.loc[part_entp['new_name'] == '', 'name_corrige'] = part_entp.loc[part_entp['new_name'] == '', 'old_name']
    part_entp.loc[part_entp['new_name'] == '', 'name_corrige'] = part_entp.loc[part_entp['new_name'] == '', 'old_name']
    part_entp.loc[(part_entp['new_name'] == '') & (part_entp['name_corrected'] != ''), 'name_corrige'] = part_entp.loc[
        (part_entp['new_name'] == '') & (part_entp['name_corrected'] != ''), 'name_corrected']
    part_entp['name_corrige'] = part_entp['name_corrige'].apply(tf.put_title_format)

    part_entp.to_csv("part_entp.csv", sep="|", index=False, encoding="utf-8")
    swift.upload_object('patstat', 'part_entp.csv')
