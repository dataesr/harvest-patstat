#!/usr/bin/env python
# coding: utf-8

import operator
import os
import re
from datetime import datetime as dt

import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

import dtypes_patstat_declaration as types
import text_functions as tf

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"

# set working directory
os.chdir(DATA_PATH)


def get_part_to_deduplicate_by_fam(part_table, famtype, name_or, name_cor):
    """   This function gets from a participant table a table ready to clusterize

    param part_table: table with participants informations - must have 'isascii' and columns corresponding to the input
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

    part = part_table[[famtype, "doc_std_name", name_or, name_cor, 'isascii']].copy()
    for col in part.columns:
        part[col] = part[col].replace(np.nan, '')
    #  on ne dédoublonne que les noms en caractères latins
    part2 = part[part['isascii']].copy()
    # on sélectionne les familles qui ont des nouveaux participants
    # (ceux dont le nom corrigé de la version précédente est nul)
    fam_to_deduplicate = part2[part2[name_cor] == ''][[famtype]].drop_duplicates().copy()
    # on sélectionne tous les particiants qui appartiennent à ces familles :
    # on reclusterise sur toutes les familles où il y a des nouveaux participants
    # depuis la version précédente
    part_to_deduplicate_by_fam = pd.merge(part2[[famtype, name_or]], fam_to_deduplicate, on=famtype,
                                          how='inner').drop_duplicates()

    return part_to_deduplicate_by_fam


def get_max_key(one_dict):
    """   This function gets the item with maximum value in a dictionary

    param one_dict: a dictionary from wich we want to get the maximum value
    type one_dict: dictionary

    :return:  the item of the dictionary with the maximum value

    """

    return max(one_dict.items(), key=operator.itemgetter(1))[0]


def get_initial_info(text):
    """ This function tests if the string contains initials : names of persons with initials alone like 'P. Andrews'

    param text: the string to be tested for initials
    type text: string

    :return: a boolean that returns True if there is at least one initial in the string, False otherwise

    """

    list_words = text.split(' ')
    list_init = [mot for mot in list_words if len(mot) == 1]
    if list_init:
        init = True
    else:
        init = False

    return init


def get_multinames_info(name, separator=","):
    """   This function tests the number of words separated by the separator in input

    param name: the string to be tested for number of words
    type name: string
    param separator: the character that is meant to separate the words in the string
    type separator: string

    :return: a boolean that returns True if there are more than two words seperated, False otherwise

    """

    if len(name.split(separator)) > 2:
        multin = True
    else:
        multin = False

    return multin


def get_dict_cluster_name(fam_table, name_var, function_dict_score):
    """   This function clusterises all participants of the  input table (corresponding to one family)

    param fam_table: table with participant's names of the same family
    type fam_table: dataframe
    param name_var: name of the variable containing the names to deduplicate
    type name_var: string
    param function_dict_score: the function to be used for clustering
    type function_dict_score: function

    :return: a dictionary where the key is the index of table and the value is the new deduplicated name

    """

    family_table = fam_table.copy()
    dict_clusters = {}
    # get_clean_name est défini dans le fichier 'text_functions.py'
    # on crée les variables 'init' et 'multin' qui sont des indicateurs utilisés dans les comparaisons entre noms
    family_table['name_clean'] = family_table[name_var].apply(tf.get_clean_name,
                                                              list_pattern_to_remove=['^dr ', '^mme ', ' mme '])
    family_table['init'] = family_table['name_clean'].apply(get_initial_info)
    family_table['multin'] = family_table[name_var].apply(get_multinames_info)
    family_table['len_name'] = family_table['name_clean'].apply(len)
    # Les premières chaînes vont être sélectionnées en premier pour servir de référent de cluster.
    # il faut donc que ces premiers référents se rapprochent le plus possible du nom 'commun' de la personne :
    # on trie donc pour avoir la plus courte chaîne, hors cas d'initiales et de multiples noms qu'on traite en dernier
    # en effet, si on garde un nom long en premier dans le cluster, avec plusieurs prénoms, les autres ne vont pas
    # matcher avec.
    family_table = family_table.sort_values(['init', 'multin', 'len_name'])
    for index, row in family_table.iterrows():
        name1 = row['name_clean']
        namesource = row[name_var]
        dict_score = function_dict_score(dict_clusters, namesource)
        test_cluster = True
        if len(dict_score):
            best_occur = get_max_key(dict_score)
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


def deduplicate_part_one_family(part_table, famtype, name_var, family, function_dict_score):
    """   This function gets from a participant table an axtract table with deduplicated names
    for the family 'family' specified in input

    param part_table: table with participants informations - must have the columns corresponding
    to the input variables famtype and name_var
    type part_table: dataframe
    param famtype: name of the variable containing the type of family in which we want to assimilate
    the individuals (can be simple family 'docdb_family_id' or large family 'inpadoc_family_id')
    type famtype: string
    param name_var: name of the variable containing the names to deduplicate
    type name_var: string
    param family: the current family identifier on which the deduplication is being done
    type family: string
    param function_dict_score: the function to be used for clustering
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


def deduplicate_part_by_fam(part_table, famtype, name_var, function_dict_score):
    """   This function gets from a participant table a table with deduplicated names

    param part_table: table with participants informations - must have the columns corresponding
    to the input variables famtype and name_var
    type part_table: dataframe
    param famtype: name of the variable containing the type of family in which we want to assimilate
    the individuals (can be simple family 'docdb_family_id'
    or large family 'inpadoc_family_id')
    type famtype: string
    param name_var: name of the variable containing the names to deduplicate
    type name_var: string
    param function_dict_score: the function to be used for clustering
    type function_dict_score: function

    :return: a dataframe with all deduplicated names by family

    """

    list_families = set(part_table[famtype])
    deduplicated_table_list = []
    count = 0
    print(dt.now())
    # on va clusteriser par famille, donc on boucle sur les familles
    # la variable famtype permet de clusteriser au choix sur les familles simples
    # 'docdb_family_id' ou sur les familles élargies 'inpadoc_family_id'
    for family in list_families:
        count += 1
        # cette fonction clusterise sur une famille
        family_table = deduplicate_part_one_family(part_table, famtype, name_var, family, function_dict_score)
        # on enregistre les résultat dasn une liste contentant toutes les tables dédoublonnées
        deduplicated_table_list.append(family_table)
        # pour savoir où on en est, tous les 3000 familles, on affiche un message
        if count % 3000 == 0:
            print('Traitement de la famille numéro ' + str(count))
    print(dt.now())
    # a la fin on concatène tous les tableaux résultats correspondant à toutes les familles
    deduplicated_ind = pd.concat(deduplicated_table_list)

    return deduplicated_ind


def get_dict_score_entp_identicals(dict_clusters, name):
    """   This function tries different methods to match a person name from the input with the different occurences of
    names loaded in
    'dict_clusters' dictionary and gets the best score - This function is adapted to the entreprises, not the persons

    param dict_clusters: a dictionary where the key is a name and the value is another dictionary where keys are the
    index of original person's table and
    the occurences of cluster name referent
    type dict_clusters: dictionnary
    param name: name to match
    type name: string

    :return: a dictionnary where the key is the name to deduplicate and the value is the best score

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


def clean_siren(string):
    """   This function verify if the SIREN in input has 9 characters with digits after cleaning (spaces, punctuations)

    param string: SIREN to clean
    type string: string

    :return:  the given SIREN if he's OK, None otherwise

    """

    siren_regex = re.compile(r'^\d{9,9}$')
    siren_clean = tf.remove_all_spaces(tf.remove_punctuations(string))
    if not pd.isnull(siren_clean):
        mo = siren_regex.search(siren_clean.replace(" ", ""))
        if (mo is None) or (siren_clean == '000000000'):
            return None
        else:
            return siren_clean
    else:
        return None


def extrapolation(table_to_fill, famtype, var_to_fill, var_to_match):
    """
    This function fills the variable 'var_to_fill' when the value is missing in the table 'table_to_fill' for the same
    'famtype' and the same 'var_to_match'

    param name the name to get pretty
    type name string

    return  a string, the pretty name  - we keep "-" for exemple for double names ('Jean-Pierre)

    """

    table_var_filled = table_to_fill.loc[
        table_to_fill[var_to_fill] != '', [famtype, var_to_fill, var_to_match]].drop_duplicates() \
        .rename(columns={var_to_fill: 'var_completed'})
    table_ss_doublon_count = table_var_filled.groupby([famtype, var_to_match]).count()
    table_ss_doublon = table_ss_doublon_count[table_ss_doublon_count['var_completed'] == 1].reset_index() \
        .drop(columns={'var_completed'})
    table_var_filled_ss_db = pd.merge(table_var_filled, table_ss_doublon, how='inner', on=[famtype, var_to_match])
    table_extrapol = pd.merge(table_to_fill, table_var_filled_ss_db, how='left', on=[famtype, var_to_match])
    table_extrapol[var_to_fill] = table_extrapol[var_to_fill].replace(np.nan, '')
    table_extrapol[var_to_match] = table_extrapol[var_to_match].replace(np.nan, '')
    table_extrapol[var_to_fill] = np.where(table_extrapol[var_to_fill] == '', table_extrapol['var_completed'],
                                           table_extrapol[var_to_fill])
    table_extrapol = table_extrapol.drop(columns={'var_completed'})
    for col in table_extrapol.columns:
        table_extrapol[col] = table_extrapol[col].fillna('')

    return table_extrapol


def get_max_occurences_from_list(one_list):
    max_occurr = max(one_list, key=one_list.count)
    return max_occurr


def affectation_most_occurences(table_to_fill, famtype, var_to_fill, var_to_match):
    # dans une famille, pour un même nom, quand il y a plusieurs pays, on prend celui le plus fréquent

    table_var_filled = table_to_fill.groupby([famtype, var_to_match]).agg({var_to_fill: list}).reset_index()
    table_var_filled['var_selected'] = table_var_filled[var_to_fill].apply(get_max_occurences_from_list)
    table_filled_ss_doublon = table_var_filled[table_var_filled['var_selected'] != ''].drop(columns={var_to_fill})
    table_extrapol = pd.merge(table_to_fill, table_filled_ss_doublon, how='left', on=[famtype, var_to_match])
    for col in table_extrapol.columns:
        table_extrapol[col] = table_extrapol[col].fillna('')
    table_extrapol[var_to_fill] = table_extrapol['var_selected']
    for col in table_extrapol.columns:
        table_extrapol[col] = table_extrapol[col].fillna('')
    table_extrapol = table_extrapol.drop(columns={'var_selected'})

    return table_extrapol


def main():
    part = pd.read_csv('part.csv', sep='|', dtype=types.part_init_types)

    for col in part.columns:
        part[col] = part[col].replace(np.nan, '')

    part_entp_init = part[(part['type'] == 'pm') & (part['isascii'])].copy()

    # on dédoublonne une première fois sur les entp identiques
    # on part des ajouts d'entreprises sur familles existentes et nouvelles familles

    entp_to_deduplicate = get_part_to_deduplicate_by_fam(part_entp_init, 'inpadoc_family_id', 'name_source', 'old_name')

    deduplicated_entp = deduplicate_part_by_fam(entp_to_deduplicate, 'inpadoc_family_id', 'name_source',
                                                get_dict_score_entp_identicals)

    # table complète des participants dedoublonnée

    part_entp = part_entp_init.merge(
        deduplicated_entp[['inpadoc_family_id', 'name_source', 'new_name']].drop_duplicates(),
        on=['inpadoc_family_id', 'name_source'], how='left')

    for col in part_entp.columns:
        part_entp[col] = part_entp[col].fillna('')
        part_entp['name_corrected'] = np.where(part_entp['new_name'] == '', part_entp['old_name'],
                                               part_entp['new_name'])
    part_entp['name_corrected'] = part_entp['name_corrected'].apply(tf.put_title_format)

    # on sélectionne les identifiants participants de la dernière version pour rechercher les siren correspondant

    siren_inpi_brevet = pd.read_csv('siren_inpi_brevet.csv', sep='|',
                                    dtype={'siren': str, 'nom': str, 'numpubli': str})
    siren_inpi_generale = pd.read_csv('siren_inpi_generale.csv', sep='|',
                                      dtype={'siren': str, 'nom': str, 'numpubli': str})

    part_entp1 = part_entp.merge(siren_inpi_brevet.rename(columns={'siren': 'siren_nouv'}), how='left',
                                 left_on=['publication_number', 'name_source'], right_on=['numpubli', 'nom']).drop(
        columns={'nom', 'numpubli'}).drop_duplicates()
    # on élimine les anciens siren erronés
    part_entp1['siren'] = part_entp1['siren'].apply(clean_siren)

    part_entp1['siren_nouv'] = part_entp1['siren_nouv'].fillna('')
    part_entp1['siren_nouv'] = part_entp1['siren_nouv'].astype(str)
    part_entp1['siren'] = np.where(part_entp1['siren'] == '', part_entp1['siren_nouv'], part_entp1['siren'])
    part_entp2 = part_entp1.drop(columns={'siren_nouv'}).drop_duplicates().copy()

    # Partie non effectuée - A faire
    # - recherche semi_manuelle avec l'aide de la machine à données des SIREN manquants sur la dernière année

    # extrapolations  -  les fonctions sont dans le fichier 'extrapolation_functions.py'

    # si on a le même nom dans la même famille, on extrapole le pays
    part_entp3 = extrapolation(part_entp2, 'inpadoc_family_id', 'country_corrected', 'name_corrected')

    # si on a le même nom dans la même famille, on extrapole le siren
    part_entp4 = extrapolation(part_entp3, 'inpadoc_family_id', 'siren', 'name_corrected')

    # pour une liste de psn_id qui sont sûrs, on va extrapoler
    # - on extrapole mais on ne prend en compte les changements que pour les
    # psn_id dans la liste

    psn_id_valids = pd.read_csv('psn_id_valids.txt', sep='|', dtype={'psn_id': str}).drop_duplicates()
    set_psn_id_valids = set(psn_id_valids['psn_id'])

    part_entp4['name_psn'] = part_entp4['name_corrected']

    part_entp5 = affectation_most_occurences(part_entp4, 'inpadoc_family_id', 'name_psn', 'psn_id')

    part_entp5['name_corrected'] = np.where(part_entp5['psn_id'].isin(set_psn_id_valids), part_entp5['name_psn'],
                                            part_entp5['name_corrected'])

    part_entp5['siren_psn'] = part_entp5['siren']
    part_entp5['fam'] = 1

    part_entp_final = affectation_most_occurences(part_entp5, 'fam', 'siren_psn', 'psn_id')

    part_entp_final['siren'] = np.where(part_entp_final['psn_id'].isin(set_psn_id_valids), part_entp_final['siren_psn'],
                                        part_entp_final['siren'])

    # on ne prend pas en compte les extrapolations de siren par psn_id pour les pays étrangers

    part_entp_final['siren'] = np.where(part_entp_final['country_corrected'].isin({'FR', ''}),
                                        part_entp_final['siren'], '')

    part_entp_final.to_csv('part_entp_final.csv', sep='|', index=False)


if __name__ == '__main__':
    main()
