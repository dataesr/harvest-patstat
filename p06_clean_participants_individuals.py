#!/usr/bin/env python
# coding: utf-8

import os
import time

import numpy as np
import pandas as pd
import requests

import clean_participants as cp
import config_emmanuel
import dtypes_patstat_declaration as types
import text_functions as tf

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"

# set working directory
os.chdir(DATA_PATH)


# Ensuite on va choisir pour chaque cluster mis en avant dans chaque famille,
# le nom qu'on va garder. On ne fait la sélection que sur les nouveaux clusters

def select_nice_name(name_table: pd.DataFrame, family_var: str, source_name_var: str, cluster_name_var: str,
                     office_var: str) -> pd.DataFrame:
    """   This function selects from a person participants table already clusterised the name to be
    kept among the different occurences from the same cluster

    param name_table: table with participant's names already clusterise
    type name_table: dataframe
    param family_var: family variable on which the clusterisation has benn done
    type family_var: string
    param source_name_var: Original name of the person (patsta's version)
    type source_name_var: string
    param cluster_name_var: The name of the cluster in the family (2 persons with the same cluster_name are meant to be
    the same)
    type cluster_name_var: string
    param office_var: the office from where comes the application
    type office_var: string

    :return:  a dataframe with one cluster name selected for each family

    """

    # on ne va faire tourner la sélection des bons noms que pour les noms qui ont été clusterisés dans cette version :
    # donc 'cluster_name_var'
    # n'est pas nul
    name_to_select = name_table[name_table[cluster_name_var] != '']
    df_select_name = name_to_select[
        [family_var, cluster_name_var, office_var, source_name_var]].copy().drop_duplicates()
    # on ajoute les variables de tri : le nom contient des initiales, de quel office provient le dossier ?
    df_select_name['init'] = df_select_name[source_name_var].apply(tf.remove_punctuations).apply(
        tf.remove_multiple_spaces) \
        .apply(tf.remove_first_end_spaces).apply(cp.get_initial_info)
    # on retient en dernier les informations des dossiers des offices russes et chinois pour le choix du bon nom
    # (ce sont des traductions automatiques
    # très erronnées)
    df_select_name['order'] = np.where(df_select_name[office_var].isin(['WO', 'EP', 'FR', 'CA', 'ES', 'US']),
                                       np.where(df_select_name['init'] == 0, 0, 1),
                                       np.where(df_select_name[office_var].isin(['RU', 'CN']), 99,
                                                np.where(df_select_name['init'] == 0, 2, 3)))
    # enfin on trie en fonction du numéro de sélection et on prend le premier pour chaque cluster de la famille
    df_name_nice = df_select_name.sort_values([family_var, cluster_name_var, 'order']) \
        .groupby([family_var, cluster_name_var]).first().reset_index()

    return df_name_nice


# Enfin on prend une belle orthographe
def clean_name_to_nice(name: str) -> str:
    """   This function transforms a name in a nice one for publication

    param name: the name to get pretty
    type name: string

    :return:  a string, the pretty name  - we keep "-" for exemple for double names ('Jean-Pierre)

    """

    pattern_to_remove = '^dr '
    s1 = tf.remove_words(name, pattern_to_remove)
    string_clean = tf.remove_first_end_spaces(
        tf.remove_multiple_spaces(tf.remove_punctuations(s1.lower(), list_punct_except=["'", "-", "."])))

    return string_clean.title()


# On corrige le pays :
# on considère qu'un inventeur n'est rattaché qu'à un seul pays pour une invention donnée (même famille)
# - Il y a beaucoup d'erreurs dans les pays : ce sont sensés être les pays de l'adresse de l'inventeur,
# mais dans certains offices cette information est mal renseignée et ils mettent la nationalité de
# l'inventeur au lieu du pays de résidence. On va donc affecter pour une famille et un nom donné le même pays :
# celui qui a le plus d'occurences.

def get_max_occurences_from_list(one_list: list) -> str:
    max_occurr = max(one_list, key=one_list.count)
    return max_occurr


def affectation_most_occurences(table_to_fill: pd.DataFrame, famtype: str, var_to_fill: str,
                                var_to_match: str) -> pd.DataFrame:
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


def get_sex_proba_from_name(name: str):
    """   This function uses a dataesr API to get sex from name : it can be a first name or complete name. It gets the
    first occurence : best result

    param name: the name to get sex from
    type name: string

    :return:  4 variables : 'sexe', 'proba' (vaut entre 0 et 1), 'occurences' (le nombre d'occurences dans la base de
    référence du prénom identifié dans la chaîne) et en dernier le résultat de l'appel API. (erreur, détecté,
    non détecté)

    """

    url_sexe_request = "http://185.161.45.213/persons/persons/_gender?q="
    headers = {"Authorization": f"{config_emmanuel.P06}"}
    proba = 0.0
    r = requests.get(url_sexe_request + name, headers=headers)
    if r.status_code != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        if (r.json()['status']) == 'detected':
            prem_occurr = r.json()['data'][0]['firstnames_detected'][0]
            sexe = prem_occurr['gender']
            if sexe == 'M':
                proba = round(prem_occurr['proportion_M'])
            if sexe == 'F':
                proba = prem_occurr['proportion_F']
            occurrences = prem_occurr['nb_occurrences_with_gender']
            return sexe, proba, occurrences, 'detected'
        else:
            return "", "", "", "not_detected"


def get_sex_from_name_set(set_nom: set) -> pd.DataFrame:
    """   This function uses a dataesr API to get sex from name. It takes a set of names and returns a dataframe with
    API variables results for each name in the set

    param name: the set of names to get sex from
    type name: set

    :return:  a dataframe with the original name, and the results of sex API

    """

    dict_nom = {}
    set_nom = set_nom.difference(set(dict_nom))
    count = 0
    start_time = time.time()
    for name in set_nom:
        dict_tmp = {'sex': (get_sex_proba_from_name(name))[0], 'proba': (get_sex_proba_from_name(name))[1],
                    'occurence': (get_sex_proba_from_name(name))[2], 'error': (get_sex_proba_from_name(name))[3]}
        dict_nom[name] = dict_tmp
        count += 1
        if count % 1000 == 0:
            print("Nombre de requêtes de nom effectuées : " + str(count) + " --  ")
            print("--- %s seconds ---" % (time.time() - start_time))
        if count % 5000 == 0:
            pd.DataFrame.from_dict(dict_nom, orient='index').reset_index() \
                .rename(columns={'index': 'name'}).to_csv('sex_names.csv', sep='|', index=False)
    sex_table = pd.DataFrame.from_dict(dict_nom, orient='index').reset_index().rename(columns={'index': 'name'})
    return sex_table


def main():
    part_ind = pd.read_csv('part_ind.csv', sep='|', dtype=types.part_init_types)

    part_ind_name_nice = select_nice_name(part_ind[part_ind['new_name'] != ''], 'inpadoc_family_id', 'name_source',
                                          'new_name', 'appln_auth')

    part_individuals = part_ind.merge(part_ind_name_nice[['inpadoc_family_id', 'new_name']],
                                      on=['inpadoc_family_id', 'new_name'], how='left')
    for col in part_individuals.columns:
        part_individuals[col] = part_individuals[col].fillna('')

    # pour les anciens clusters, on garde le old_name, qui était l'ancien nom du cluster de la version précédente

    part_individuals['name_corrected'] = np.where(part_individuals['new_name'] == '', part_individuals['old_name'],
                                                  part_individuals['new_name'])

    part_individuals['name_corrected'] = part_individuals['name_corrected'].apply(clean_name_to_nice)

    part_individuals = affectation_most_occurences(part_individuals, 'inpadoc_family_id', 'country_corrected',
                                                   'name_corrected')

    # Enfin on récupère le sexe par API pour les nouvelles personnes

    table_to_get_sex = part_individuals[
        (part_individuals['isascii']) & (part_individuals['new_name'] != '')].copy()
    set_to_get_sex = set(table_to_get_sex['name_corrected'])

    sex_table = get_sex_from_name_set(set_to_get_sex)

    sex_table.to_csv('sex_table.csv', sep='|', index=False)

    sex_table = pd.read_csv('sex_table.csv', sep='|')

    part_individuals_fin = part_individuals.merge(sex_table, left_on='name_corrected', right_on='name', how='left')

    for col in part_individuals_fin.columns:
        part_individuals_fin[col] = part_individuals_fin[col].fillna('')
        part_individuals_fin[col] = part_individuals_fin[col].astype(str)

    part_individuals_fin['sexe'] = np.where(part_individuals_fin['sexe'] == '', part_individuals_fin['sex'],
                                            part_individuals_fin['sexe'])

    part_individuals_fin.to_csv('part_individuals.csv', sep='|', index=False)


if __name__ == '__main__':
    main()
