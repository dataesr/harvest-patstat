#!/usr/bin/env python
# coding: utf-8

import os
import time
import concurrent.futures

import numpy as np
import pandas as pd
import requests
import logging
import threading
import sys
from retry import retry

from utils import swift

from patstat import p05b_clean_participants as cp
from patstat import dtypes_patstat_declaration as types
from patstat import text_functions as tf

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


# Ensuite on va choisir pour chaque cluster mis en avant dans chaque famille,
# le nom qu'on va garder. On ne fait la sélection que sur les nouveaux clusters

def get_logger(name):
    """
    This function helps to follow the execution of the parallel computation.
    """
    loggers = {}
    if name in loggers:
        return loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    loggers[name] = logger
    return loggers[name]


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    prct10 = int(round(len(df) / 100, 0))
    dict_nb = {}
    df = df.reset_index().drop(columns="index")
    indices = list(df.index)
    listes_indices = [indices[i:i + prct10] for i in range(0, len(indices), prct10)]
    i = 1
    for liste in listes_indices:
        min_ind = np.min(liste)
        max_ind = np.max(liste) + 1
        dict_nb["df" + str(i)] = df.iloc[min_ind: max_ind, :]
        i = i + 1

    return dict_nb


def res_futures(dict_nb: dict, query) -> pd.DataFrame:
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    global jointure
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=101, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                data = future.result()
                res.append(data)
                jointure = pd.concat(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)

    return jointure


def select_nice_name(name_table: pd.DataFrame, family_var: str, source_name_var: str, cluster_name_var: str,
                     office_var: str) -> pd.DataFrame:
    """   This function selects from a person participants table already clusterised the name to be
    kept among the different occurences from the same cluster

    param name_table: table with the participant names already clusterised
    type name_table: dataframe
    param family_var: family variable on which the clusterisation has been done
    type family_var: string
    param source_name_var: Original name of the person (PATSTAT's version)
    type source_name_var: string
    param cluster_name_var: The name of the cluster in the family (2 persons with the same cluster_name are meant to be
    the same)
    type cluster_name_var: string
    param office_var: the office where the application comes from
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
    """
    Get the most common name in a list
    :param one_list: list with names
    :return:
    """
    max_occurr = max(one_list, key=one_list.count)
    return max_occurr


def affectation_most_occurences(table_to_fill: pd.DataFrame, famtype: str, var_to_fill: str,
                                var_to_match: str) -> pd.DataFrame:
    """

    :param table_to_fill: df with info we want to complete/correct
    :param famtype: patent family
    :param var_to_fill: variable we want to complete/correct
    :param var_to_match: participant name
    :return: df where for each participant name in a family only the most common value of the var_to_fill is kept
    """
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


@retry(tries=3, delay=5, backoff=5)
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
    headers = {"Authorization": f"Basic {os.getenv('DATAESR')}"}
    proba = 0.0
    r = requests.get(url_sexe_request + name, headers=headers)
    if r.status_code != 200:
        print(f"{name}: Failed while trying to access the URL", flush=True)
        pass
        # raise ConnectionError("Failed while trying to access the URL")
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


def get_sex_from_name_set(df_nom: pd.DataFrame) -> pd.DataFrame:
    """   This function uses a dataesr API to get sex from name. It takes a set of names and returns a dataframe with
    API variables results for each name in the set

    param name: the set of names to get sex from
    type name: set

    :return:  a dataframe with the original name, and the results of sex API

    """
    logger = get_logger(threading.current_thread().name)
    logger.info("start query gender")
    dict_nom = {}
    count = 0
    start_time = time.time()
    for row in df_nom.itertuples():
        name = row.name_corrige
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

    logger.info("end query gender")
    return sex_table


def get_clean_ind():
    # set working directory
    os.chdir(DATA_PATH)
    part_ind = pd.read_csv('part_ind.csv', sep='|', dtype=types.part_init_types,
                           encoding="utf-8", engine="python")

    part_ind["name_corrige"] = part_ind["name_corrige"].fillna("")

    part_ind_name_nice = select_nice_name(part_ind.loc[part_ind['name_corrige'] != ''],
                                          'docdb_family_id', 'name',
                                          'name_corrige', 'appln_auth')

    part_individuals = part_ind.merge(part_ind_name_nice[['docdb_family_id', 'name_corrige']],
                                      on=['docdb_family_id', 'name_corrige'], how='left')
    for col in part_individuals.columns:
        part_individuals[col] = part_individuals[col].fillna('')

    # pour les anciens clusters, on garde le old_name, qui était l'ancien nom du cluster de la version précédente

    part_individuals.loc[part_individuals["name_corrige"] == "", "name_corrige"] = part_individuals.loc[
        part_individuals["name_corrige"] == "", "old_name"]

    # part_individuals['name_corrected'] = np.where(part_individuals['new_name'] == '', part_individuals['old_name'],
    #                                               part_individuals['new_name'])

    part_individuals['name_corrige'] = part_individuals['name_corrige'].apply(clean_name_to_nice)

    part_individuals = affectation_most_occurences(part_individuals, 'docdb_family_id', 'country_corrected',
                                                   'name_corrige')

    part_individuals_fin = part_individuals.copy()
    # Enfin on récupère le sexe par API pour les nouvelles personnes

    table_to_get_sex = part_individuals.loc[
        (part_individuals['islatin']) & (part_individuals['name_corrige'] != '')].copy()

    df_get_sex = pd.DataFrame(data={"name_corrige": table_to_get_sex["name_corrige"].unique()})

    # sex_table = get_sex_from_name_set(df_get_sex)

    sub_get_sex = subset_df(df_get_sex)

    sex_table = res_futures(sub_get_sex, get_sex_from_name_set)

    sex_table.to_csv('sex_table.csv', sep='|', index=False)

    sex_table = pd.read_csv('sex_table.csv', sep='|')

    part_individuals_fin = part_individuals.merge(sex_table, left_on='name_corrige', right_on='name', how='left')

    for col in part_individuals_fin.columns:
        part_individuals_fin[col] = part_individuals_fin[col].fillna('')
        part_individuals_fin[col] = part_individuals_fin[col].astype(str)

    part_individuals_fin.loc[part_individuals["sexe"] == "", "sexe"] = part_individuals_fin.loc[
        part_individuals["sexe"] == "", "sex"]

    # part_individuals_fin['sexe'] = np.where(part_individuals_fin['sexe'] == '', part_individuals_fin['sex'],
    #                                        part_individuals_fin['sexe'])

    part_individuals_fin.to_csv('part_individuals.csv', sep='|', index=False)
    swift.upload_object('patstat', 'part_individuals.csv')
