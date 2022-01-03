#!/usr/bin/env python
# coding: utf-8

import json
import os
import re

import numpy as np
import pandas as pd
import requests

import config_emmanuel
import dtypes_patstat_declaration as types
import text_functions as tf

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"
COOKIE_NAME = "JSESSIONID"

# set working directory
os.chdir(DATA_PATH)


def clean_siren(string: str) -> object:
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


def multi_name(df_part: pd.DataFrame) -> pd.DataFrame:
    """

    :param df_part:
    :return:
    """
    df = df_part.copy()
    df["name_source_list"] = df["name_source"].copy()
    df["name_source_list"] = df["name_source_list"].apply(lambda a: re.sub(r"^1\)\s?", "", a)).copy()
    df["name_source_list"] = df["name_source_list"].apply(lambda a: re.split(r";?\s?\d\)\s?", a)).copy()

    df2 = df[["key_appln_nr_person",
              "publication_number",
              "psn_id",
              "name_source",
              "name_source_list", "siren"]].copy()

    df3 = df2.explode("name_source_list").copy()

    return df3


# matcher SIREN

def extrapolation(table_to_fill: pd.DataFrame, famtype: str, var_to_fill: str, var_to_match: str) -> pd.DataFrame:
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


def login_inpi():
    headers = {'Login': config_emmanuel.P07A_LOGIN, 'Password': config_emmanuel.P07A_PWD}
    r = requests.post(
        url='https://opendata-rncs.inpi.fr/services/diffusion/login',
        headers=headers
    )
    if r.status_code != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed")
    sess_id = r.headers.get("Set-Cookie").split(";")[0].replace(COOKIE_NAME + "=", '')
    return sess_id


def logout_inpi(sess_id):
    return requests.post("https://opendata-rncs.inpi.fr/services/diffusion/logout",
                         cookies={COOKIE_NAME: sess_id})


def request_inpi(url, sess_id):
    return requests.get(url, cookies={COOKIE_NAME: sess_id})


def json_df(res):
    data = json.loads(res.text)
    df_json = pd.json_normalize(data)
    df_json = df_json.drop_duplicates()

    return df_json


def select_denom(df, denom):
    df2 = df[df["denominationSociale"] == denom]
    df2 = df2.drop_duplicates()

    return df2


def get_missing_siren(url, item, sess_id):
    url = url + item
    response = request_inpi(url, sess_id)
    df_json = json_df(response)
    df_select = select_denom(df_json, item)

    return df_select


def all_missing_siren(url, ls_denom, sess_id):
    for item in ls_denom:
        url = url + item
        response = request_inpi(url, sess_id)
        print(item)
        print(response.text)

    return response


def main():
    part_entp = pd.read_csv('part_entp.csv', sep='|', dtype=types.part_entp_types)

    for col in part_entp.columns:
        part_entp[col] = part_entp[col].fillna('')

    part_multi = multi_name(part_entp)

    # on sélectionne les identifiants participants de la dernière version pour rechercher les siren correspondant

    siren_inpi_brevet = pd.read_csv('siren_inpi_brevet.csv', sep='|',
                                    dtype={'siren': str, 'nom': str, 'numpubli': str})
    siren_inpi_generale = pd.read_csv('siren_inpi_generale.csv', sep='|',
                                      dtype={'siren': str, 'nom': str, 'numpubli': str})

    part_entp1 = part_multi.merge(siren_inpi_brevet.rename(columns={'siren': 'siren_nouv'}), how='left',
                                  left_on=['publication_number', 'name_source_list'],
                                  right_on=['numpubli', 'nom']).drop(
        columns={'nom', 'numpubli'}).drop_duplicates()
    # on élimine les anciens siren erronés
    part_entp1['siren'] = part_entp1['siren'].apply(clean_siren)

    part_entp1['siren_nouv'] = part_entp1['siren_nouv'].fillna('')
    part_entp1['siren_nouv'] = part_entp1['siren_nouv'].astype(str)
    part_entp1['siren'] = np.where(part_entp1['siren'] == '', part_entp1['siren_nouv'], part_entp1['siren'])
    part_entp2 = part_entp1.drop(columns={'siren_nouv'}).drop_duplicates().copy()

    part_entp3 = pd.merge(part_entp.drop(columns="siren"),
                          part_entp2, on=["key_appln_nr_person",
                                          "publication_number",
                                          "psn_id",
                                          "name_source"],
                          how="left")

    table_siren = part_entp3[["psn_name", "name_source", "name_source_list", "address_source", "siren"]].groupby(
        ["psn_name", "name_source", "name_source_list", "address_source"]).nunique().reset_index()

    missing_siren = part_entp3[part_entp3["siren"].isna()][
        ["key_appln_nr_person", "psn_name", "name_source", "name_source_list", "address_source"]].copy()

    missing_siren = missing_siren[missing_siren["address_source"]!=""]

    missing_siren_table = missing_siren.groupby(["psn_name", "name_source", "name_source_list", "address_source"]).nunique().reset_index().rename(columns={"key_appln_nr_person": "occurences"})
    missing_siren_table = missing_siren_table.sort_values("occurences", ascending=False)

    siren = pd.merge(missing_siren, siren_inpi_generale, left_on="name_source", right_on="nom", how="left")

    siren2 = siren[siren["siren"].isna()][["name_source_list"]].drop_duplicates()
    siren2["uri"] = "https://opendata-rncs.inpi.fr/services/diffusion/imrs-saisis/find?denominationSociale=" + siren2[
        "name_source_list"]
    siren2["length"] = siren2["uri"].apply(lambda a: len(a))
    list_denm = siren2.name_source_list.tolist()

    session_id = login_inpi()
    url = "https://opendata-rncs.inpi.fr/services/diffusion/imrs-saisis/find?denominationSociale="
    response = request_inpi(url, session_id)
    dataframe = json_df(response)
    dataframe = select_denom(dataframe, "")
    print(response.text)
    manquant = all_missing_siren(url, list_denm, session_id)
    logout_inpi(session_id)

    # Partie non effectuée - A faire
    # - recherche semi_manuelle avec l'aide de la machine à données des SIREN manquants sur la dernière année

    # extrapolations  -  les fonctions sont dans le fichier 'extrapolation_functions.py'

    # si on a le même nom dans la même famille, on extrapole le pays
    part_entp4 = extrapolation(part_entp3, 'inpadoc_family_id', 'country_corrected', 'name_corrected')

    # si on a le même nom dans la même famille, on extrapole le siren
    part_entp5 = extrapolation(part_entp4, 'inpadoc_family_id', 'siren', 'name_corrected')

    # pour une liste de psn_id qui sont sûrs, on va extrapoler
    # - on extrapole mais on ne prend en compte les changements que pour les
    # psn_id dans la liste

    psn_id_valids = pd.read_csv('psn_id_valids.txt', sep='|',
                                dtype={'psn_id': str}).drop_duplicates()
    set_psn_id_valids = set(psn_id_valids['psn_id'])

    part_entp5['name_psn'] = part_entp5['name_corrected']

    part_entp6 = affectation_most_occurences(part_entp5, 'inpadoc_family_id', 'name_psn', 'psn_id')

    part_entp6['name_corrected'] = np.where(part_entp6['psn_id'].isin(set_psn_id_valids), part_entp6['name_psn'],
                                            part_entp6['name_corrected'])

    part_entp6['siren_psn'] = part_entp6['siren']
    part_entp6['fam'] = 1

    part_entp_final = affectation_most_occurences(part_entp6, 'fam', 'siren_psn', 'psn_id')

    part_entp_final['siren'] = np.where(part_entp_final['psn_id'].isin(set_psn_id_valids), part_entp_final['siren_psn'],
                                        part_entp_final['siren'])

    # on ne prend pas en compte les extrapolations de siren par psn_id pour les pays étrangers

    part_entp_final['siren'] = np.where(part_entp_final['country_corrected'].isin({'FR', ''}),
                                        part_entp_final['siren'], '')

    part_entp_final.to_csv('part_entp_final.csv', sep='|', index=False)


if __name__ == '__main__':
    main()
