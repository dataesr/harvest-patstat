#!/usr/bin/env python
# coding: utf-8

import os
import re

import numpy as np
import pandas as pd

from patstat import dtypes_patstat_declaration as types
from patstat import text_functions as tf

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

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


def main():
    part_entp = pd.read_csv('part_entp.csv', sep='|', dtype=types.part_entp_types)

    for col in part_entp.columns:
        part_entp[col] = part_entp[col].fillna('')

    # on sélectionne les identifiants participants de la dernière version pour rechercher les siren correspondant

    siren_inpi_brevet = pd.read_csv('siren_inpi_brevet.csv', sep='|',
                                    dtype={'siren': str, 'nom': str, 'numpubli': str})
    # siren_inpi_generale = pd.read_csv('siren_inpi_generale.csv', sep='|',
    #                                   dtype={'siren': str, 'nom': str, 'numpubli': str})

    part_entp1 = part_entp.merge(siren_inpi_brevet.rename(columns={'siren': 'siren_nouv'}), how='left',
                                 left_on=['publication_number', 'name_source'],
                                 right_on=['numpubli', 'nom']).drop(
        columns={'nom', 'numpubli'}).drop_duplicates()
    # on élimine les anciens siren erronés
    part_entp1['siren'] = part_entp1['siren'].apply(clean_siren)

    part_entp1['siren_nouv'] = part_entp1['siren_nouv'].fillna('')
    part_entp1['siren_nouv'] = part_entp1['siren_nouv'].astype(str)
    part_entp1['siren'] = np.where(part_entp1['siren'].isna(), part_entp1['siren_nouv'], part_entp1['siren'])
    part_entp2 = part_entp1.drop(columns='siren_nouv').drop_duplicates().copy()

    part_entp2["siren2"] = part_entp2["siret"].apply(lambda a: a[0:9])
    part_entp2["siren3"] = np.where(part_entp2["siren"] == "", part_entp2["siren2"], part_entp2["siren"]).copy()
    part_entp2["siren"] = part_entp2["siren3"].copy()
    part_entp2 = part_entp2.drop(columns=["siren2", "siren3"])

    # Identification des participants dont il manque les SIREN pour recherche semi_manuelle

    # part_missing = part_entp2.loc[
    #     (part_entp2["address_source"] != "") & (part_entp2["siren"].isna()) & (part_entp2["siret"] == "") & (
    #             part_entp2["id_paysage"] != "") & (part_entp2["rnsr"] != "") & (part_entp2["grid"] != "") & (
    #             part_entp2["idref"] != "") & (part_entp2["oc"] != "") & (part_entp2["ror"] != "")]
    #
    # missing_siren = part_missing[["psn_name", "psn_id", "name_source", "address_source", "key_appln_nr_person"]]
    # missing_siren["occurences"] = part_missing["key_appln_nr_person"].copy()
    #
    # missing_siren_table = missing_siren.groupby(
    #     ["psn_name", "psn_id", "name_source", "address_source", "key_appln_nr_person"]).nunique().reset_index()
    # missing_siren_table = missing_siren_table.sort_values("occurences", ascending=False)
    #
    # siren = pd.merge(missing_siren, siren_inpi_generale, left_on="name_source", right_on="nom", how="left")

    # extrapolations  -  les fonctions sont dans le fichier 'extrapolation_functions.py'

    # si on a le même nom dans la même famille, on extrapole le pays
    part_entp3 = extrapolation(part_entp2, 'inpadoc_family_id', 'country_corrected', 'name_corrected')

    # si on a le même nom dans la même famille, on extrapole le siren
    part_entp4 = extrapolation(part_entp3, 'inpadoc_family_id', 'siren', 'name_corrected')

    # pour une liste de psn_id qui sont sûrs, on va extrapoler
    # - on extrapole mais on ne prend en compte les changements que pour les
    # psn_id dans la liste

    psn_id_valids = pd.read_csv('psn_id_valids.txt', sep='|',
                                dtype={'psn_id': str}).drop_duplicates()
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
