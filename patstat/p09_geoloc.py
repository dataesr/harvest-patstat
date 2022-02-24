#!/usr/bin/env python
# coding: utf-8

import os

import numpy as np
import pandas as pd

from patstat import a01_outils_divers as a01
from patstat import text_functions as tf

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# set working directory
os.chdir(DATA_PATH)


def main():
    # geolocalisation of participants
    part_init = pd.read_csv('part.csv', sep='|',
                            dtype={'id_participant': str, 'id_patent': str, 'docdb_family_id': str,
                                   'inpadoc_family_id': str, 'siren': str, 'siret': str, 'id_paysage': str, 'rnsr': str,
                                   'id_personne': str, 'grid': str, 'idref': str, 'oc': str, 'ror': str})
    patent = pd.read_csv('patent.csv', sep='|', dtype={'docdb_family_id': str})
    adresses_inpi_init = pd.read_csv('adresses_inpi.csv', sep='|',
                                     dtype={'numpubli': str, 'siren': str, 'family_id': str, 'orgname': str})

    part = part_init[part_init['country_corrected'] == 'FR'].copy().merge(
        patent[['key_appln_nr', 'appln_nr', 'appln_filing_year', 'appln_publn_number']], how='left', on='key_appln_nr')[
        ['key_appln_nr_person', 'docdb_family_id', 'inpadoc_family_id', 'id_patent', 'isascii', 'type',
         'address_source',
         'name_corrected', 'appln_nr', 'appln_filing_year', 'appln_publn_number', 'siren']].rename(
        columns={'appln_publn_number': 'numpubli', 'siren': 'siren_patstat', 'appln_nr': 'numdepot',
                 'address_source': 'adr_patstat', 'appln_filing_year': 'annee_depot', 'name_corrected': 'name_source'})

    part.loc[part["name_source"].isna(), "name_source"] = ""

    part['name'] = part['name_source'].apply(lambda x: x.lower())

    part2 = part.merge(adresses_inpi_init, on=['numpubli', 'name'], how='left')

    for col in part2.columns:
        part2[col] = part2[col].replace(np.nan, '')

    part2['adresse'] = part2['address'] + ' ' + part2['postcode'] + ' ' + part2['city']
    part2['adresse'] = part2['adresse'].apply(tf.remove_multiple_spaces).apply(tf.remove_first_end_spaces)
    part2['adresse'] = np.where(part2['adresse'] == '', part2['adr_patstat'], part2['adresse'])
    part_adr = part2[(part2['adresse'] != '')][
        ['key_appln_nr_person', 'adr_patstat', 'address', 'city', 'postcode', 'adresse']]

    # a01.save_object(part_adr, 'part_adr.p')
    # part_adr = a01.load_object('part_adr.p')
    # part_adr = part_adr0.sample(200)

    lien_id_adr = part_adr[['key_appln_nr_person', 'adr_patstat', 'address', 'city', 'postcode']].rename(
        columns={'address': 'adr_inpi', 'city': 'city_inpi', 'postcode': 'cp_inpi'})
    lien_id_adr['adr_patstat_orig'] = lien_id_adr['adr_patstat']
    lien_id_adr['adr_inpi_orig'] = lien_id_adr['adr_inpi']
    for col in ['adr_patstat', 'adr_inpi', 'city_inpi']:
        lien_id_adr[col] = lien_id_adr[col].apply(lambda x: x.lower()).apply(tf.remove_punctuations).apply(
            a01.replace_saut_ligne).apply(a01.transform_address)
    lien_id_adr['cpville'] = (lien_id_adr['cp_inpi'] + ' ' + lien_id_adr['city_inpi']).apply(
        tf.remove_multiple_spaces).apply(
        tf.remove_first_end_spaces)
    lien_id_adr['cpvillepat'] = lien_id_adr['adr_patstat'].apply(a01.get_cpville_from_adresse)
    lien_id_adr['adresse'] = (
            lien_id_adr['adr_inpi'] + ' ' + lien_id_adr['cp_inpi'] + ' ' + lien_id_adr['city_inpi']).apply(
        tf.remove_multiple_spaces).apply(tf.remove_first_end_spaces)
    lien_id_adr['cpville'] = np.where(lien_id_adr['cpville'] == '', lien_id_adr['cpvillepat'], lien_id_adr['cpville'])
    lien_id_adr['adresse'] = np.where(lien_id_adr['adresse'] == '', lien_id_adr['adr_patstat'], lien_id_adr['adresse'])

    adr_to_geocod = pd.concat(
        [lien_id_adr.loc[lien_id_adr['cpville'] != '', ['cpville']].rename(columns={'cpville': 'adresse'}),
         lien_id_adr.loc[lien_id_adr['adresse'] != '', ['adresse']]])
    set_to_geocode = set(adr_to_geocod['adresse'])
    list_adr_to_geocod = list(set_to_geocode)

    adr_geocoded = a01.get_list_geocod_adresses(list_adr_to_geocod)
    # save_object(adr_geocoded, path_inter + 'liste_adresses_completed2.p')

    a01.save_object(adr_geocoded, 'adr_geocoded.p')

    df_adr_geocoded = a01.get_df_from_list_adresses(adr_geocoded)

    # adr_geocoded = load_object(path_final + 'adresses_geocoded.p')

    idadr = lien_id_adr[(lien_id_adr['cpville'] != '') | (lien_id_adr['adresse'] != '')]

    best_scores_a = idadr.merge(df_adr_geocoded.loc[
                                    df_adr_geocoded['geocoded'], ['input_address', 'score', 'city',
                                                                  'housenumber',
                                                                  'post_code', 'street', 'city_code']],
                                how='left',
                                left_on='adresse', right_on='input_address').drop(columns={'input_address'}).rename(
        columns={'city': 'city_comp', 'housenumber': 'housenumber_comp', 'post_code': 'cp_comp',
                 'street': 'street_comp', 'city_code': 'city_code_comp', 'score': 'score_comp'})
    for col in ['city_comp', 'housenumber_comp', 'cp_comp', 'street_comp', 'city_code_comp']:
        best_scores_a[col] = best_scores_a[col].replace(np.nan, '')
    best_scores_a['dep'] = best_scores_a['cpville'].apply(a01.get_dep_from_adresse)
    best_scores_a['dep_comp'] = best_scores_a['cp_comp'].apply(a01.get_departement)

    best_scores_b = best_scores_a.merge(df_adr_geocoded.loc[
                                            df_adr_geocoded['geocoded'], ['input_address', 'score', 'city',
                                                                          'housenumber', 'post_code', 'street',
                                                                          'city_code']], how='left',
                                        left_on='cpville', right_on='input_address').rename(
        columns={'city': 'city_cpville', 'housenumber': 'housenumber_cpville',
                 'post_code': 'cp_cpville', 'street': 'street_cpville', 'city_code': 'city_code_cpville',
                 'score': 'score_cpville'})
    for col in ['city_cpville', 'cp_cpville', 'street_cpville', 'housenumber_cpville', 'city_code_cpville']:
        best_scores_b[col] = best_scores_b[col].replace(np.nan, '')
    best_scores_b['score_comp'] = best_scores_b['score_comp'].replace(np.nan, 0)
    best_scores_b['score_cpville'] = best_scores_b['score_cpville'].replace(np.nan, 0)
    best_scores_b['dep_cpville'] = best_scores_b['cp_cpville'].apply(a01.get_departement)

    best_scores_b.to_csv('best_scores_b.csv', sep='|', index=False, encoding="utf-8")

    # 1er cas : que la ville : score > 0.9
    # 2ème cas : adresse complète : score > 0.4
    # 3ème cas : CPville : score > 0.4

    best_geocod_a = best_scores_b[(best_scores_b['cpville'] == '') & (best_scores_b['score_comp'] > 0.9)][
        ['key_appln_nr_person', 'city_inpi', 'cp_inpi', 'cpville', 'adresse', 'dep', 'score_comp', 'city_comp',
         'housenumber_comp', 'cp_comp', 'street_comp', 'city_code_comp', 'dep_comp']].rename(
        columns={'score_comp': 'score', 'city_comp': 'geocoded_city', 'housenumber_comp': 'geocoded_number',
                 'cp_comp': 'geocoded_cp', 'street_comp': 'geocoded_street', 'city_code_comp': 'geocoded_city_code',
                 'dep_comp': 'geocoded_dep'})

    best_geocod_b = best_scores_b[(best_scores_b['cpville'] != '') & (best_scores_b['score_comp'] > 0.4)][
        ['key_appln_nr_person', 'city_inpi', 'cp_inpi', 'cpville', 'adresse', 'dep', 'score_comp', 'city_comp',
         'housenumber_comp', 'cp_comp', 'street_comp', 'city_code_comp', 'dep_comp']].rename(
        columns={'score_comp': 'score', 'city_comp': 'geocoded_city', 'housenumber_comp': 'geocoded_number',
                 'cp_comp': 'geocoded_cp', 'street_comp': 'geocoded_street', 'city_code_comp': 'geocoded_city_code',
                 'dep_comp': 'geocoded_dep'})

    best_geocod_c = best_scores_b[(best_scores_b['cpville'] != '') & (best_scores_b['score_cpville'] > 0.4)][
        ['key_appln_nr_person', 'city_inpi', 'cp_inpi', 'cpville', 'adresse', 'dep', 'score_cpville',
         'city_cpville', 'housenumber_cpville', 'cp_cpville', 'street_cpville',
         'city_code_cpville', 'dep_cpville']] \
        .rename(columns={'city_cpville': 'geocoded_city', 'housenumber_cpville': 'geocoded_number',
                         'cp_cpville': 'geocoded_cp', 'street_cpville': 'geocoded_street',
                         'city_code_cpville': 'geocoded_city_code', 'dep_cpville': 'geocoded_dep',
                         'score_cpville': 'score'})

    best_geocod = pd.concat([best_geocod_a, best_geocod_b, best_geocod_c])

    geocod = best_geocod[(best_geocod['dep'] == '') | (best_geocod['dep'] == best_geocod['geocoded_dep'])]

    geocod.sample(6)

    best_geocod.to_csv('best_geocod.csv', sep='|', index=False, encoding="utf-8")

    return best_scores_b, best_geocod


if __name__ == '__main__':
    main()
