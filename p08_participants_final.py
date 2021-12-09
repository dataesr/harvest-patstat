#!/usr/bin/env python
# coding: utf-8

import os

import numpy as np
import pandas as pd

import dtypes_patstat_declaration as types

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"

# set working directory
os.chdir(DATA_PATH)


def main():
    part_init = pd.read_csv('part_init.csv', sep='|', dtype=types.part_init_types)
    part = pd.read_csv('part.csv', sep='|', dtype=types.part_init_types)

    part_entp = pd.read_csv('part_entp_final.csv', sep='|',
                            dtype=types.part_entp_types)[
        ['key_appln_nr_person', 'doc_std_name', "doc_std_name_id", 'name_corrected', 'country_corrected', 'siren',
         'siret', 'id_paysage', 'rnsr',
         'grid']].copy()

    part_indiv = pd.read_csv('part_individuals.csv', sep='|',
                             dtype=types.part_entp_types)[
        ['id_participant', 'country_corrected', 'name_corrected', 'sexe']].copy()

    part_tmp = pd.concat([part_entp, part_indiv], sort=True)

    particip = part[part['isascii']][
        ['key_appln_nr_person', 'person_id', 'id_patent', 'docdb_family_id', 'inpadoc_family_id',
         'earliest_filing_date',
         'name_source', 'address_source', 'country_source', 'appln_auth', 'type', 'isascii']].merge(part_tmp,
                                                                                                    on='key_appln_nr_person',
                                                                                                    how='left')

    for col in particip.columns:
        particip[col] = particip[col].fillna('')

    particip['id_personne'] = particip['key_appln_nr_person']
    particip['name_corrected'] = np.where(particip['name_corrected'] == '', particip['name_source'],
                                          particip['name_corrected'])

    particip.to_csv('part.csv', sep='|', index=False, encoding="utf-8")

    participants = particip[
        ['id_patent', 'key_appln_nr_person', 'id_personne', 'type', 'sexe', 'doc_std_name', "doc_std_name_id",
         'name_source', 'name_corrected',
         'address_source',
         'country_source', 'country_corrected']]

    participants.to_csv('participants.csv', sep='|', index=False, encoding="utf-8")

    # création de la table idext

    part_idext = particip[['key_appln_nr_person', 'siren', 'siret', 'id_paysage', 'rnsr', 'grid']]

    idext1 = pd.melt(frame=part_idext, id_vars='key_appln_nr_person', var_name='id_type', value_name='id_value')

    idext = idext1[idext1['id_value'] != '']

    idext.to_csv('idext.csv', sep='|', index=False)

    # création de la table role

    part_role = part_init[part_init['isascii']][['key_appln_nr_person', 'applt_seq_nr', 'invt_seq_nr']].rename(
        columns={'applt_seq_nr': 'dep', 'invt_seq_nr': 'inv'})

    role1 = pd.melt(frame=part_role, id_vars='key_appln_nr_person', var_name='role')

    role = role1[role1['value'] > 0].drop(columns={'value'})

    role.to_csv('role.csv', sep='|', index=False)


if __name__ == '__main__':
    main()