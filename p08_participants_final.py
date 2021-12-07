#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd

import dtypes_patstat_declaration as types


def main():
    part_init = pd.read_csv('part_init.csv', sep='|', dtype=types.part_init_types)
    part = pd.read_csv('part.csv', sep='|', dtype=types.part_init_types)

    part_entp = pd.read_csv('part_entp_final.csv', sep='|',
                            dtype={'id_participant': str, 'name_corrected': str, 'country_corrected': str, 'siren': str,
                                   'id_paysage': str, 'rnsr': str, 'siret': str, 'grid': str})[
        ['id_participant', 'name_corrected', 'country_corrected', 'siren', 'siret', 'id_paysage', 'rnsr',
         'grid']].copy()

    part_indiv = pd.read_csv('part_individuals.csv', sep='|',
                             dtype={'id_participant': str, 'name_corrected': str, 'country_corrected': str, 'sexe': str,
                                    'type': str, 'old_name': str, 'id_personne': str})[
        ['id_participant', 'country_corrected', 'name_corrected', 'sexe']].copy()

    part_tmp = pd.concat([part_entp, part_indiv], sort=True)

    particip = part[part['isascii']][
        ['id_participant', 'person_id', 'id_patent', 'docdb_family_id', 'inpadoc_family_id', 'earliest_filing_date',
         'name_source', 'address_source', 'country_source', 'appln_auth', 'type', 'isascii']].merge(part_tmp,
                                                                                                    on='id_participant',
                                                                                                    how='left')

    for col in particip.columns:
        particip[col] = particip[col].fillna('')

    particip['id_personne'] = particip['id_participant']
    particip['name_corrected'] = np.where(particip['name_corrected'] == '', particip['name_source'],
                                          particip['name_corrected'])

    particip.to_csv('part.csv', sep='|', index=False, encoding="utf-8")

    participants = particip[
        ['id_patent', 'id_participant', 'id_personne', 'type', 'sexe', 'name_source', 'name_corrected',
         'address_source',
         'country_source', 'country_corrected']]

    participants.to_csv('participants.csv', sep='|', index=False, encoding="utf-8")

    # création de la table idext

    part_idext = particip[['id_participant', 'siren', 'siret', 'id_paysage', 'rnsr', 'grid']]

    idext1 = pd.melt(frame=part_idext, id_vars='id_participant', var_name='id_type', value_name='id_value')

    idext = idext1[idext1['id_value'] != '']

    idext.to_csv('idext.csv', sep='|', index=False)

    # création de la table role

    part_role = part_init[part_init['isascii']][['id_participant', 'applt_seq_nr', 'invt_seq_nr']].rename(
        columns={'applt_seq_nr': 'dep', 'invt_seq_nr': 'inv'})

    role1 = pd.melt(frame=part_role, id_vars='id_participant', var_name='role')

    role = role1[role1['value'] > 0].drop(columns={'value'})

    role.to_csv('role.csv', sep='|', index=False)


if __name__ == '__main__':
    main()
