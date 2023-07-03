#!/usr/bin/env python
# coding: utf-8

import os
import re
from utils import swift

import numpy as np
import pandas as pd

from patstat import dtypes_patstat_declaration as types

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def part_final():
    # set working directory
    os.chdir(DATA_PATH)

    # load part_init, part, part_entp and part_individuals to merge them and create a single file with all participants
    part_init = pd.read_csv('part_init_p05.csv', sep='|', dtype=types.part_init_types, engine="python")
    part = pd.read_csv('part_p05.csv', sep='|', dtype=types.part_init_types)

    part_entp = pd.read_csv('part_entp_final2.csv', sep='|',
                            dtype=types.part_entp_types)[
        ['key_appln_nr_person', 'doc_std_name', "doc_std_name_id", 'name_corrected',
         'country_corrected', 'siren',
         'siret', 'id_paysage', 'rnsr',
         'grid', 'idref', 'oc', 'ror', 'sexe']].copy()

    part_indiv = pd.read_csv('part_individuals_p06b.csv', sep='|',
                             dtype=types.part_entp_types)[
        ['key_appln_nr_person', 'country_corrected', 'doc_std_name', "doc_std_name_id", 'name_corrected', 'sexe',
         'siren',
         'siret', 'id_paysage', 'rnsr',
         'grid', 'idref', 'oc', 'ror']].copy()

    part_tmp = pd.concat([part_entp, part_indiv], sort=True)

    # keep only participants with ASCII names

    particip = part[part['isascii']][
        ['key_appln_nr_person', 'key_appln_nr', 'person_id', 'docdb_family_id', 'inpadoc_family_id',
         'earliest_filing_date', 'name_source', 'address_source',
         'country_source', 'appln_auth', 'type', 'isascii']].merge(part_tmp,
                                                                   on='key_appln_nr_person',
                                                                   how='left')

    for col in list(particip.columns):
        particip[col] = particip[col].fillna('')

    particip['id_personne'] = particip['key_appln_nr_person']

    # fill empty name_corrected with name_source
    particip['name_corrected'] = np.where(particip['name_corrected'] == '', particip['name_source'],
                                          particip['name_corrected'])

    particip.loc[(particip["siret"].notna()) & (particip["siret"].str.findall(r"\(\'")), "siret2"] = particip.loc[
        (particip["siret"].notna()) & (particip["siret"].str.findall(r"\(\'")), "siret"].replace(r"\(\'|\'|\)", "",
                                                                                                 regex=True).str.split(
        ",")

    siret = {"key_appln_nr_person": [], "siret": [], "siret3": []}

    for _, r in particip.loc[particip["siret2"].notna()].iterrows():
        key = r.key_appln_nr_person
        sir = r.siret
        s2 = []
        for i in r.siret2:
            res = re.sub("\s+", "", i)
            s2.append(res)
        siret["key_appln_nr_person"].append(key)
        siret["siret"].append(sir)
        siret["siret3"].append(s2)

    sup_esp = pd.DataFrame(data=siret)

    particip = pd.merge(particip, sup_esp, on=["key_appln_nr_person", "siret"], how="left")

    particip = particip.drop(columns="siret2").rename(columns={"siret3": "siret2"})

    particip.loc[particip["siret2"].notna(), "siren2"] = particip.loc[particip["siret2"].notna(), "siret2"].apply(
        lambda a: ", ".join([item[0:9] for item in a]))

    particip.loc[particip["siret2"].notna(), "siret2"] = particip.loc[particip["siret2"].notna(), "siret2"].apply(
        lambda a: ", ".join(a))

    particip.loc[particip["siret2"].notna(), "siret"] = particip.loc[particip["siret2"].notna(), "siret2"]

    particip.loc[particip["siren2"].notna(), "siren"] = particip.loc[particip["siren2"].notna(), "siren2"]

    particip = particip.drop(columns=["siret2", "siren2"])

    particip.to_csv('part_p08.csv', sep='|', index=False, encoding="utf-8")
    swift.upload_object('patstat', 'part_p08.csv')

    # création de la table participants

    participants = particip[
        ['key_appln_nr', 'key_appln_nr_person', 'id_personne', 'type', 'sexe', 'doc_std_name',
         "doc_std_name_id",
         'name_source', 'name_corrected',
         'address_source',
         'country_source', 'country_corrected']]

    participants.to_csv('participants.csv', sep='|', index=False, encoding="utf-8")
    swift.upload_object('patstat', 'participants.csv')

    # création de la table idext

    part_idext = particip[['key_appln_nr_person', 'siren', 'siret', 'id_paysage', 'rnsr', 'grid']]

    idext1 = pd.melt(frame=part_idext, id_vars='key_appln_nr_person', var_name='id_type', value_name='id_value')

    idext = idext1[idext1['id_value'] != '']

    idext.to_csv('idext.csv', sep='|', index=False)
    swift.upload_object('patstat', 'idext.csv')

    # création de la table role

    part_role = part_init[part_init['isascii']][['key_appln_nr_person', 'applt_seq_nr', 'invt_seq_nr']].rename(
        columns={'applt_seq_nr': 'dep', 'invt_seq_nr': 'inv'})

    role1 = pd.melt(frame=part_role, id_vars='key_appln_nr_person', var_name='role')

    role = role1[role1['value'] > 0].drop(columns={'value'})

    role.to_csv('role.csv', sep='|', index=False)
    swift.upload_object('patstat', 'role.csv')
