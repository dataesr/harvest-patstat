#!/usr/bin/env python
# coding: utf-8

import os

import pandas as pd

from patstat import dtypes_patstat_declaration as types
from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def recreate():
    os.chdir(DATA_PATH)

    swift.download_object('patstat', 'partfin.csv', 'partfin.csv')
    old_part = pd.read_csv("partfin.csv",
                           sep='|',
                           dtype=types.partfin_types)

    swift.download_object('patstat', 'part_p08.csv', 'part_p08.csv')
    part = pd.read_csv("part_p08.csv",
                       sep='|',
                       dtype=types.partfin_types,
                       engine="python",
                       usecols=["key_appln_nr_person", "key_appln_nr", "person_id", "id_patent"])

    pat = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.tls201_types, engine="python")
    part2 = pd.merge(part, pat,
                     on="key_appln_nr", how="left")
    part2 = part2[["key_appln_nr_person", "person_id", "id_patent", "appln_id", "appln_auth", "appln_nr", "appln_kind",
                   "receiving_office", "key_appln_nr"]]

    part3 = part2.drop(columns="appln_id").drop_duplicates()

    old_part2 = pd.merge(old_part, part2, on=["id_patent", "person_id", "appln_auth"], how="left")

    old_part2 = old_part2[old_part2["appln_id"].notna()]

    old_part2 = old_part2.drop(columns=["id_participant", "id_patent", "id_personne"])
    old_part2.to_csv("old_part_key2.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'old_part_key2.csv')

    old_part3 = pd.merge(old_part, part3, on=["id_patent", "person_id", "appln_auth"], how="left")

    old_part3 = old_part3.drop(columns=["id_participant", "id_patent", "id_personne"])
    old_part3.to_csv("partfin2.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'partfin2.csv')
