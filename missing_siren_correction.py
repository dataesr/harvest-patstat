#!/usr/bin/env python
# coding: utf-8

import os
import re

import dtypes_patstat_declaration as types
import numpy as np
import pandas as pd

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"

# set working directory
os.chdir(DATA_PATH)

part_init = pd.read_csv("part_init.csv",
                        sep='|',
                        dtype=types.part_init_types,
                        encoding="utf-8")

correction = pd.read_excel("siren_manquants_avec_occurences_complete.xlsx", engine="openpyxl")

correction = correction.drop(columns="occurences")
correction["type"] = correction["type"].str.lower()
correction = correction.loc[correction["type"] != "x"].copy()
correction = correction.loc[correction["ID"] != "x"].copy()
correction.loc[correction["ID"].str.contains("grid"), "type"] = "grid"
correction["id_1"] = correction.loc[
    correction["ID"].str.contains(";"), "ID"].apply(lambda a: re.split(";", a, maxsplit=1)[0])
correction["id_2"] = correction.loc[
    correction["ID"].str.contains(";"), "ID"].apply(lambda a: re.split(";", a, maxsplit=1)[1])
correction["type"] = correction["type"].fillna("multi")
correction.loc[correction["psn_name"] == "ZODIAC DATA SYSTEMS", "type"] = "siret"
correction = correction.rename(columns={"RNSR": "RNSR_original"})

col = list(correction["type"].unique())

for cl in col:
    correction[cl] = correction.loc[correction["type"] == cl, "ID"]

apreciser = correction.loc[correction["ID"].str.contains(";")]
apreciser.to_csv("apreciser.csv", index=False, sep="|", encoding="utf-8")

apreciser = pd.read_csv("apreciser_corrige.csv", sep="|", encoding="utf-8")

apreciser["liste_type"] = apreciser.apply(lambda x: [x.type1, x.type2], axis=1)
apreciser["liste_id"] = apreciser.apply(lambda x: [x.id_1, x.id_2], axis=1)

apreciser = apreciser.drop(columns=["id_1", "type1", "id_2", "type2"])
apreciser["test"] = apreciser["liste_type"].apply(lambda a: list(set(a)))
apreciser["len_test"] = apreciser["test"].apply(lambda a: len(a))

col_precision = ['oc', 'siret', 'ror', 'pp', 'rnsr']

apreciser_unique = apreciser.loc[apreciser["len_test"] == 1]
apreciser_unique["test"] = apreciser_unique["test"].apply(lambda a: a[0])
apreciser_unique = apreciser_unique.drop(columns=["liste_type", "len_test"])

col_unique = list(apreciser_unique["test"].unique())

for cl in col_unique:
    apreciser_unique[cl] = apreciser_unique.loc[apreciser_unique["test"] == cl, "liste_id"]

apreciser_unique = apreciser_unique.drop(columns=["test", "liste_id"])

apreciser_multi = apreciser.loc[apreciser["len_test"] == 2]
apreciser_multi = apreciser_multi.drop(columns=["len_test", "test"])

apreciser_multi["dict"] = apreciser_multi.apply(
    lambda x: {x.liste_type[0]: x.liste_id[0], x.liste_type[1]: x.liste_id[1]}, axis=1)

apreciser_multi2 = pd.DataFrame([[a, b, c, c2, k, v] for a, b, c, c2, d in apreciser_multi[['psn_name', 'name_source', 'name_source_list', 'address_source', 'dict']].values for a, b, c, c2, k, v in d.items()])