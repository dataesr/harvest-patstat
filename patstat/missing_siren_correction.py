#!/usr/bin/env python
# coding: utf-8

import os
import re

import numpy as np
import pandas as pd

from patstat import dtypes_patstat_declaration as types

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# set working directory
os.chdir(DATA_PATH)

# chargement des données des participants
part_init = pd.read_csv("part_init.csv",
                        sep='|',
                        dtype=types.part_init_types,
                        encoding="utf-8")

# chargement des données "personnes morales" corrigées par Emmanuel
correction = pd.read_excel("siren_manquants_avec_occurences_complete.xlsx", engine="openpyxl")

# ne garde pas la colonne avec les occurences
correction = correction.drop(columns="occurences")
# crée le type grid pour les personnes ayant un ID grid
correction.loc[correction["ID"].str.contains("grid"), "type"] = "grid"
# pour les personnes qui ont plusieurs ID séparés par ";", dédouble l'ID en 2 colonnes
correction["id_1"] = correction.loc[
    correction["ID"].str.contains(";"), "ID"].apply(lambda a: re.split(";", a, maxsplit=1)[0])
correction["id_2"] = correction.loc[
    correction["ID"].str.contains(";"), "ID"].apply(lambda a: re.split(";", a, maxsplit=1)[1])
# quand type est vide, plusieurs ID - crée colonne multi pour les signaler
correction["type"] = correction["type"].fillna("multi")
# type vide pour ZODIAC DATA SYSTEMS - indique que l'ID est un SIRET
correction.loc[correction["psn_name"] == "ZODIAC DATA SYSTEMS", "type"] = "siret"
# met les types en minuscule
correction["type"] = correction["type"].apply(lambda a: a.lower())
correction = correction.loc[correction["type"] != "x"].copy()
correction = correction.loc[correction["ID"] != "x"].copy()

# renomme la colonne RNSR car dans type des autres ID RNSR présents - ne veut pas écraser les données
correction = correction.rename(columns={"RNSR": "RNSR_original"})

# liste de tous les types disponibles - les stocke pour créer les nouvelles colonnes du fichier
col = list(correction["type"].unique())

# met les valeurs d'ID dans les colonnes correspondant au type (si type unique)
for cl in col:
    correction[cl] = correction.loc[correction["type"] == cl, "ID"]

# ne garde que les enregistrements où ID multiples et où il n'y a pas d'infos sur le type d'ID
# traitement à la main
apreciser = correction.loc[correction["ID"].str.contains(";")]
apreciser.to_csv("apreciser.csv", index=False, sep="|", encoding="utf-8")

# recharge les données corrigées
apreciser = pd.read_csv("apreciser_corrige.csv", sep="|", encoding="utf-8")

# met les 2 types et les 2 ID dans une liste chacun
apreciser["liste_type"] = apreciser.apply(lambda x: [x.type1, x.type2], axis=1)
apreciser["tuple_id"] = apreciser.apply(lambda x: (x.id_1, x.id_2), axis=1)
apreciser["liste_id2"] = apreciser.apply(lambda x: [x.id_1, x.id_2], axis=1)

# traitement spécifique pour les personnes où plusieurs ID, mais du même type
apreciser = apreciser.drop(columns=["id_1", "type1", "id_2", "type2"])
# cherche si valeur unique dans liste type
apreciser["test"] = apreciser["liste_type"].apply(lambda a: list(set(a)))
apreciser["len_test"] = apreciser["test"].apply(lambda a: len(a))

col_precision = ['oc', 'siret', 'ror', 'pp', 'rnsr']

apreciser_unique = apreciser.loc[apreciser["len_test"] == 1].copy()
apreciser_unique["test"] = apreciser_unique["test"].apply(lambda a: a[0])
apreciser_unique = apreciser_unique.drop(columns=["liste_type", "len_test"])

col_unique = list(apreciser_unique["test"].unique())

# met le tuple des ID dans la colonne correspondant au type
for cl in col_unique:
    apreciser_unique[cl] = apreciser_unique.loc[apreciser_unique["test"] == cl, "tuple_id"]

apreciser_unique = apreciser_unique.drop(columns=["test", "tuple_id", "liste_id2"])

apreciser_unique = apreciser_unique.reset_index().drop(columns="index")

# pour les personnes ayant 2 ID de types différents
apreciser_multi = apreciser.loc[apreciser["len_test"] == 2].copy()
apreciser_multi = apreciser_multi.drop(columns=["len_test", "test"])

# crée un dictionnaire avec type en clef et ID en valeur
apreciser_multi["dict"] = apreciser_multi.apply(
    lambda x: {x.liste_type[0]: x.liste_id2[0], x.liste_type[1]: x.liste_id2[1]}, axis=1)

apreciser_multi = apreciser_multi.reset_index().drop(columns="index")

# transforme les dictionnaires en colonnes à l'aide de json_normalize
apreciser_multi2 = pd.concat(
    [apreciser_multi[["psn_name", "name_source", "name_source_list", "address_source", "liste_type", "liste_id2"]],
     pd.json_normalize(apreciser_multi["dict"])], axis=1)

apreciser_multi2 = apreciser_multi2.drop(columns=["liste_type", "liste_id2"])

# concatène les 2 df multi avec type unique et multi type
apreciser2 = pd.concat([apreciser_unique, apreciser_multi2])

# joint les multi aux données de départ
correction_final = pd.merge(correction, apreciser2,
                            on=["psn_name", "name_source", "name_source_list", "address_source"], how="left")

# remplit les valeurs manquantes et crée des colonnes propres
for cl in col_precision:
    correction_final[cl] = np.where(correction_final[cl + "_y"].isna(), correction_final[cl + "_x"],
                                    correction_final[cl + "_y"])

correction_final["rnsr"] = np.where(correction_final["rnsr"].isna(), correction_final["RNSR_original"],
                                    correction_final["rnsr"])


correction_final["pp"] = correction_final.loc[correction_final["pp"]=="PP", "pp"].apply(lambda a: a.lower())

# df final des corrections
correction_final = correction_final.drop(
    columns=["ID", "type", "id_1", "id_2", "RNSR_original", "oc_x", "oc_y", "siret_x", "siret_y", "ror_x", "ror_y",
             "pp_x", "pp_y", "multi", "rnsr_x", "rnsr_y"])

correction_final.to_csv("missing_siren.csv", index=False, sep="|", encoding="utf-8")


# fait la jointure avec les données des participants

# recrée la variable name_source_list présente dans la table corrigée - simplement une copie de name_source
# dans la table part_init
def multi_name(df_part: pd.DataFrame) -> pd.DataFrame:
    """

    :param df_part:
    :return:
    """
    df = df_part.copy()
    df["name_source_list"] = df["name_source"].copy()

    df2 = df[["key_appln_nr_person",
              "publication_number",
              "psn_id",
              "name_source",
              "name_source_list", "siren"]].copy()

    df3 = df2.explode("name_source_list").copy()

    return df3


part_init_multi = multi_name(part_init)

# jointure entre multi_name et part_init
part_init2 = pd.merge(part_init, part_init_multi,
                      on=["key_appln_nr_person", "publication_number", "psn_id", "name_source", "siren"],
                      how="left").drop_duplicates()

# jointure entre part_init et corrections
part_init3 = pd.merge(part_init2, correction_final,
                      on=["name_source", "address_source", "psn_name", "name_source_list"],
                      how="left").drop_duplicates()

# alimente les colonnes avec les données d'ID et change le type si besoin
col_part_init = ["siret", "rnsr", "grid"]

part_init3.loc[part_init3["pp"] == "pp", "type"] = "pp"

for cl in col_part_init:
    part_init3[cl] = part_init3[cl + "_y"]
    part_init3[cl] = np.where(part_init3[cl + "_y"].isna(), part_init3[cl + "_x"],
                              part_init3[cl + "_y"])

part_init3 = part_init3.drop(
    columns=["name_source_list", "pp", "siret_x", "siret_y", "rnsr_x", "rnsr_y", "grid_x", "grid_y"])

part_init3 = part_init3.rename(columns={"Idref": "idref"})
part_init3.to_csv("part_init_complete.csv", sep="|", encoding="utf-8")
