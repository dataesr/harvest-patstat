#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import text_functions as tf
import numpy as np
import os
import pandas as pd
import re

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"
DICT = {"tls207": {'sep': ',', 'chunksize': 10000000, 'dtype': types.tls207_types},
        "tls206": {'sep': ',', 'chunksize': 3000000, 'dtype': types.tls206_types}
        }

# set working directory
os.chdir(DATA_PATH)


# On commence par récupérer les fichiers patent (infos sur les dépôts de brevets) et old_part
# (participants dédoublonnés et identifiés de la version antérieure)


def initialization_participants(pat, t207, t206, part_history):
    """   This function initializes a table of participants from past corrected informations
    and new patstat version informations

    param patents: table with application informations - T201 or part of -
    must be complete - comes from programme  "p03_patents"
    type patents: dataframe
    param t207:  table patstat t207 or extract of
    type t207: dataframe
    param t206:  table patstat t206 or extract of
    type t206: dataframe
    param old_part:  participant's table corrected from the last patstat's version
    type old_part: dataframe

    :return: a dataframe, initialization of participant's table

    """

    print("Création de la table participants à partir des tables 207 et 206")
    new_participants = pat.merge(t207, how="inner", on="appln_id") \
        .merge(t206, how="inner", on="person_id")
    # on crée la variable qui nous sert d'identifiant unique pour les participants et permet de faire le lien
    # d'une version à l'autre
    print("ajout d'un identifiant")
    new_participants["id_participant"] = new_participants["appln_nr_epodoc"].astype(str) + "_" + new_participants[
        "person_id"].astype(str)
    new_participants["key_appln_nr_person"] = new_participants["key_appln_nr"] + "_" + new_participants[
        "person_id"].astype(str)
    print("Récupération des informations corrigées de la version précédente et renommage des variables")
    part = pd.merge(new_participants[
                        ["id_participant", "appln_nr_epodoc", "person_id", "docdb_family_id", "inpadoc_family_id",
                         "applt_seq_nr",
                         "earliest_filing_date", "invt_seq_nr", "person_name", "person_address", "person_ctry_code",
                         "psn_sector", "psn_id",
                         "psn_name", "appln_publn_number", "appln_auth", "appln_id", "appln_nr", "appln_kind",
                         "receiving_office", "key_appln_nr_person", "key_appln_nr"]],
                    part_history[["id_participant", "type", "name_corrected", "country_corrected", "siren", "siret",
                                  "id_paysage", "rnsr", "grid",
                                  "sexe", "id_personne", "appln_id", "appln_nr", "appln_kind",
                                  "receiving_office", "key_appln_nr", "key_appln_nr_person"]],
                    on=["id_participant", "key_appln_nr_person", "appln_id", "appln_nr", "appln_kind",
                        "receiving_office", "key_appln_nr"], how="left") \
        .rename(
        columns={"appln_nr_epodoc": "id_patent", "person_name": "name_source", "person_address": "address_source",
                 "person_ctry_code": "country_source", "appln_publn_number": "publication_number",
                 "name_corrected": "old_name"})
    # il y a en base des noms de participants nuls, qu'on enlève
    part_hist_new = part[part["name_source"].notna()].copy()
    # on rectifie les codes pays erronés qui contiennent des espaces
    part_hist_new["country_source"] = part_hist_new["country_source"].apply(lambda a: re.sub(r"\s{2}|nan", "", str(a)))

    part_hist_new.fillna("", inplace=True)

    return part_hist_new


# On crée la variable isascii,
# pour pouvoir prendre en compte ou non dans la suite des traitements les caractères spéciaux - alphabets non latins
# et une variable nom propre


# Initialisation de la variable type (pp : personne physique, pm : personne morale)

def add_type_to_part(part_table):
    """   This function updates the "type" variable (pp : personne physique ou pm : personne morale)
    in a participant table

    param part_table: table with participants informations - must have "name_clean",
    large family identifier "inpadoc_family_id", "type" (the old one)
    type part_table: dataframe

    return: the given dataframe with the "type" variable updated according to the large family

    """

    part = part_table.copy()
    # première initialisation pour les types pas déjà renseignés : si c'est un inventeur,
    # c'est forcément une personne physique
    part["type"] = np.where(part["type"] == "", np.where(part["invt_seq_nr"] > 0, "pp", "pm"), part["type"])
    # sélection des cas où un même participant (même nom au sein d'une famille élargie) est rattaché aux deux types
    indiv_with_pm_type = part[["inpadoc_family_id", "type", "name_clean"]].drop_duplicates() \
        .groupby(["inpadoc_family_id", "name_clean"]).count()
    indiv_with_pm_type = indiv_with_pm_type.reset_index()
    indiv_with_pm_type = indiv_with_pm_type[indiv_with_pm_type["type"] > 1]
    # dans ce cas, on leur colle une variable "deposant_indiv" de valeur 1
    indiv_with_pm_type["deposant_indiv"] = 1
    indiv_with_pm_type = indiv_with_pm_type.drop(columns={"type"})
    # on récupère la variable "deposant_indiv" qu'on rajoute dans la table initialisée "part"
    part2 = part.merge(indiv_with_pm_type, how="left", on=["inpadoc_family_id", "name_clean"])
    part2["type"] = np.where(part2["deposant_indiv"] == 1, "pp", part2["type"])
    part2 = part2.drop(columns={"deposant_indiv"})

    return part2


def main():
    patents = pd.read_csv("patent.csv", sep="|", dtype=types.patent_types)
    old_part = pd.read_csv("old_part_key.csv",
                           sep='|',
                           dtype=types.partfin_types,
                           encoding="utf-8")

    tls207 = cfq.filtering("tls207", patents, "appln_id", DICT["tls207"])
    tls206 = cfq.filtering("tls206", tls207, "person_id", DICT["tls206"])
    part_init = initialization_participants(patents, tls207, tls206, old_part)
    part_init["isascii"] = part_init["name_source"].apply(tf.remove_punctuations).apply(tf.remove_accents).apply(
        tf.isascii)
    part_init["name_clean"] = part_init["name_source"].apply(tf.get_clean_name)
    part_init2 = add_type_to_part(part_init)
    part_init2["country_corrected"] = np.where(part_init2["country_corrected"] == "", part_init2["country_source"],
                                               part_init2["country_corrected"])
    part_init2.to_csv("part_init.csv", sep="|", index=False)
    part_init3 = part_init2.drop(columns={"applt_seq_nr", "invt_seq_nr"}).drop_duplicates()
    part_init3.to_csv("part.csv", sep="|", index=False)


if __name__ == "__main__":
    main()
