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


def initialization_participants(pat, t207, t206, part_history):
    """ This function initializes a table of participants from past corrected informations
    and new patstat version informations

    :param pat: df with application informations - T201 or part of - comes from programme "p03_patents"
    :param t207: df with table 207 PATSTAT or extract of
    :param t206: df with table 206 PATSTAT or extract of
    :param part_history: participants history - output "part_init" of p05 from previous PATSTAT edition
    :return: df with "clean" inforation on participants
    """

    new_participants = pat.merge(t207, how="inner", on="appln_id") \
        .merge(t206, how="inner", on="person_id")

    # we create a unique participant ID "key_appln_nr_person"
    # before "id_participant" but "appln_nr_epodoc" is deprecated and will be removed at some point
    # can keep "appln_nr_epodoc" and "id_participant" for now
    new_participants["id_participant"] = new_participants["appln_nr_epodoc"].astype(str) + "_" + new_participants[
        "person_id"].astype(str)
    new_participants["key_appln_nr_person"] = new_participants["key_appln_nr"] + "_" + new_participants[
        "person_id"].astype(str)

    # merge previous file with latest PATSTAT data on "id_participant", "key_appln_nr_person", "appln_id", "appln_nr",
    # "appln_kind", "receiving_office" and "key_appln_nr"
    part = pd.merge(new_participants[
                        ["id_participant", "appln_nr_epodoc", "person_id", "docdb_family_id", "inpadoc_family_id",
                         "applt_seq_nr",
                         "earliest_filing_date", "invt_seq_nr", "person_name", "person_address", "person_ctry_code",
                         "psn_sector", "psn_id",
                         "psn_name", "appln_publn_number", "appln_auth", "appln_id", "appln_nr", "appln_kind",
                         "receiving_office", "key_appln_nr_person", "key_appln_nr"]],
                    part_history[["id_participant", "type", "old_name", "country_corrected", "siren", "siret",
                                  "id_paysage", "rnsr", "grid",
                                  "sexe", "id_personne", "appln_id", "appln_nr", "appln_kind",
                                  "receiving_office", "key_appln_nr", "key_appln_nr_person"]],
                    on=["id_participant", "key_appln_nr_person", "appln_id", "appln_nr", "appln_kind",
                        "receiving_office", "key_appln_nr"], how="left") \
        .rename(
        columns={"appln_nr_epodoc": "id_patent", "person_name": "name_source", "person_address": "address_source",
                 "person_ctry_code": "country_source", "appln_publn_number": "publication_number"})

    # remove records where name is missing
    part_hist_new = part[part["name_source"].notna()].copy()

    # "clean" country code: empty string if country_source consists of 2 blank spaces or nan
    # there is also a country code "75" which must correspond to FR but receiving office is US
    # leave it this way for now
    part_hist_new["country_source"] = part_hist_new["country_source"].apply(lambda a: re.sub(r"\s{2}|nan", "", str(a)))

    # replace missing values by empty strings
    part_hist_new.fillna("", inplace=True)

    return part_hist_new


def add_type_to_part(part_table):
    """ This function updates the "type" variable ("PP": natural person or "PM": juridical person)
     in the participant table
    :param part_table: df with participant info
    :return: df with the "type" variable updated according to the INPADOC family
    """
    part = part_table.copy()

    # subset avec psn_sector pour name_clean dans famille inpadoc

    psn_sector = part[["psn_sector", "name_clean", "inpadoc_family_id"]]
    psn_sector = psn_sector.drop_duplicates()

    # bool pour indiquer q'il y a une valeur manquante dans psn_sector
    psn_sector["missing"] = psn_sector["psn_sector"].apply(lambda a: 1 if a in ["", "UNKNOWN"] else 0)
    psn_sector = psn_sector.sort_values("name_clean")

    # regarde s'il y a des cas où plusieurs psn_sector pour même personne
    val_counts = psn_sector[["name_clean", "inpadoc_family_id"]].value_counts()
    val_counts = val_counts.reset_index()
    val_counts = val_counts.rename(columns={0: "compte"})
    psn = pd.merge(psn_sector, val_counts, on=["name_clean", "inpadoc_family_id"], how="left")

    # ne garde que les cas où il y a plusieurs psn_sector et où il n'y a pas de valeurs manquantes
    # complétera les valeurs manquantes plus tard
    psn = psn[psn["compte"] > 1]
    psn2 = psn[psn["missing"] == 0]

    # cherche également s'il y a plusieurs valeurs pour le même ID
    val_counts2 = psn2[["name_clean", "inpadoc_family_id"]].value_counts()
    val_counts2 = val_counts2.reset_index()
    val_counts2 = val_counts2.rename(columns={0: "cmpt"})
    psn3 = pd.merge(psn2, val_counts2, on=["name_clean", "inpadoc_family_id"], how="left")
    psn3 = psn3.drop_duplicates()
    # ne garde que les valeurs doublonnées
    psn4 = psn3[psn3["compte"] > 1]
    psn3 = psn3.rename(columns={"psn_sector": "secteur"})
    # psn_sector au format binaire (individuel ou non)
    psn4["sect_binaire"] = psn4["psn_sector"].apply(lambda a: 1 if a == "INDIVIDUAL" else 0)
    # récupère le type et invt_seq_nr pour les ID identifiés comme problématiques
    tricky = pd.merge(part[["name_clean", "inpadoc_family_id", "type", "invt_seq_nr"]], psn4,
                      on=["name_clean", "inpadoc_family_id"], how="right")
    tricky = tricky.drop_duplicates()
    # le manuel indique que invt_seq_nr >= 1 signifie qu'il s'agit d'un inventeur
    # et 0 pour une autre personne (le plus souvent le demandeur) donc proxy intéressant pour identifier les pp des pm
    # certains ID peuvent avoir des 0, puis, 1, 2,... et donc des "types" différents
    # dans ce cas, fait la somme des invt_seq_nr pour savoir si l'ID a une valeur = ou > 0
    tricky["somme"] = tricky.groupby(["name_clean", "inpadoc_family_id"])["invt_seq_nr"].transform("sum")
    # si somme > 0 personne physique sinon personne morale
    tricky["sect_bin2"] = tricky["somme"].apply(lambda a: "pp" if a > 0 else "pm")

    # vérifie que nos résultats sont similaires à ceux de "type" :
    tricky["test"] = tricky["type"] == tricky["sect_bin2"]
    # ne garde que les cas où il y a des différences
    # se rend compte que cette méthode est plus efficace
    # récupère les résultats nouvelles méthodes dans la variable "type"
    trick = tricky[tricky["test"] == False]
    trick["type"] = trick["sect_bin2"].apply(lambda a: "pm" if a == "pm" else "pp")
    # maintenant on fait le merge trick avec autres df pour continuer corriger


    # first: fill in missing values in type
    # if invt_seq_nr > 0, the participant is an inventor - so: natural person - PP
    # else juridical person - PM
    part["type"] = np.where(part["type"] == "", np.where(part["invt_seq_nr"] > 0, "pp", "pm"), part["type"])

    # select cases where the same participant (same name in INPADOC family) is both considered PP and PM
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
    # file which served to create part_init: old_part_key.csv
    # old_part = pd.read_csv("old_part_key.csv",
    #                            sep='|',
    #                            dtype=types.partfin_types,
    #                            encoding="utf-8")
    # old_part = old_part.rename(columns={"name_corrected": "old_name"})
    patents = pd.read_csv("patent.csv", sep="|", dtype=types.patent_types)
    old_part = pd.read_csv("part_init.csv",
                           sep='|',
                           dtype=types.part_init_types,
                           encoding="utf-8")

    tls207 = cfq.filtering("tls207", patents, "appln_id", DICT["tls207"])
    tls206 = cfq.filtering("tls206", tls207, "person_id", DICT["tls206"])
    part_init = initialization_participants(patents, tls207, tls206, old_part)

    # On crée la variable isascii,
    # pour pouvoir prendre en compte ou non dans la suite des traitements les caractères spéciaux - alphabets non latins
    # et une variable nom propre
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
