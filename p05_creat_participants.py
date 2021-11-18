#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import fasttext
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

PM = ["COMPANY",
      "GOV NON-PROFIT",
      "UNIVERSITY",
      "HOSPITAL",
      "GOV NON-PROFIT UNIVERSITY",
      "COMPANY GOV NON-PROFIT",
      "COMPANY UNIVERSITY",
      "COMPANY HOSPITAL"]

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

    part_history = part_history.drop(columns="type")

    tbl207 = pat.merge(t207, how="inner", on="appln_id")

    new_participants = pd.merge(tbl207, t206, how="inner", on="person_id")

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
                         "applt_seq_nr", "doc_std_name", "doc_std_name_id",
                         "earliest_filing_date", "invt_seq_nr", "person_name", "person_address", "person_ctry_code",
                         "psn_sector", "psn_id",
                         "psn_name", "appln_publn_number", "appln_auth", "appln_id", "appln_nr", "appln_kind",
                         "receiving_office", "key_appln_nr_person", "key_appln_nr"]],
                    part_history[["id_participant", "old_name", "country_corrected", "siren", "siret",
                                  "id_paysage", "rnsr", "grid",
                                  "sexe", "id_personne", "appln_id", "appln_nr", "appln_kind",
                                  "receiving_office", "key_appln_nr", "key_appln_nr_person"]],
                    on=["id_participant", "key_appln_nr_person", "appln_id", "appln_nr", "appln_kind",
                        "receiving_office", "key_appln_nr"], how="left") \
        .rename(
        columns={"appln_nr_epodoc": "id_patent", "person_name": "name_source", "person_address": "address_source",
                 "person_ctry_code": "country_source", "appln_publn_number": "publication_number"})

    # remove records where name is missing
    part = part.dropna(subset=["name_source"])

    part.loc[:, "label"] = part["psn_sector"].apply(
        lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

    part_na = part[part["label"].isna()]
    part_na = part_na.copy()
    part_known = part[part["label"].notna()]
    part_known = part_known.copy()
    part_known.loc[:, "type"] = part_known["psn_sector"].apply(
        lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

    mod = fasttext.load_model("model")
    part_na["txt"] = part_na["doc_std_name"] + " " + part_na["name_source"]
    part_na["prediction"] = part_na["txt"].apply(lambda a: mod.predict(a))

    part_na["type"] = part_na["prediction"].apply(lambda a: re.search("pp|pm", str(a)).group(0))

    part_na = part_na.drop(columns=["prediction", "txt"])

    part2 = pd.concat([part_na, part_known])

    part2 = part2.drop(columns="label")

    # "clean" country code: empty string if country_source consists of 2 blank spaces or nan
    # there is also a country code "75" which must correspond to FR but receiving office is US
    # leave it this way for now
    part2["country_source"] = part2["country_source"].apply(lambda a: re.sub(r"\s{2}|nan", "", str(a)))

    # replace missing values by empty strings
    part2.fillna("", inplace=True)

    return part2


def add_type_to_part(part_table):
    """ This function updates the "type" variable ("PP": natural person or "PM": juridical person)
     in the participant table
    :param part_table: df with participant info
    :return: df with the "type" variable updated according to the INPADOC family
    """
    part = part_table.copy()

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
    part_init.loc[:, "isascii"] = part_init["name_source"].apply(tf.remove_punctuations).apply(tf.remove_accents).apply(
        tf.isascii)
    part_init.loc[:, "name_clean"] = part_init["name_source"].apply(tf.get_clean_name)

    part_init2 = add_type_to_part(part_init)
    part_init2["country_corrected"] = np.where(part_init2["country_corrected"] == "", part_init2["country_source"],
                                               part_init2["country_corrected"])
    part_init2.to_csv("part_init.csv", sep="|", index=False)

    part_init3 = part_init2.drop(columns={"applt_seq_nr", "invt_seq_nr"}).drop_duplicates()
    part_init3.to_csv("part.csv", sep="|", index=False)


if __name__ == "__main__":
    main()
