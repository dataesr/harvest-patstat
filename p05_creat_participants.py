#!/usr/bin/env python
# coding: utf-8

import os
import re

import fasttext
import numpy as np
import pandas as pd

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import text_functions as tf

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


def add_type_to_part(part_table: pd.DataFrame, lbl: str) -> pd.DataFrame:
    """
    Function to check if each doc_std_name, doc_std_id and name_source combinaison has only one label/type pp/pm
    If a combinaison has several types, get the sum of invt_sq_nr. If the sum is greater than 0, pp, else pm

    :param part_table: df with doc_std_name, doc_std_id, name_source, label/type and invt_sq_nr
    :param lbl: column with label/type info
    :return:
    """
    part = part_table.copy()

    # count number of occurences of label/type grouped by doc_std_name, doc_std_name_id and name_source
    # keep only cases where occurences are greater than 1 (same person has different types/labels)

    indiv_with_pm_type = part[["doc_std_name", "doc_std_name_id", "name_source", lbl]].drop_duplicates() \
        .groupby(["doc_std_name", "doc_std_name_id", "name_source"]).count().reset_index().rename(
        columns={lbl: f"count_{lbl}"})
    indiv_with_pm_type = indiv_with_pm_type[indiv_with_pm_type[f"count_{lbl}"] > 1]

    # merge identified people with part in order to get invt_seq_nr and all the occurences of doc_std_name,
    # doc_std_name_id and name_source

    # get the sum of invt_seq_nr : if it's greater than 0, pp, else pm

    double = pd.merge(indiv_with_pm_type, part, on=["doc_std_name", "doc_std_name_id", "name_source"], how="left")
    sum_invt = pd.DataFrame(double.groupby(["doc_std_name", "name_source"])["invt_seq_nr"].sum()).rename(
        columns={"invt_seq_nr": "sum_invt"})
    double = pd.merge(double, sum_invt, on=["doc_std_name", "name_source"], how="left")
    double["type2"] = double["sum_invt"].apply(lambda a: "pp" if a > 0 else "pm")
    double = double.copy()
    double["type3"] = np.where(double[lbl] != double["type2"], double["type2"], double[lbl])
    double = double[["doc_std_name", "name_source", "type3"]].rename(
        columns={"type3": f"double_{lbl}"}).drop_duplicates()

    # replace type/label in the part table with the new ones

    deduplicated = pd.merge(part, double, on=["doc_std_name", "name_source"], how="left")
    deduplicated = deduplicated.copy()
    deduplicated["type3"] = np.where(deduplicated[f"double_{lbl}"].isna(), deduplicated[lbl],
                                     deduplicated[f"double_{lbl}"])
    deduplicated = deduplicated.drop(columns=[lbl, f"double_{lbl}"]).rename(columns={"type3": lbl})

    return deduplicated


def initialization_participants(pat: pd.DataFrame, t207: pd.DataFrame, t206: pd.DataFrame,
                                part_history: pd.DataFrame) -> pd.DataFrame:
    """ This function initializes a table of participants from past corrected informations
    and new patstat version informations

    :param pat: df with application informations - T201 or part of - comes from programme "p03_patents"
    :param t207: df with table 207 PATSTAT or extract of
    :param t206: df with table 206 PATSTAT or extract of
    :param part_history: participants history - output "part_init" of p05 from previous PATSTAT edition
    :return: df with "clean" information on participants
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
                                  "id_paysage", "rnsr", "grid", "id_personne", "appln_id", "appln_nr", "appln_kind",
                                  "receiving_office", "key_appln_nr", "key_appln_nr_person"]],
                    on=["id_participant", "key_appln_nr_person", "appln_id", "appln_nr", "appln_kind",
                        "receiving_office", "key_appln_nr"], how="left") \
        .rename(
        columns={"appln_nr_epodoc": "id_patent", "person_name": "name_source", "person_address": "address_source",
                 "person_ctry_code": "country_source", "appln_publn_number": "publication_number"})

    # remove records where name is missing
    part = part.dropna(subset=["name_source"])

    # create label based on PSN sector
    part.loc[:, "label"] = part["psn_sector"].apply(
        lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

    # make sure that label info are the complete and the same for each combinaison of doc_std_name, doc_std_name_id
    # and name_source
    part2 = add_type_to_part(part, "label")

    return part2


def fasttext_learning(part_record: pd.DataFrame) -> pd.DataFrame:
    """
    Predict label pp/pm with fasttext model

    :param part_record: df create_part
    :return: df with pm/pp for each records
    """

    # split dataset between records with label and without label
    # fasttext model only on the dataset which doesn't have labels
    part_na = part_record[part_record["label"].isna()]
    part_na = part_na.copy()
    part_known = part_record[part_record["label"].notna()]
    part_known = part_known.copy()
    part_known.loc[:, "type"] = part_known.loc[:, "label"]

    # predict label with fasttext model
    mod = fasttext.load_model("model_test")
    part_na["txt"] = part_na["doc_std_name"] + " " + part_na["name_source"]
    part_na["prediction"] = part_na["txt"].apply(lambda a: mod.predict(a))

    # extract type from prediction
    part_na["type"] = part_na["prediction"].apply(lambda a: re.search("pp|pm", str(a)).group(0))

    part_na = part_na.drop(columns=["prediction", "txt"])

    # join datasets dataset with predicted types with dataset with known types
    part_concat = pd.concat([part_na, part_known])

    part_concat = part_concat.drop(columns="label")

    # "clean" country code: empty string if country_source consists of 2 blank spaces or nan
    # there is also a country code "75" which must correspond to FR but receiving office is US
    # leave it this way for now
    part_concat["country_source"] = part_concat["country_source"].apply(lambda a: re.sub(r"\s{2}|nan", "", str(a)))

    # replace missing values by empty strings
    part_concat.fillna("", inplace=True)

    # make sure that each doc_std_name, doc_std_name_id and name combinaison has only one type
    # if not, use invt_seq_nr to assign a value
    part_concat = add_type_to_part(part_concat, "type")

    return part_concat


def isascii(part_ds: pd.DataFrame) -> pd.DataFrame:
    # On crée la variable isascii,
    # pour pouvoir prendre en compte ou non dans la suite des traitements les caractères spéciaux - alphabets non latins
    # et une variable nom propre
    part_ds.loc[:, "isascii"] = part_ds["name_source"].apply(tf.remove_punctuations).apply(tf.remove_accents).apply(
        tf.isascii)
    part_ds.loc[:, "name_clean"] = part_ds["name_source"].apply(tf.get_clean_name)

    part_ds2 = part_ds.copy()
    part_ds2["country_corrected"] = np.where(part_ds2["country_corrected"] == "", part_ds2["country_source"],
                                             part_ds2["country_corrected"])

    return part_ds2


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
    create_part = initialization_participants(patents, tls207, tls206, old_part)
    part_init = fasttext_learning(create_part)

    part_init2 = isascii(part_init)

    part_init2.to_csv("part_init.csv", sep="|", index=False)

    part_init3 = part_init2.drop(columns={"applt_seq_nr", "invt_seq_nr"}).drop_duplicates()
    part_init3.to_csv("part.csv", sep="|", index=False)


if __name__ == "__main__":
    main()
