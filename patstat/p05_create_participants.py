#!/usr/bin/env python
# coding: utf-8

import os
import re

import datatable as dt
import fasttext
import numpy as np
import pandas as pd

from utils import swift

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types
from patstat import text_functions as tf

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
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


def add_type_to_part(part_table: pd.DataFrame, lbl: str) -> pd.DataFrame:
    part = part_table.copy()

    for col in list(part.columns):
        if str(part[col].dtypes) == "Int64":
            part[col] = part[col].astype(int)
    part_dt = dt.Frame(part)
    part_count = part[["doc_std_name", "doc_std_name_id", "name_source", lbl]].drop_duplicates()
    part_count_dt = dt.Frame(part_count)
    part_count_dt[:, dt.update(count_label=dt.count()), dt.by("doc_std_name", "doc_std_name_id", "name_source")]
    part_count_dt = part_count_dt[dt.f.count_label > 1, :]
    print(f"9 : ne garde que les cas où les types de {lbl} sont supérieurs à 1", flush=True)

    part_count_dt.key = ("doc_std_name", "doc_std_name_id", "name_source", lbl)

    part_dt = part_dt[:, :, dt.join(part_count_dt)]
    print("10 : jointure left part_dt et part_count_dt", flush=True)

    part_dt2 = part_dt[dt.f.count_label != None, :]
    part_dt2_autre = part_dt[dt.f.count_label == None, :]
    part_dt2_autre = part_dt2_autre[:, dt.f[:].remove(dt.f.count_label)]
    print("11 : séparation de part_dt en 2 selon que count_label est vide ou non", flush=True)

    part_dt2[:, dt.update(seq_nr=dt.sum(dt.f.invt_seq_nr)), dt.by("doc_std_name_id", "doc_std_name", "name_source")]
    print("12-1 : calcul seq_nr", flush=True)

    part_dt2[:, dt.update(label2=dt.ifelse(dt.f.seq_nr > 0, "pp", "pm")),
    dt.by("doc_std_name", "doc_std_name_id", "name_source")]
    print(f"12-2 : correction {lbl} pour ceux dédupliqués", flush=True)

    del part_dt2[:, [lbl, "count_label", "seq_nr"]]
    part_dt2.names = {"label2": lbl}

    concat_dt = dt.rbind(part_dt2, part_dt2_autre)
    print("12-3 : concaténation des 2 tables part_dt2 et part_dt2_autre", flush=True)

    concat_df = concat_dt.to_pandas()
    print("12-4 : transformation dt en df", flush=True)

    for col in list(concat_df.columns):
        if str(concat_df[col].dtypes) == "int64":
            concat_df[col] = concat_df[col].astype(pd.Int64Dtype())

    print("12-5 : pd.Int64Dtype()", flush=True)

    return concat_df


# def add_type_to_part(part_table: pd.DataFrame, lbl: str) -> pd.DataFrame:
#     key_dict = {}
#     for row in part_table.itertuples():
#         current_key = f'{row.doc_std_name};;;{row.doc_std_name_id};;;{row.name_source}'
#         if current_key not in key_dict:
#             key_dict[current_key] = {'types': [], 'seq_nr': []}
#         if lbl == "label":
#             key_dict[current_key]['types'].append(row.label)
#         elif lbl == "type":
#             key_dict[current_key]['types'].append(row.type)
#         else:
#             assert (False)
#         key_dict[current_key]['seq_nr'].append(row.invt_seq_nr)
#     for current_key in key_dict:
#         if len(set(key_dict[current_key]['types'])) == 1:
#             key_dict[current_key]['type_final'] = key_dict[current_key]['types'][0]
#         else:
#             seq_nr_sum = np.sum(key_dict[current_key]['seq_nr'])
#             if seq_nr_sum > 0:
#                 key_dict[current_key]['type_final'] = 'pp'
#             else:
#                 key_dict[current_key]['type_final'] = 'pm'
#
#     ix = 0
#     for row in part_table.itertuples():
#         current_key = f'{row.doc_std_name};;;{row.doc_std_name_id};;;{row.name_source}'
#         part_table.at[ix, lbl] = key_dict[current_key]['type_final']
#         ix += 1
#
#     return part_table


# def add_type_to_part(part_table: pd.DataFrame, lbl: str) -> pd.DataFrame:
#     """
#     Function to check if each doc_std_name, doc_std_id and name_source combinaison has only one label/type pp/pm
#     If a combinaison has several types, get the sum of invt_sq_nr. If the sum is greater than 0, pp, else pm
#
#     :param part_table: df with doc_std_name, doc_std_id, name_source, label/type and invt_sq_nr
#     :param lbl: column with label/type info
#     :return:
#     """
#     # part = part_table.copy()
#     # print("9 : copie table part", flush=True)
#
#     # count number of occurences of label/type grouped by doc_std_name, doc_std_name_id and name_source
#     # keep only cases where occurences are greater than 1 (same person has different types/labels)
#
#     part_table["id"] = part_table["doc_std_name"] + part_table["doc_std_name_id"].astype(str) + part_table[
#         "name_source"]
#
#     indiv_with_pm_type = part_table[["id", lbl]].drop_duplicates() \
#         .groupby("id").count().reset_index().rename(
#         columns={lbl: f"count_{lbl}"})
#     indiv_with_pm_type = indiv_with_pm_type.loc[indiv_with_pm_type[f"count_{lbl}"] > 1]
#
#     # merge identified people with part in order to get invt_seq_nr and all the occurences of doc_std_name,
#     # doc_std_name_id and name_source
#
#     # get the sum of invt_seq_nr : if it's greater than 0, pp, else pm
#
#     double = part_table.loc[part_table["id"].isin(indiv_with_pm_type["id"])]
#     del [indiv_with_pm_type]
#     for id in double["id"]:
#         double.loc[double["id"] == id, "sum_invt"] = double.loc[double["id"] == id, "invt_seq_nr"].sum()
#     double["sum_invt"] = double["sum_invt"].astype(int)
#     double["type2"] = double["sum_invt"].apply(lambda a: "pp" if a > 0 else "pm")
#     # double = double.copy()
#     double.loc[double[lbl] != double["type2"], "type3"] = double.loc[double[lbl] != double["type2"], "type2"]
#     double.loc[double[lbl] == double["type2"], "type3"] = double.loc[double[lbl] == double["type2"], lbl]
#     # double["type3"] = np.where(double[lbl] != double["type2"], double["type2"], double[lbl])
#     double = double[["id", "type3"]].rename(
#         columns={"type3": f"double_{lbl}"}).drop_duplicates()
#
#     # replace type/label in the part table with the new ones
#
#     deduplicated = pd.merge(part_table, double, on="id", how="left")
#     print("10 : jointure part et double", flush=True)
#     del [double]
#     # deduplicated = deduplicated.copy()
#     # print("11 : Copie deduplicated", flush=True)
#     deduplicated.loc[deduplicated[f"double_{lbl}"].isna(), "type3"] = deduplicated.loc[
#         deduplicated[f"double_{lbl}"].isna(), lbl]
#     deduplicated.loc[deduplicated[f"double_{lbl}"].notna(), "type3"] = deduplicated.loc[
#         deduplicated[f"double_{lbl}"].notna(), f"double_{lbl}"]
#     print("12 : np.where dedupliacted", flush=True)
#     deduplicated = deduplicated.drop(columns=[lbl, f"double_{lbl}", "id"]).rename(columns={"type3": lbl})
#
#     return deduplicated


def initialization_participants(pat: pd.DataFrame, t207: pd.DataFrame, t206: pd.DataFrame,
                                part_history: pd.DataFrame) -> pd.DataFrame:
    """ This function initializes a table of participants from past corrected information
    and new patstat version informations

    :param pat: df with application informations - T201 or part of - comes from programme "p03_patents"
    :param t207: df with table 207 PATSTAT or extract of
    :param t206: df with table 206 PATSTAT or extract of
    :param part_history: participants history - output "part_init" of p05 from previous PATSTAT edition
    :return: df with "clean" information on participants
    """

    tbl207 = pat.merge(t207, how="inner", on="appln_id")
    print("5 : tbl207 : jointure patents et tls207", flush=True)

    new_participants = pd.merge(tbl207, t206, how="inner", on="person_id")
    print("6 : new parts jointure tbl207 tls206", flush=True)

    # we create a unique participant ID "key_appln_nr_person"
    # before "id_participant" but "appln_nr_epodoc" is deprecated and will be removed at some point
    # can keep "appln_nr_epodoc" and "id_participant" for now
    # new_participants["id_participant"] = new_participants["appln_nr_epodoc"].astype(str) + "_" + new_participants[
    #     "person_id"].astype(str)
    new_participants["key_appln_nr_person"] = new_participants["key_appln_nr"] + "_" + new_participants[
        "person_id"].astype(str)

    # merge previous file with latest PATSTAT data on "id_participant", "key_appln_nr_person", "appln_id", "appln_nr",
    # "appln_kind", "receiving_office" and "key_appln_nr"
    part = pd.merge(new_participants[
                        ["person_id", "docdb_family_id", "inpadoc_family_id",
                         "applt_seq_nr", "doc_std_name", "doc_std_name_id",
                         "earliest_filing_date", "invt_seq_nr", "person_name", "person_address", "person_ctry_code",
                         "psn_sector", "psn_id",
                         "psn_name", "appln_publn_number", "appln_auth", "appln_id", "appln_nr", "appln_kind",
                         "receiving_office", "key_appln_nr_person", "key_appln_nr"]],
                    part_history[["old_name", "country_corrected", "siren", "siret",
                                  "id_paysage", "rnsr", "grid", "sexe", "id_personne", "type", "idref", "oc", "ror",
                                  "appln_id", "appln_nr",
                                  "appln_kind",
                                  "receiving_office", "key_appln_nr", "key_appln_nr_person"]],
                    on=["key_appln_nr_person", "appln_id", "appln_nr", "appln_kind",
                        "receiving_office", "key_appln_nr"], how="left") \
        .rename(
        columns={"person_name": "name_source", "person_address": "address_source",
                 "person_ctry_code": "country_source", "appln_publn_number": "publication_number"})

    print("7 : jointure part_init new parts", flush=True)

    # remove records where name is missing
    part = part.dropna(subset=["name_source"])

    # create label based on PSN sector if type is missing
    part.loc[:, "label"] = part.loc[part["type"].isna(), "psn_sector"].apply(
        lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

    part.loc[:, "label"] = part.loc[part["type"].notna(), "type"]

    part = part.drop(columns="type")
    print("8 : nettoyage")

    # make sure that label info are the complete and the same for each combinaison of doc_std_name, doc_std_name_id
    # and name_source
    part2 = add_type_to_part(part, "label")
    print("8-2 : add_type_to_part réussi", flush=True)

    return part2


def fasttext_learning(part_record: pd.DataFrame) -> pd.DataFrame:
    """
    Predict label pp/pm with fasttext model

    :param part_record: df create_part
    :return: df with pm/pp for each records
    """

    # split dataset between records with label and without label
    # fasttext model only on the dataset which doesn't have labels
    part_na = part_record.loc[part_record["label"].isna()]
    # part_na = part_na.copy()
    part_known = part_record.loc[part_record["label"].notna()]
    # part_known = part_known.copy()
    part_known.loc[:, "type"] = part_known.loc[:, "label"]

    # predict label with fasttext model
    swift.download_object('patstat', 'model_test', 'model_test')
    mod = fasttext.load_model("model_test")
    print("14 : chargement du modèle fasttext", flush=True)

    part_na["txt"] = part_na["doc_std_name"] + " " + part_na["name_source"]
    part_na["prediction"] = part_na["txt"].apply(lambda a: mod.predict(a))
    print("15 : prédictions type", flush=True)

    # extract type from prediction
    part_na["type"] = part_na["prediction"].apply(lambda a: re.search("pp|pm", str(a)).group(0))
    print("16 : extraction type de la prédiction", flush=True)

    part_na = part_na.drop(columns=["prediction", "txt"])

    # join datasets dataset with predicted types with dataset with known types
    part_concat = pd.concat([part_na, part_known]).drop(columns="label")
    print("17 : concat part_na et part_known", flush=True)

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
    """
    Create 'isascii' to indicate if a name is written in the latin alphabet and with special characters,
    name_clean with name_source cleaned
    and replace missing values in country_correcetd by values in country_source
    :param part_ds: df with participant type
    :return: df
    """
    part_ds.loc[:, "isascii"] = part_ds["name_source"].apply(tf.remove_punctuations).apply(tf.remove_accents).apply(
        tf.isascii)
    print("19 : fin remove punctuations et remove accents", flush=True)
    part_ds.loc[:, "name_clean"] = part_ds["name_source"].apply(tf.get_clean_name)
    print("20 : fin name source", flush=True)

    # part_ds2 = part_ds.copy()
    # print("21 : fin copy part_ds2", flush=True)
    part_ds.loc[part_ds["country_corrected"] == "", "country_corrected"] = part_ds.loc[
        part_ds["country_corrected"] == "", "country_source"]
    # part_ds2["country_corrected"] = np.where(part_ds2["country_corrected"] == "", part_ds2["country_source"],
    #                                          part_ds2["country_corrected"])

    print("22 : fin remplacement pays manquant par \"\"", flush=True)

    return part_ds


def start_part():
    # set working directory
    os.chdir(DATA_PATH)
    # file which served to create part_init: old_part_key.csv
    # old_part = pd.read_csv("old_part_key2.csv",
    #                            sep='|',
    #                            dtype=types.partfin_types,
    #                            encoding="utf-8")
    # old_part = old_part.rename(columns={"name_corrected": "old_name"})
    patents = pd.read_csv("patent.csv", sep="|", dtype=types.patent_types)
    print("1 : chargement patents", flush=True)
    swift.download_object('patstat', 'part_init_p05.csv', 'part_init_p05.csv')
    old_part = pd.read_csv("part_init_p05.csv",
                           sep='|',
                           dtype=types.part_init_types,
                           encoding="utf-8",
                           engine="python")
    old_part = old_part.drop_duplicates()
    old_part.to_csv("part_init_backup.csv", sep="|", index=False)
    print("2 : chargement part_init", flush=True)

    tls207 = cfq.filtering("tls207", patents, "appln_id", DICT["tls207"])
    print("3 : chargement tls207", flush=True)
    tls206 = cfq.filtering("tls206", tls207, "person_id", DICT["tls206"])
    print("4 : chargement tls206", flush=True)
    create_part = initialization_participants(patents, tls207, tls206, old_part)
    print("13 : fin initialization_participants", flush=True)
    part_init = fasttext_learning(create_part)
    print("18 : fin fasttext", flush=True)

    part_init2 = isascii(part_init)
    print("23 : fin isascii", flush=True)

    part_init2.to_csv("part_init_p05.csv", sep="|", index=False)
    swift.upload_object('patstat', 'part_init_p05.csv')

    part_init3 = part_init2.drop(columns={"applt_seq_nr", "invt_seq_nr"}).drop_duplicates()
    part_init3.to_csv("part_p05.csv", sep="|", index=False)
    swift.upload_object('patstat', 'part_p05.csv')
