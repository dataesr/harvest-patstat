#!/usr/bin/env python
# coding: utf-8

import concurrent.futures
import logging
import sys
import threading

import requests
from retry import retry

import os
import re
from utils import swift

import numpy as np
import pandas as pd

from patstat import dtypes_patstat_declaration as types

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

URL_STRUCTURES = "https://scanr-api.enseignementsup-recherche.gouv.fr/elasticsearch/structures/_search"
URL_PEOPLE = "https://scanr-api.enseignementsup-recherche.gouv.fr/elasticsearch/persons/_search"


def check_fam(df: pd.DataFrame) -> pd.DataFrame:
    liste = []

    logger = get_logger(threading.current_thread().name)
    logger.info("start")

    for fam in list(df["docdb_family_id"]):
        df1 = df.loc[df["docdb_family_id"] == fam]
        sir = df1.loc[df1["siren"].notna()]
        pay = sir.loc[sir["Paysage_id"].notna()]
        if len(pay) > 0:
            liste.append(df1)

    df2 = pd.concat(liste)

    logger.info("end")
    return df2


def get_logger(name):
    """
    This function helps to follow the execution of the parallel computation.
    """
    loggers = {}
    if name in loggers:
        return loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    loggers[name] = logger
    return loggers[name]


def res_futures(dict_nb: dict, query) -> pd.DataFrame:
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=11, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                data = future.result()
                res.append(data)
                jointure = pd.concat(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)

    return jointure


def requete(ul: str, form: str, ky: str) -> object:
    """
    This function queries a website.
    """
    adr = ul + form + ky
    res = requests.get(adr)
    status = res.status_code
    if status != 200:
        raise ConnectionError(f"Failed while trying to access the URL with status code {status}")
    else:
        print("URL successfully accessed", flush=True)
    return res


@retry(tries=3, delay=5, backoff=5)
def df_req(ul: str, frm: str, ky: str) -> pd.DataFrame:
    """
    This functions takes the text result (CSV with ; as separator) from the query, writes it and reads it as a df.
    It returns a df
    """
    res = requete(ul, frm, ky)
    text = res.text
    with open("structures.csv", "w") as f:
        f.write(text)
    df = pd.read_csv("structures.csv", sep=";", encoding="utf-8", engine="python",
                     dtype=types.structures_types)
    return df


@retry(tries=3, delay=5, backoff=5)
def df_sirens(ul: str, frm: str, ky: list) -> pd.DataFrame:
    """
    This functions takes the text result (CSV with ; as separator) from the query, writes it and reads it as a df.
    It returns a df
    """
    liste_res_structures = []

    for siren in ky:
        res = requete(ul, frm, siren)
        if res.text != "":
            result = res.json()
            ids = result.get("externalIds")
            dict_ids = {"libelle": result.get("label").get("fr")}
            for item in ids:
                typ = item.get("type")
                if typ in ["sirene", "grid", "idref", "siret"]:
                    id = item.get("id")
                    dict_ids[typ] = id
                df = pd.DataFrame(data=[dict_ids])
                liste_res_structures.append(df)

    df_id_etab = pd.concat(liste_res_structures)
    return df_id_etab


def cpt_struct(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function checks how many Paysage ID there are for each unique SIREN.
    It returns a df.
    """
    cmpt_struc = df[["uo_lib", "siren", "identifiant_interne"]].drop_duplicates()
    cmpt_struc = cmpt_struc.loc[cmpt_struc["siren"].notna()]
    cmpt_struc = cmpt_struc.sort_values("siren")
    cmpt_struc = cmpt_struc[["siren", "identifiant_interne"]].groupby("siren").nunique(
        dropna=False).reset_index()

    return cmpt_struc


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    prct10 = int(round(len(df) * 10 / 100, 0))
    dict_nb = {}
    deb = 0
    fin = prct10
    dict_nb["df1"] = df.iloc[deb:fin, :]
    deb = fin
    dixieme = 10 * prct10
    reste = (len(df) - dixieme)
    fin_reste = len(df) + 1
    for i in range(2, 11):
        fin = (i * prct10 + 1)
        dict_nb["df" + str(i)] = df.iloc[deb:fin, :]
        if reste > 0:
            dict_nb["reste"] = df.iloc[fin: fin_reste, :]
        deb = fin

    return dict_nb


@retry(tries=3, delay=5, backoff=5)
def req_scanr_stru(df_stru_fuz: pd.DataFrame) -> pd.DataFrame:
    headers = os.getenv("HEADERS")

    dict_res = {"doc_std_name_id": [], "doc_std_name": [], "externalIds": [], "label_default": [], "label_fr": []}

    logger = get_logger(threading.current_thread().name)
    logger.info("start")

    for _, r in df_stru_fuz.iterrows():
        query1 = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": r.doc_std_name,
                                "default_field": "label.default"
                            }
                        },
                        {
                            "query_string": {
                                "query": r.doc_std_name,
                                "default_field": "label.fr"
                            }
                        }
                    ]
                }
            }
        }

        res1 = requests.get(URL_STRUCTURES, headers=headers, json=query1).json()
        if res1.get("status"):
            pass
        elif len(res1.get("hits").get("hits")) > 0:
            dict_res["person_id"].append(r.doc_std_name_id)
            dict_res["name_source"].append(r.doc_std_name)
            dict_res["externalIds"].append(res1.get("hits").get("hits")[0].get("_source").get("externalIds"))
            dict_res["label_default"].append(res1.get("hits").get("hits")[0].get("_source").get("label").get("default"))
            dict_res["label_fr"].append(res1.get("hits").get("hits")[0].get("_source").get("label").get("fr"))

    df = pd.DataFrame(data=dict_res)

    logger.info("end")

    return df


@retry(tries=3, delay=5, backoff=5)
def req_scanr_person(df_pers_fuz: pd.DataFrame) -> pd.DataFrame:
    headers = os.getenv("HEADERS")

    dict_res = {"doc_std_name_id": [], "doc_std_name": [], "externalIds": [], "fullName": []}

    logger = get_logger(threading.current_thread().name)
    logger.info("start")

    for fam in list(df_pers_fuz["docdb_family_id"].unique()):
        df = df_pers_fuz.loc[df_pers_fuz["docdb_family_id"] == fam]
        pm = list(df.loc[df["type"] == "pm", "siren"].unique())
        df_pers = df.loc[df["type"] == "pp", ["doc_std_name_id", "doc_std_name"]]
        if len(df_pers) > 0 and len(pm) > 0:
            for _, r in df_pers.iterrows():
                query2 = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "fullName": {
                                            "query": r.doc_std_name,
                                            "operator": "AND"
                                        }
                                    }
                                }
                            ],
                            "filter": [
                                {
                                    "terms": {
                                        "affiliations.structure.id": pm
                                    }
                                }
                            ]
                        }
                    }
                }

                res2 = requests.get(URL_PEOPLE, headers=headers, json=query2).json()
                if res2.get("status"):
                    pass
                elif len(res2.get("hits").get("hits")) > 0:
                    dict_res["doc_std_name_id"].append(r.doc_std_name_id)
                    dict_res["doc_std_name"].append(r.doc_std_name)
                    dict_res["externalIds"].append(
                        res2.get("hits").get("hits")[0].get("_source").get("externalIds"))
                    dict_res["fullName"].append(
                        res2.get("hits").get("hits")[0].get("_source").get("fullName"))

                df2 = pd.DataFrame(data=dict_res)

            logger.info("end")

    return df2


def structures_paysage():
    ## STRUCTURES - from dataESR ##
    url = "https://data.enseignementsup-recherche.gouv.fr/explore/dataset/fr_esr_paysage_structures_all/download/"
    form = "?format=csv&timezone=Europe/Berlin&lang=fr&use_labels_for_header=true&csv_separator=%3B"
    key = f"&apikey={os.getenv('ODS_API_KEY')}"

    structures = df_req(url, form, key)

    # select only cases where there is only 1 Paysage ID per SIREN

    compte_structures = cpt_struct(structures)

    compte_struct_uni = compte_structures.loc[compte_structures["identifiant_interne"] == 1]

    siren_uni = set(compte_struct_uni["siren"])

    struct_uni = structures.loc[structures["siren"].isin(siren_uni)].reset_index().drop(columns="index")
    struct_uni = struct_uni.rename(columns={"siret": "siret2"})
    struct_uni = struct_uni[
        ["siren", "siret2", "identifiant_interne", "identifiant_idref", "identifiant_ror", "identifiant_grid",
         "identifiant_rnsr"]]

    return struct_uni


def clean_siren_siret_paysage(df_part: pd.DataFrame, struct_uni: pd.DataFrame) -> pd.DataFrame:
    df_part["siren"] = df_part["siren"].replace(r"\(\'", "", regex=True)
    df_part.loc[(df_part["siret"].notna()) & (df_part["siret"].str.findall(r"\(\'")), "siret2"] = df_part.loc[
        (df_part["siret"].notna()) & (df_part["siret"].str.findall(r"\(\'")), "siret"].replace(r"\(\'|\'|\)", "",
                                                                                               regex=True).str.split(
        ",")

    siret = {"key_appln_nr_person": [], "siret": [], "siret3": []}

    for _, r in df_part.loc[df_part["siret2"].notna()].iterrows():
        key = r.key_appln_nr_person
        sir = r.siret
        s2 = []
        for i in r.siret2:
            res = re.sub(r"\s+", "", i)
            s2.append(res)
        siret["key_appln_nr_person"].append(key)
        siret["siret"].append(sir)
        siret["siret3"].append(s2)

        sup_esp = pd.DataFrame(data=siret)

    df_part = pd.merge(df_part, sup_esp, on=["key_appln_nr_person", "siret"], how="left")

    # clean SIREN and SIRET in certainn cases

    df_part = df_part.drop(columns="siret2").rename(columns={"siret3": "siret2"})

    df_part.loc[df_part["siret2"].notna(), "siren2"] = df_part.loc[
        df_part["siret2"].notna(), "siret2"].apply(
        lambda a: ", ".join([item[0:9] for item in a]))

    df_part.loc[df_part["siret2"].notna(), "siret2"] = df_part.loc[df_part["siret2"].notna(), "siret2"].apply(
        lambda a: ", ".join(a))

    df_part.loc[df_part["siret2"].notna(), "siret"] = df_part.loc[df_part["siret2"].notna(), "siret2"]

    df_part.loc[df_part["siren2"].notna(), "siren"] = df_part.loc[df_part["siren2"].notna(), "siren2"]

    df_part = df_part.drop(columns=["siret2", "siren2"])

    # keep columns final df

    df_part2 = df_part

    # split applicant df between those who alreday have Paysage ID and those who don't

    df_part2_na = df_part2.loc[df_part2["id_paysage"].notna()]
    struct_uni2 = struct_uni.drop(columns="identifiant_interne")
    df_part2_na = pd.merge(df_part2_na, struct_uni2, on="siren")
    df_part2_na.loc[df_part2_na["idref"].isna(), "idref"] = df_part2_na.loc[
        df_part2_na["idref"].isna(), "identifiant_idref"]
    df_part2_na.loc[df_part2_na["grid"].isna(), "grid"] = df_part2_na.loc[
        df_part2_na["grid"].isna(), "identifiant_grid"]
    df_part2_na.loc[df_part2_na["rnsr"].isna(), "rnsr"] = df_part2_na.loc[
        df_part2_na["rnsr"].isna(), "identifiant_rnsr"]
    df_part2_na.loc[df_part2_na["ror"].isna(), "ror"] = df_part2_na.loc[
        df_part2_na["ror"].isna(), "identifiant_ror"]

    df_part2 = df_part2.loc[df_part2["id_paysage"].isna()]

    df_part2 = df_part2.drop_duplicates()

    # for applicants who don't have a Paysage ID but have a SIREN, match structure and applicant df

    df_part_nna = df_part2.loc[df_part2["siren"].notna()]

    df_part_virgule = df_part_nna.loc[df_part_nna["siren"].str.contains(", ")]
    df_part_nna2 = df_part_nna.loc[~df_part_nna["siren"].isin(df_part_virgule["siren"])]

    df_part_na = df_part2.loc[df_part2["siren"].isna()]

    df_part_nna2 = pd.merge(df_part_nna2, struct_uni, on="siren", how="left")
    df_part_nna2.loc[df_part_nna2["identifiant_interne"].notna(), "Paysage_id"] = df_part_nna2.loc[
        df_part_nna2["identifiant_interne"].notna(), "identifiant_interne"]
    df_part_nna2.loc[df_part_nna2["idref"].isna(), "idref"] = df_part_nna2.loc[
        df_part_nna2["idref"].isna(), "identifiant_idref"]
    df_part_nna2.loc[df_part_nna2["grid"].isna(), "grid"] = df_part_nna2.loc[
        df_part_nna2["grid"].isna(), "identifiant_grid"]
    df_part_nna2.loc[df_part_nna2["rnsr"].isna(), "rnsr"] = df_part_nna2.loc[
        df_part_nna2["rnsr"].isna(), "identifiant_rnsr"]
    df_part_nna2.loc[df_part_nna2["ror"].isna(), "ror"] = df_part_nna2.loc[
        df_part_nna2["ror"].isna(), "identifiant_ror"]

    df_part_nna2 = df_part_nna2.drop(
        columns=["identifiant_interne", "identifiant_idref", "identifiant_grid", "identifiant_rnsr",
                 "identifiant_ror"])

    paysage_id = {"key_appln_nr_person": [], "siren": [], "paysage": [], "idref": [], "grid": [], "rnsr": [], "ror": []}

    for _, r in df_part_virgule.iterrows():
        key = r.key_appln_nr_person
        s = r["siren"].split(", ")
        p2 = []
        i2 = []
        g2 = []
        rn2 = []
        ro2 = []
        for i in s:
            res = struct_uni.loc[struct_uni["siren"] == i, "identifiant_interne"].values
            res_idref = struct_uni.loc[struct_uni["siren"] == i, "identifiant_idref"].values
            res_grid = struct_uni.loc[struct_uni["siren"] == i, "identifiant_grid"].values
            res_rnsr = struct_uni.loc[struct_uni["siren"] == i, "identifiant_rnsr"].values
            res_ror = struct_uni.loc[struct_uni["siren"] == i, "identifiant_ror"].values
            for item in [res, res_idref, res_grid, res_rnsr, res_ror]:
                if item.size == 0:
                    r = ""
                else:
                    r = item[0]
                    if item == res:
                        p2.append(r)
                    elif item == res_idref:
                        i2.append(r)
                    elif item == res_grid:
                        g2.append(r)
                    elif item == res_rnsr:
                        rn2.append(r)
                    else:
                        ro2.append(r)

        paysage_id["key_appln_nr_person"].append(key)
        paysage_id["siren"].append(s)
        paysage_id["paysage"].append(p2)
        paysage_id["idref"].append(i2)
        paysage_id["grid"].append(g2)
        paysage_id["rnsr"].append(rn2)
        paysage_id["ror"].append(ro2)

    multi_siren = pd.DataFrame(data=paysage_id)

    for col in ["siren", "paysage", "idref", "grid", "rnsr", "ror"]:
        multi_siren[col] = multi_siren[col].apply(lambda a: ", ".join(a))
    multi_siren[col] = multi_siren[col].replace(r"^,$", "", regex=True)
    multi_siren[col] = multi_siren[col].replace(r"^\s{0,},\s{0,}|\s{0,},\s{0,}$", "", regex=True)

    df_part_virgule2 = pd.merge(df_part_virgule, multi_siren, on=["key_appln_nr_person", "siren"], how="left")
    df_part_virgule2.loc[df_part_virgule2["Paysage_id"].isna(), "Paysage_id"] = df_part_virgule2.loc[
        df_part_virgule2["Paysage_id"].isna(), "paysage"]

    df_part_virgule2 = df_part_virgule2.drop(columns="paysage")

    # concat all the df again

    df_part3 = pd.concat([df_part_na, df_part_nna2, df_part2_na, df_part_virgule2])

    return df_part3


def get_ids(d_ids: dict, df_res: pd.DataFrame, liste_id: list, col_id: str, col_name: str) -> pd.DataFrame:
    for _, r in df_res.iterrows():
        ids = r.externalIds
        if ids is not None:
            d_ids[col_id].append(r[col_id])
            d_ids[col_name].append(r[col_name])
            for item in ids:
                liste_keys = list(item.keys())
                for it in liste_keys:
                    if it not in ["id", "type"]:
                        ids.remove(item)

            lab_id = [id.get("type") for id in ids]
            lab_id2 = {}
            for item in lab_id:
                lab_id2[item] = lab_id.count(item)
                if lab_id2[item] > 1:
                    del lab_id2[item]

            keys = [k for k in list(lab_id2.keys()) if k in liste_id]
            not_keys = list(set(liste_id) - set(keys))

            for k in [id.get("type") for id in ids]:
                if k not in keys:
                    for item in ids:
                        if item["type"] == k:
                            ids.remove(item)

            for item in ids:
                typ = item.get("type")
                id = item.get("id")
                d_ids[typ].append(id)

            for typ in not_keys:
                d_ids[typ].append("")

    df_ids = pd.DataFrame(data=d_ids)

    return df_ids


def part_final():
    # set working directory
    os.chdir(DATA_PATH)

    structures_uni = structures_paysage()
    fam = pd.read_csv("families.csv", sep="|", encoding="utf-8", engine="python", dtype=types.patent_types)

    # load part_init, part, part_entp and part_individuals to merge them and create a single file with all participants
    part_init = pd.read_csv('part_init_p05.csv', sep='|', dtype=types.part_init_types, engine="python")
    part = pd.read_csv('part_p05.csv', sep='|', dtype=types.part_init_types)

    part_entp = pd.read_csv('part_entp_final.csv', sep='|',
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
        ['key_appln_nr_person', 'key_appln_nr', 'person_id', 'id_patent', 'docdb_family_id', 'inpadoc_family_id',
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

    particip.to_csv('part_p08_old.csv', sep='|', index=False, encoding="utf-8")

    swift.upload_object('patstat', 'part_p08_old.csv')

    part = clean_siren_siret_paysage(particip, structures_uni)
    stru_fuz = part.loc[
        (part["siren"].isna()) & (part["type"] == "pm") & (part["country_corrected"] == "FR"), [
            "person_id", "name_source"]].drop_duplicates()

    sub_structure = subset_df(stru_fuz)

    futures_stru = res_futures(sub_structure, req_scanr_stru)

    dict_ids = {"person_id": [], "name_source": [], "sirene": [], "grid": [], "idref": [], "siret": []}

    key_ids_stru = ["sirene", "grid", "idref", "siret"]

    df_id_etab = get_ids(dict_ids, futures_stru, key_ids_stru, "person_id", "name_source")

    df_id_etab["siren2"] = df_id_etab.loc[df_id_etab["sirene"] != "", "sirene"].apply(lambda a: a.split()[0][0:9])

    df_id_etab = df_id_etab.rename(columns={"siret": "siret2", "grid": "grid2", "idref": "idref2"})

    part2 = pd.merge(part, df_id_etab, on=["person_id", "name_source"], how="left")

    part2.loc[part2["siren"].isna(), "siren"] = part2.loc[part2["siren"].isna(), "siren2"]
    part2.loc[part2["siret"].isna(), "siret"] = part2.loc[part2["siret"].isna(), "siret2"]
    part2.loc[part2["grid"].isna(), "grid"] = part2.loc[part2["grid"].isna(), "grid2"]
    part2.loc[part2["idref"].isna(), "idref"] = part2.loc[part2["idref"].isna(), "idref2"]

    part2 = part2.drop(columns=["sirene", "siret2", "grid2", "idref2", "siren2"])

    sub_fam = subset_df(pd.DataFrame(data={"docdb_family_id": fam["docdb_family_id"].unique()}))

    dict_nb = {}

    for key in list(sub_fam.keys()):
        dict_nb[key] = part2.loc[part2["docdb_family_id"].isin(sub_fam[key]["docdb_family_id"])]

    df_fam_check = res_futures(dict_nb, check_fam)

    df_fam_check2 = df_fam_check[
        ["doc_std_name_id", "doc_std_name", "siren", "type", "docdb_family_id"]].drop_duplicates()
    df_fam_check3 = df_fam_check2.loc[~((df_fam_check2["type"] == "pm") & (df_fam_check2["siren"].isna()))]

    sub_fam2 = subset_df(pd.DataFrame(data={"docdb_family_id": df_fam_check3["docdb_family_id"].unique()}))

    dict_fam = {}

    for key in list(sub_fam2.keys()):
        dict_fam[key] = df_fam_check3.loc[df_fam_check3["docdb_family_id"].isin(sub_fam2[key]["docdb_family_id"])]

    df_req_pers = res_futures(dict_fam, req_scanr_person)

    dict_ids_pp = {"doc_std_name_id": [], "doc_std_name": [], "idref": [], "paysage": [], "id_hal": [], "docid_hal": [],
                   "orcid": []}

    key_ids_pp = ["idref", "paysage", "id_hal", "docid_hal", "orcid"]

    df_ids_pp = get_ids(dict_ids_pp, df_req_pers, key_ids_pp, "doc_std_name_id", "doc_std_name")
    df_ids_pp = df_ids_pp.rename(columns={"idref": "idref_pp", "paysage": "paysage_pp"})

    part3 = pd.merge(part2, df_ids_pp, on=["doc_std_name_id", "doc_std_name"], how="left")
    part3.loc[(part3["idref"].isna()) & (part3["fullName"].notna()), "idref"] = part3.loc[
        (part3["idref"].isna()) & (part3["fullName"].notna()), "idref_pp"]
    part3.loc[(part3["idref"] == "") & (part3["fullName"].notna()), "idref"] = part3.loc[
        (part3["idref"] == "") & (part3["fullName"].notna()), "idref_pp"]
    part3 = part3.drop(columns="idref_pp")

    part3.loc[(part3["paysage_pp"] != "") & (part3["fullName"].notna()), "id_paysage"] = part3.loc[
        (part3["paysage_pp"] != "") & (part3["fullName"].notna()), "paysage_pp"]

    part3 = part3.drop(columns="paysage_pp")

    part3.to_csv('part_p08.csv', sep='|', index=False, encoding="utf-8")

    swift.upload_object('patstat', 'part_p08.csv')

    # création de la table participants

    participants = part3[
        ['id_patent', 'key_appln_nr', 'key_appln_nr_person', 'id_personne', 'type', 'sexe', 'doc_std_name',
         "doc_std_name_id",
         'name_source', 'name_corrected',
         'address_source',
         'country_source', 'country_corrected']]

    participants.to_csv('participants.csv', sep='|', index=False, encoding="utf-8")

    # création de la table idext

    part_idext = part3[['key_appln_nr_person', 'siren', 'siret', 'id_paysage', 'rnsr', 'grid']]

    idext1 = pd.melt(frame=part_idext, id_vars='key_appln_nr_person', var_name='id_type', value_name='id_value')

    idext = idext1[idext1['id_value'] != '']

    idext.to_csv('idext.csv', sep='|', index=False)

    # création de la table role

    part_role = part_init[part_init['isascii']][['key_appln_nr_person', 'applt_seq_nr', 'invt_seq_nr']].rename(
        columns={'applt_seq_nr': 'dep', 'invt_seq_nr': 'inv'})

    role1 = pd.melt(frame=part_role, id_vars='key_appln_nr_person', var_name='role')

    role = role1[role1['value'] > 0].drop(columns={'value'})

    role.to_csv('role.csv', sep='|', index=False)
    swift.upload_object('patstat', 'role.csv')
