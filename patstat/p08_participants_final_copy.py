#!/usr/bin/env python
# coding: utf-8

import os
import re
from utils import swift

import numpy as np
import pandas as pd

from patstat import dtypes_patstat_declaration as types

import requests
from retry import retry
import concurrent.futures
import logging
import threading
import sys
import timeit
from datetime import date

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


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


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    longueur = len(df)
    if longueur >= 10:
        prct10 = int(round(longueur * 10 / 100, 0))
    else:
        prct10 = 1
    dict_nb = {}
    indices = list(df.index)
    listes_indices = [indices[i:i + prct10] for i in range(0, len(indices), prct10)]
    i = 1
    for liste in listes_indices:
        min_ind = np.min(liste)
        # if np.max(liste)
        max_ind = np.max(liste) + 1
        dict_nb["df" + str(i)] = df.iloc[min_ind: max_ind, :]
        i = i + 1

    return dict_nb


@retry(tries=10, delay=5, backoff=5)
def req_scanr_person(df_pers_fuz: pd.DataFrame) -> pd.DataFrame:
    url_persons = "https://scanr-api.enseignementsup-recherche.gouv.fr/elasticsearch/persons/_search"
    headers = os.getenv("AUTHORIZATION_SCANR_ES")

    dict_res = {"person_id": [], "name_source": [], "externalIds": [], "fullName": []}

    logger = get_logger(threading.current_thread().name)
    logger.info("start")

    for fam in list(df_pers_fuz["person_id"].unique()):
        df = df_pers_fuz.loc[df_pers_fuz["person_id"] == fam]
        pm = list(df["siren"].unique())
        pers = list(df["name_source"].unique())[0]
        query2 = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "fullName": {
                                    "query": pers,
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

        res2 = requests.get(url_persons, headers=headers, json=query2).json()

        if res2.get("status"):
            pass
        elif len(res2.get("hits").get("hits")) > 0:
            print(res2.get("hits").get("hits"))
            dict_res["person_id"].append(fam)
            dict_res["name_source"].append(pers)
            dict_res["externalIds"].append(
                res2.get("hits").get("hits")[0].get("_source").get("externalIds"))
            dict_res["fullName"].append(
                res2.get("hits").get("hits")[0].get("_source").get("fullName"))

    df2 = pd.DataFrame(data=dict_res)

    logger.info("end")

    return df2


def idref(prt):
    df_entp = prt.loc[(prt["siren"].notna()) & (prt["type"] == "pm")]
    df_pp = prt.loc[prt["type"] == "pp"]
    part2 = pd.concat([df_entp, df_pp])
    part3 = part2[["docdb_family_id", "type", "name_source",
                   "person_id", "siren"]].drop_duplicates()

    docdb = list(set(prt["docdb_family_id"]))

    df_docdb = pd.DataFrame(data={"docdb_family_id": docdb})

    pp = list(set(df_pp.loc[df_pp["idref"].isna(), "person_id"]))
    df_pp2 = pd.DataFrame(data={"person_id": pp})

    df_pp2 = pd.merge(df_pp2, part3[["person_id", "name_source", "docdb_family_id"]], on="person_id", how="left")

    df_docdb2 = pd.merge(df_docdb, part3[["person_id", "docdb_family_id", "type", "siren"]], on="docdb_family_id",
                         how="left")
    df_docdb2["person_id"] = df_docdb2["person_id"].astype(pd.Int64Dtype())
    df_docdb2 = df_docdb2.rename(columns={"person_id": "personnes"})
    df_docdb2 = df_docdb2.loc[df_docdb2["type"] == "pm"]

    df3 = pd.merge(df_pp2, df_docdb2, on="docdb_family_id", how="right")
    df3 = df3.drop(columns="docdb_family_id").drop_duplicates()
    df3["personnes"] = df3["personnes"].astype(pd.Int64Dtype())

    df4 = df3.drop(columns=["type", "personnes"]).drop_duplicates()

    df4 = df4.sort_values("person_id").reset_index().drop(columns="index")
    df4["person_id"] = df4["person_id"].astype(pd.Int64Dtype())
    df4 = df4.loc[df4["person_id"].notna()]

    liste_res = []
    liste_etab = ["mCpLW", "Eg7tX", "2ZdzP", "MTFHZ", "Sv5bb", "J0mQ2", "Dgbe9", "1uQLb", "hg3v5"]

    for etab in liste_etab:
        res = requests.get(
            f"https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-categorie"
            f"&filters[relatedObjectId]={etab}&limit=2000",
            headers={"Content-Type": "application/json", "X-API-KEY": os.getenv("PAYSAGE_API_KEY")}).json()
        res_data = res.get("data")
        if len(res_data) > 0:
            for item in res_data:
                res_resource = item.get("resource")
                res_resource["category2"] = etab
                start = item.get("startDate")
                end = item.get("endDate")
                res_resource["startDate"] = start
                res_resource["endDate"] = end
                liste_res.append(res_resource)

    liste_res = [item for item in liste_res if len(item) > 0]

    dict_etab = {"Paysage_id": [], "displayName": [], "category": [], "startDate": [], "endDate": []}
    for item in liste_res:
        dict_etab["Paysage_id"].append(item.get("id"))
        dict_etab["displayName"].append(item.get("displayName"))
        dict_etab["startDate"].append(item.get("startDate"))
        dict_etab["endDate"].append(item.get("endDate"))
        cat = item.get("category2")
        dict_etab["category"].append(cat)

    df = pd.DataFrame(data=dict_etab)
    df = df.drop_duplicates()

    df[["startDate", "endDate"]] = df[["startDate", "endDate"]].apply(pd.to_datetime)
    df["startDate"] = df["startDate"].dt.date
    df["endDate"] = df["endDate"].dt.date
    today = date.today()
    df.loc[df["startDate"] > today, "statut"] = "futur"
    df.loc[(df["startDate"] <= today) & (df["endDate"].isna()), "statut"] = "actuel"
    df.loc[(df["startDate"] <= today) & (df["endDate"] > today), "statut"] = "actuel"
    df.loc[(df["startDate"] <= today) & (df["endDate"] <= today), "statut"] = "fermé"
    df.loc[(df["startDate"].isna()) & (df["endDate"] <= today), "statut"] = "fermé"

    df = df.drop(columns=["startDate", "endDate"])

    libelle = pd.DataFrame(
        data={"category": ["mCpLW", "Eg7tX", "2ZdzP", "MTFHZ", "Sv5bb", "J0mQ2", "Dgbe9", "1uQLb", "hg3v5"],
              "category_libelle": ["Université", "Établissement public expérimental",
                                   "Organisme de recherche",
                                   "Société d'accélération du transfert de technologies",
                                   "Tutelle des établissements", "Grand établissement",
                                   "Grand établissement relevant d'un autre département ministériel",
                                   "Centre hospitalier", "École d'ingénieurs"]})

    df = pd.merge(df, libelle, on="category", how="left")

    liste_res_id = []
    liste = []

    for _, r in df.iterrows():
        res = requests.get(
            f"https://api.paysage.dataesr.ovh/structures/{r.Paysage_id}/identifiers",
            headers={"Content-Type": "application/json", "X-API-KEY": os.getenv("PAYSAGE_API_KEY")}).json()
        res_data = res.get("data")
        if len(res_data) > 0:
            liste_res_id.append(res_data)
            for item in res_data:
                tmp = pd.json_normalize(item)
                tmp["Paysage_id"] = r.Paysage_id
                tmp["displayName"] = r.displayName
                liste.append(tmp)
        else:
            tmp = pd.DataFrame(data={"Paysage_id": [r.Paysage_id], "displayName": [r.displayName]})
            liste.append(tmp)

    df2 = pd.concat(liste)
    df2 = df2.loc[df2["type"] == "siret"]
    df2 = df2.rename(columns={"value": "siret"})

    df2["siren"] = df2["siret"].apply(lambda a: a[0:9])
    df3 = df2.loc[df2["type"] == "siret"]
    df3 = df3.rename(columns={"value": "siret"})

    res = requests.post("https://api.insee.fr/token",
                        data="grant_type=client_credentials",
                        auth=(os.getenv("SIRENE_API_KEY"), os.getenv("SIRENE_API_SECRET")))

    liste_etab = []

    token = res.json()['access_token']
    api_call_headers = {'Authorization': 'Bearer ' + token}

    for _, r in df3.iterrows():
        url = f'https://api.insee.fr/entreprises/sirene/V3/siret?q=siret:{r.siret}'
        rinit = requests.get(url, headers=api_call_headers, verify=False)
        if rinit.status_code == 200:
            rep = pd.json_normalize(rinit.json()["etablissements"])
            rep = rep[["siret", "etablissementSiege"]].drop_duplicates()
            liste_etab.append(rep)

    etab = pd.concat(liste_etab)

    df3["siren"] = df3["siret"].apply(lambda a: a[0:9])
    df3 = pd.merge(df3, etab, on="siret", how="left")
    df3 = pd.merge(df3, df, on=["Paysage_id", "displayName"], how="left")
    df3 = df3[
        ["Paysage_id", "displayName", "siren", "category_libelle", "etablissementSiege", "statut"]].drop_duplicates()
    col_df3 = list(df3.columns)

    df3_compte = df3[["Paysage_id", "siren"]].groupby("siren").nunique().reset_index().rename(
        columns={"Paysage_id": "id_per_siren"})
    df3 = pd.merge(df3, df3_compte, on="siren", how="left")

    prt["earliest_filing_date"] = prt["earliest_filing_date"].apply(pd.to_datetime)
    prt["year"] = prt["earliest_filing_date"].apply(lambda a: a.year)
    prt = prt.loc[prt["year"] >= 2010]

    url = 'https://docs.google.com/spreadsheet/ccc?key=***REMOVED***&output=xls'
    CORRECTIFS_dict = {}
    VARS = ['C_ETABLISSEMENTS']
    df_c = pd.read_excel(url, sheet_name=VARS, dtype=str, na_filter=False)
    c_etab = df_c["C_ETABLISSEMENTS"]
    col_c_etab = list(c_etab.columns)
    col_idpaysage = [item for item in col_c_etab if "id_paysage_" in item]
    col_paysage = ["id_paysage", "date_de_creation", "date_de_fermeture"]
    for item in col_idpaysage:
        if item == "id_paysage_actuel":
            col_paysage.append(item)
        else:
            item2 = item.split("_")
            if int(item2[2]):
                an = int(item2[2])
                if an >= 2010:
                    col_paysage.append(item)

    c_etab2 = c_etab[col_paysage]
    c_etab2 = c_etab2.rename(columns={"id_paysage": "Paysage_id"})

    df3 = pd.merge(df3, c_etab2, on="Paysage_id", how="left")

    sel_siren = set(prt["siren"]).intersection(set(df3["siren"]))
    df3_com = df3.loc[df3["siren"].isin(sel_siren)]

    df3_unique = df3_com.loc[df3_com["id_per_siren"] == 1]
    liste_df3 = []
    for siren in set(df3_unique["siren"]):
        tp = df3_unique.loc[df3_unique["siren"] == siren]
        if len(tp) > 1:
            if tp["etablissementSiege"].nunique() > 1:
                tp = tp.loc[tp["etablissementSiege"] == True]
                liste_df3.append(tp)
            elif tp["statut"].nunique(dropna=False) > 1:
                tp = tp.loc[tp["statut"] == "actuel"]
                liste_df3.append(tp)
            else:
                liste_df3.append(tp)
        else:
            liste_df3.append(tp)

    df32 = pd.concat(liste_df3)
    df3_multi = df3_com.loc[df3_com["id_per_siren"] > 1]

    df3_multi3 = df3_multi.copy()
    df3_multi3["test"] = df3_multi3.apply(lambda a: "oui" if a.id_paysage_actuel == a.Paysage_id else "non", axis=1)

    grouped = df3_multi3.groupby("siren")

    liste_group = []

    for _, group in grouped:
        if set(group['test']) == {"oui", "non"}:
            if len(group.loc[group["test"] == "oui"]) == 1:
                temp = group.loc[group["test"] == "oui"]
                liste_group.append(temp)
            else:
                if len(group.loc[group["etablissementSiege"] == True]) == 1:
                    temp = group.loc[group["etablissementSiege"] == True]
                    liste_group.append(temp)
                else:
                    if len(group.loc[group["statut"].notna()] == 1):
                        temp = group.loc[group["statut"].notna()]
                        liste_group.append(temp)
                    else:
                        liste_group.append(group)
        elif set(group['test']) == {"oui"}:
            if len(group.loc[group["etablissementSiege"] == True]) > 0:
                temp = group.loc[group["etablissementSiege"] == True]
                liste_group.append(temp)
            else:
                liste_group.append(group)
        elif set(group['test']) == {"non"}:
            if len(group.loc[group["etablissementSiege"] == True]) == 1:
                temp = group.loc[group["etablissementSiege"] == True]
                liste_group.append(temp)
            elif len(group.loc[group["etablissementSiege"] == True]) > 1:
                if len(group.loc[(group["etablissementSiege"] == True) & (group["statut"].notna())] == 1):
                    temp = group.loc[(group["etablissementSiege"] == True) & (group["statut"].notna())]
                    liste_group.append(temp)
                elif len(group.loc[(group["etablissementSiege"] == True) & (group["statut"].notna())] > 1):
                    if "actuel" in list(set(group.loc[group["etablissementSiege"] == True, "statut"])):
                        temp = group.loc[(group["etablissementSiege"] == True) & (group["statut"] == "actuel")]
                        liste_group.append(temp)
                    else:
                        liste_group.append(group)
            # else:
            #     liste_group.append(group)
        else:
            liste_group.append(group)

    groupes = pd.concat(liste_group)
    groupes = groupes[list(df32.columns)]

    paysage = pd.concat([df32, groupes])
    paysage = paysage.drop(columns=["etablissementSiege", "statut"])

    paysage2 = paysage.copy()
    paysage2 = paysage2.rename(columns={"id_paysage_actuel": "id_paysage_2024"})

    paysage3 = pd.wide_to_long(paysage2, "id_paysage_", i=['Paysage_id', 'displayName',
                                                           'siren', 'category_libelle', 'id_per_siren',
                                                           'date_de_creation', 'date_de_fermeture'],
                               j="year").reset_index()
    paysage3.loc[paysage3["id_paysage_"] == "", "id_paysage_"] = paysage3.loc[
        paysage3["id_paysage_"] == "", "Paysage_id"]
    paysage3.loc[paysage3["id_paysage_"].isna(), "id_paysage_"] = paysage3.loc[
        paysage3["id_paysage_"].isna(), "Paysage_id"]
    paysage3 = paysage3.drop(columns="Paysage_id").rename(columns={"id_paysage_": "Paysage_id"})

    liste_pay_id = []
    liste_pay = []

    for pay_id in list(paysage3["Paysage_id"].unique()):
        res = requests.get(
            f"https://api.paysage.dataesr.ovh/structures/{pay_id}",
            headers={"Content-Type": "application/json", "X-API-KEY": os.getenv("PAYSAGE_API_KEY")}).json()
        res_data = res.get("currentName")
        if len(res_data) > 0:
            liste_pay_id.append(res_data)
            tmp = pd.DataFrame(data=[res_data])
            tmp["Paysage_id"] = pay_id
            liste_pay.append(tmp)
        else:
            tmp = pd.DataFrame(data={"Paysage_id": [pay_id]})
            liste_pay.append(tmp)

    df_pay = pd.concat(liste_pay)
    df_pay2 = df_pay[["usualName", "Paysage_id"]].drop_duplicates()
    paysage3 = pd.merge(paysage3, df_pay2, on="Paysage_id", how="left")

    prt2 = pd.merge(prt, paysage3, on=["siren", "year"], how="left")
    prt2.loc[(prt2["Paysage_id"].notna()) & (prt2["id_paysage"].isna()), "id_paysage"] = prt2.loc[
        (prt2["Paysage_id"].notna()) & (prt2["id_paysage"].isna()), "Paysage_id"]
    prt2 = prt2.drop(columns=["Paysage_id", "displayName", "usualName", "id_per_siren", "date_de_creation",
                              "date_de_fermeture", "year"])

    df5 = subset_df(pd.DataFrame(data={"person_id": df4["person_id"].unique()}))

    dict_fam = {}

    for key in list(df5.keys()):
        dict_fam[key] = df4.loc[df4["person_id"].isin(df5[key]["person_id"])]

    start = timeit.default_timer()
    print(start)
    df_req_pers = res_futures(dict_fam, req_scanr_person)
    stop = timeit.default_timer()
    execution_time = stop - start

    print("Program Executed in " + str(execution_time))

    df_req_pers["person_id"] = df_req_pers["person_id"].astype(pd.Int64Dtype())

    type_ids = list(df_req_pers["externalIds"])
    list_df = []
    for item in type_ids:
        tmp = pd.json_normalize(item)
        list_df.append(tmp)

    dict_ids_pp = {"person_id": [], "name_source": [], "idref": [], "id_hal": [], "orcid": [], "fullName": []}

    for _, r in df_req_pers.iterrows():
        ids = r.externalIds
        if ids is not None:
            dict_ids_pp["person_id"].append(r.person_id)
            dict_ids_pp["name_source"].append(r.name_source)
            dict_ids_pp["fullName"].append(r.fullName)
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

            keys = [k for k in list(lab_id2.keys()) if k in ["idref"]]
            not_keys = list({"idref"} - set(keys))

            for k in [id.get("type") for id in ids]:
                if k not in keys:
                    for item in ids:
                        if item["type"] == k:
                            ids.remove(item)

            for item in ids:
                typ = item.get("type")
                id = item.get("id")
                if typ == "idref":
                    id = "idref" + str(id)
                dict_ids_pp[typ].append(id)

            for typ in not_keys:
                dict_ids_pp[typ].append("")

    df_ids_pp = pd.DataFrame(data=dict_ids_pp)
    df_ids_pp = df_ids_pp.rename(columns={"idref": "idref_pp"})
    df_ids_pp = df_ids_pp.drop_duplicates()
    df_ids_pp_compte = df_ids_pp[["person_id", "idref_pp"]].groupby("person_id").nunique().reset_index()
    df_ids_pp_id = set(df_ids_pp_compte.loc[df_ids_pp_compte["idref_pp"] > 1, "person_id"])
    df_ids_pp2 = df_ids_pp.loc[~df_ids_pp["person_id"].isin(df_ids_pp_id)]

    jntre = pd.merge(prt2, df_ids_pp2, on=["person_id", "name_source"], how="left")
    jntre["person_id"] = jntre["person_id"].astype(pd.Int64Dtype())
    jntre = jntre.drop_duplicates()

    pp_comp = jntre.loc[(jntre["type"] == "pp") & (jntre["fullName"].notna())]
    pp_comp.loc[pp_comp["idref"].isna(), "comp"] = "manquant"
    pp_comp.loc[(pp_comp["idref"].notna()) & (pp_comp["idref"] == pp_comp["idref_pp"]), "comp"] = "identique"
    pp_comp.loc[(pp_comp["idref"].notna()) & (pp_comp["idref"] != pp_comp["idref_pp"]), "comp"] = "différent"
    pp_comp2 = pp_comp[["person_id", "name_source", "idref", "idref_pp", "fullName", "comp"]].drop_duplicates()

    pp_comp3 = pp_comp2[["person_id", "comp"]].drop_duplicates()

    compte_df = pd.crosstab(pp_comp3['person_id'], pp_comp3['comp'].fillna(0)).reset_index()
    compte_df["somme"] = compte_df["différent"] + compte_df["identique"] + compte_df["manquant"]
    unique = compte_df.loc[compte_df["somme"] == 1]
    u_di = list(unique.loc[unique["différent"] == 1, "person_id"].unique())
    u_id = list(unique.loc[unique["identique"] == 1, "person_id"].unique())
    u_man = list(unique.loc[unique["manquant"] == 1, "person_id"].unique())
    multi = compte_df.loc[compte_df["somme"] > 1]
    m_tous = list(multi.loc[multi["somme"] == 3, "person_id"].unique())
    m_di = list(
        multi.loc[(multi["somme"] == 2) & (multi["différent"] == 1) & (multi["identique"] == 1), "person_id"].unique())
    m_dm = list(
        multi.loc[(multi["somme"] == 2) & (multi["différent"] == 1) & (multi["manquant"] == 1), "person_id"].unique())
    m_im = list(
        multi.loc[(multi["somme"] == 2) & (multi["identique"] == 1) & (multi["manquant"] == 1), "person_id"].unique())

    pp_comp3 = pp_comp2.copy()
    pp_udi = pp_comp3.loc[pp_comp3["person_id"].isin(u_di), ["person_id", "idref"]].rename(
        columns={"idref": "idref_correct"})
    pp_uid = pp_comp3.loc[pp_comp3["person_id"].isin(u_id), ["person_id", "idref"]].rename(
        columns={"idref": "idref_correct"})
    pp_man = pp_comp3.loc[pp_comp3["person_id"].isin(u_man), ["person_id", "idref_pp"]].rename(
        columns={"idref_pp": "idref_correct"})
    pp_mtous = pp_comp3.loc[
        (pp_comp3["person_id"].isin(m_tous)) & (pp_comp3["comp"] == "identique"), ["person_id", "idref"]].rename(
        columns={"idref": "idref_correct"})
    pp_mdi = pp_comp3.loc[
        (pp_comp3["person_id"].isin(m_di)) & (pp_comp3["comp"] == "identique"), ["person_id", "idref"]].rename(
        columns={"idref": "idref_correct"})
    pp_mdm = pp_comp3.loc[
        (pp_comp3["person_id"].isin(m_dm)) & (pp_comp3["comp"] == "différent"), ["person_id", "idref"]].rename(
        columns={"idref": "idref_correct"})
    pp_mim = pp_comp3.loc[
        (pp_comp3["person_id"].isin(m_im)) & (pp_comp3["comp"] == "identique"), ["person_id", "idref"]].rename(
        columns={"idref": "idref_correct"})
    pp_correct = pd.concat([pp_udi, pp_uid, pp_man, pp_mtous, pp_mdi, pp_mdm, pp_mim])
    set_ids = set(pp_correct["person_id"])
    set_autres_ids = set(u_di)
    set_autres_ids.update(m_dm)
    jointure2 = pd.merge(jntre, pp_correct, on="person_id", how="left")
    jointure2.loc[jointure2["person_id"].isin(set_ids), "idref"] = jointure2.loc[
        jointure2["person_id"].isin(set_ids), "idref_correct"]

    return jointure2


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

    particip2 = idref(particip)

    particip2.to_csv('part_p08_test.csv', sep='|', index=False, encoding="utf-8")
    swift.upload_object('patstat', 'part_p08_test.csv')

    # # création de la table participants
    #
    # participants = particip2[
    #     ['key_appln_nr', 'key_appln_nr_person', 'id_personne', 'type', 'sexe', 'doc_std_name',
    #      "doc_std_name_id",
    #      'name_source', 'name_corrected',
    #      'address_source',
    #      'country_source', 'country_corrected']]
    #
    # participants.to_csv('participants.csv', sep='|', index=False, encoding="utf-8")
    # swift.upload_object('patstat', 'participants.csv')
    #
    # # création de la table idext
    #
    # part_idext = particip2[['key_appln_nr_person', 'siren', 'siret', 'id_paysage', 'rnsr', 'grid']]
    #
    # idext1 = pd.melt(frame=part_idext, id_vars='key_appln_nr_person', var_name='id_type', value_name='id_value')
    #
    # idext = idext1[idext1['id_value'] != '']
    #
    # idext.to_csv('idext.csv', sep='|', index=False)
    # swift.upload_object('patstat', 'idext.csv')
    #
    # # création de la table role
    #
    # part_role = part_init[part_init['isascii']][['key_appln_nr_person', 'applt_seq_nr', 'invt_seq_nr']].rename(
    #     columns={'applt_seq_nr': 'dep', 'invt_seq_nr': 'inv'})
    #
    # role1 = pd.melt(frame=part_role, id_vars='key_appln_nr_person', var_name='role')
    #
    # role = role1[role1['value'] > 0].drop(columns={'value'})
    #
    # role.to_csv('role.csv', sep='|', index=False)
    # swift.upload_object('patstat', 'role.csv')
