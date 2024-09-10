#!/usr/bin/env python
# coding: utf-8
import numpy as np
import pandas as pd
from patstat import dtypes_patstat_declaration as types
import os
import re
import requests
import json
import xmltodict
import Levenshtein as lev
import time
from unidecode import unidecode
# import boto3
import logging
import threading
import sys
import concurrent.futures
from retry import retry

from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
URL = 'https://bodacc-datadila.opendatasoft.com/api/records/1.0/search/?dataset=annonces-commerciales&'


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
    global jointure
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
    prct10 = int(round(len(df) * 10 / 100, 0))
    dict_nb = {}
    if prct10 > 0:
        df = df.reset_index().drop(columns="index")
        indices = list(df.index)
        listes_indices = [indices[i:i + prct10] for i in range(0, len(indices), prct10)]
        i = 1
        for liste in listes_indices:
            min_ind = np.min(liste)
            max_ind = np.max(liste) + 1
            dict_nb["df" + str(i)] = df.iloc[min_ind: max_ind, :]
            i = i + 1
    else:
        dict_nb["df1"] = df

    return dict_nb

def get_url(ul: str):
    """
    fonction pour faire les requêtes GET sur l'API Patstat
    function that produces a GET request and check if it's successful
    url: string
    tkn: token, get it with authentication function
    strm: boolean, stream data from the API or not
    """
    res = requests.get(ul)
    status = res.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed", flush=True)
    return res


# def xml_json(rt):
#     """
#     Get parties from XML result
#     """
#     data = xmltodict.parse(rt.content)
#     djson = json.dumps(data)
#     djson2 = json.loads(djson)
#     djson2 = djson2["ops:world-patent-data"]["ops:register-search"]["reg:register-documents"]["reg:register-document"][
#         "reg:bibliographic-data"]["reg:parties"]
#     return djson2
#
#
# def r_dict(xjson):
#     """
#     Get applicants from XML results
#     """
#     jsn = xml_json(xjson)
#     res_part1 = jsn["reg:applicants"]
#
#     return res_part1


def df_applicant(res_part1) -> pd.DataFrame:
    """
    Create dataframe with applicants
    """
    global df1
    global nom
    if isinstance(res_part1, dict):
        if isinstance(res_part1["reg:applicant"], list):
            df1 = pd.json_normalize([res_part1], record_path=["reg:applicant"],
                                    meta=["@change-date", "@change-gazette-num"])

            for col in list(df1.columns):
                nom = re.sub("reg:applicant\.", "", col)
                nom = re.sub("\.\$", "", nom)
                df1 = df1.rename(columns={col: nom})

            df1 = df1.drop(
                columns=["@app-type", "@designation", "reg:nationality.reg:country", "reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            df1["address"] = df1[col_address[0]]
            for i in range(1, len(col_address)):
                df1["address"] = df1["address"] + " " + df1[col_address[i]]
                df1["address"] = df1["address"].apply(lambda a: a.strip())

            for col in col_address:
                name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                df1 = df1.rename(columns={col: name})

            df1 = df1.rename(columns={"@sequence": "sequence",
                                      "reg:addressbook.@cdsid": "cdsid",
                                      "reg:addressbook.reg:name": "name",
                                      "reg:addressbook.reg:address.reg:country": "country"})

        else:
            df1 = pd.json_normalize([res_part1])

            for col in list(df1.columns):
                nom = re.sub("reg:applicant\.", "", col)
                nom = re.sub("\.\$", "", nom)
                df1 = df1.rename(columns={col: nom})

            df1 = df1.drop(columns=["@app-type",
                                    "@designation",
                                    "reg:nationality.reg:country",
                                    "reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            df1["address"] = df1[col_address[0]]
            for i in range(1, len(col_address)):
                df1["address"] = df1["address"] + " " + df1[col_address[i]]
                df1["address"] = df1["address"].apply(lambda a: a.strip())

            for col in col_address:
                name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                df1 = df1.rename(columns={col: name})

            df1 = df1.rename(columns={"@sequence": "sequence",
                                      "reg:addressbook.@cdsid": "cdsid",
                                      "reg:addressbook.reg:name": "name",
                                      "reg:addressbook.reg:address.reg:country": "country"})
    else:
        set_liste = [isinstance(item["reg:applicant"], list) for item in res_part1]
        set_liste = set(set_liste)
        if len(set_liste) == 1 and True in set_liste:
            df1 = pd.json_normalize(res_part1, record_path=["reg:applicant"],
                                    meta=["@change-date", "@change-gazette-num"])

            for col in list(df1.columns):
                nom = re.sub("reg:applicant\.", "", col)
                nom = re.sub("\.\$", "", nom)
                df1 = df1.rename(columns={col: nom})

            df1 = df1.drop(
                columns=["@app-type", "@designation", "reg:nationality.reg:country", "reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            df1["address"] = df1[col_address[0]]
            for i in range(1, len(col_address)):
                df1["address"] = df1["address"] + " " + df1[col_address[i]]
                df1["address"] = df1["address"].apply(lambda a: a.strip())

            for col in col_address:
                name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                df1 = df1.rename(columns={col: name})

            df1 = df1.rename(columns={"@sequence": "sequence",
                                      "reg:addressbook.@cdsid": "cdsid",
                                      "reg:addressbook.reg:name": "name",
                                      "reg:addressbook.reg:address.reg:country": "country"})

        elif len(set_liste) > 1 and True in set_liste:
            liste_part = []
            for item in res_part1:
                if isinstance(item["reg:applicant"], list):
                    tmp = pd.json_normalize(item, record_path=["reg:applicant"],
                                            meta=["@change-date", "@change-gazette-num"])

                    for col in list(tmp.columns):
                        nom = re.sub("reg:applicant\.", "", col)
                        nom = re.sub("\.\$", "", nom)
                        tmp = tmp.rename(columns={col: nom})

                    tmp = tmp.drop(
                        columns=["@app-type", "@designation", "reg:nationality.reg:country",
                                 "reg:residence.reg:country"])

                    tmp = tmp.fillna("")

                    col_address = []

                    for col in list(tmp.columns):
                        if col.startswith("reg:addressbook.reg:address.reg:address-"):
                            col_address.append(col)

                    tmp["address"] = tmp[col_address[0]]
                    for i in range(1, len(col_address)):
                        tmp["address"] = tmp["address"] + " " + tmp[col_address[i]]
                        tmp["address"] = tmp["address"].apply(lambda a: a.strip())

                    tmp = tmp.drop(
                        columns=col_address)

                    tmp = tmp.rename(columns={"@sequence": "sequence",
                                              "reg:addressbook.@cdsid": "cdsid",
                                              "reg:addressbook.reg:name": "name",
                                              "reg:addressbook.reg:address.reg:country": "country"})
                    liste_part.append(tmp)
                else:
                    tmp = pd.json_normalize(item)

                    for col in list(tmp.columns):
                        nom = re.sub("reg:applicant\.", "", col)
                        nom = re.sub("\.\$", "", nom)
                        tmp = tmp.rename(columns={col: nom})

                    tmp = tmp.drop(columns=["@app-type",
                                            "@designation",
                                            "reg:nationality.reg:country",
                                            "reg:residence.reg:country"])

                    tmp = tmp.fillna("")

                    col_address = []

                    for col in list(tmp.columns):
                        if col.startswith("reg:addressbook.reg:address.reg:address-"):
                            col_address.append(col)

                    tmp["address"] = tmp[col_address[0]]
                    for i in range(1, len(col_address)):
                        tmp["address"] = tmp["address"] + " " + tmp[col_address[i]]
                        tmp["address"] = tmp["address"].apply(lambda a: a.strip())

                    for col in col_address:
                        name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                        df1 = df1.rename(columns={col: name})

                    # tmp = tmp.drop(
                    #     columns=col_address)

                    tmp = tmp.rename(columns={"@sequence": "sequence",
                                              "reg:addressbook.@cdsid": "cdsid",
                                              "reg:addressbook.reg:name": "name",
                                              "reg:addressbook.reg:address.reg:country": "country"})
                    liste_part.append(tmp)

            df1 = pd.concat(liste_part)

        else:
            df1 = pd.json_normalize(res_part1)

            for col in list(df1.columns):
                nom = re.sub("reg:applicant\.", "", col)
                nom = re.sub("\.\$", "", nom)
                df1 = df1.rename(columns={col: nom})

            df1 = df1.drop(columns=["@app-type",
                                    "@designation",
                                    "reg:nationality.reg:country",
                                    "reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            df1["address"] = df1[col_address[0]]
            for i in range(1, len(col_address)):
                df1["address"] = df1["address"] + " " + df1[col_address[i]]
                df1["address"] = df1["address"].apply(lambda a: a.strip())

            for col in col_address:
                name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                df1 = df1.rename(columns={col: name})

            df1 = df1.rename(columns={"@sequence": "sequence",
                                      "reg:addressbook.@cdsid": "cdsid",
                                      "reg:addressbook.reg:name": "name",
                                      "reg:addressbook.reg:address.reg:country": "country"})

    return df1

@retry(tries=3, delay=5, backoff=5)
def get_token_oeb():
    """
    Get token OEB
    """
    token_url = 'https://ops.epo.org/3.2/auth/accesstoken'
    key = os.getenv("KEY_OPS_OEB")
    data = {'grant_type': 'client_credentials'}
    headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded'}

    r = requests.post(token_url, data=data, headers=headers)

    rs = r.content.decode()
    response = json.loads(rs)

    tken = f"Bearer {response.get('access_token')}"

    return tken

@retry(tries=3, delay=5, backoff=5)
def get_part(pn: str, tkn: str) -> pd.DataFrame:
    """
    Get applicants from EPO query with publication number
    """
    global d_app
    ret1 = requests.get(f"http://ops.epo.org/3.2/rest-services/register/search/biblio?q=pn%3D{pn}",
                        headers={"Authorization": tkn, 'Accept': 'application/json'})

    status = ret1.status_code
    if status != 200:
        if ret1.text == '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' \
                        '<fault xmlns="http://ops.epo.org">\n    <code>SERVER.EntityNotFound</code>\n' \
                        '    <message>No results found</message>\n</fault>\n':
            pass
        else:
            raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed", flush=True)
        res_part1 = \
            ret1.json()["ops:world-patent-data"]["ops:register-search"]["reg:register-documents"][
                "reg:register-document"][
                "reg:bibliographic-data"]
        if "reg:parties" in res_part1.keys():
            res_part1 = res_part1["reg:parties"]
            if "reg:applicants" in res_part1.keys():
                res_part1 = res_part1["reg:applicants"]
                d_app = df_applicant(res_part1)
        else:
            pass

    return d_app


def split_hs(a: str) -> list:
    """
    Keep only digits from string
    """
    global a2
    a = str(a)
    if a.isdigit():
        a2 = [a]
    else:
        li = [c for c in a if not c.isdigit()]
        if li:
            sep = "["
            for item in li:
                sep = sep + item
            sep = sep + "]"

            a2 = re.split(sep, a)
            a2 = [a for a in a2 if a]
            a2 = [a for a in a2 if a != "-"]
            a2 = [int(a) for a in a2]
            a2 = list(dict.fromkeys(a2))
            a2.sort()
        else:
            a2 = np.NaN
    return a2

@retry(tries=3, delay=5, backoff=5)
def address_api(t_ad: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    Clean addresses with api-adresse.data.gouv.fr
    Keep only cases with housenumber results
    """
    type_insee = {"Allée": "ALL",
                  "Avenue": "AV",
                  "Boulevard": "BD",
                  "Carrefour": "CAR",
                  "Chemin": "CHE",
                  "Chaussée": "CHS",
                  "Cité": "CITE",
                  "Corniche": "COR",
                  "Cours": "CRS",
                  "Domaine": "DOM",
                  "Descente": "DSC",
                  "Ecart": "ECA",
                  "Esplanade": "ESP",
                  "Faubourg": "FG",
                  "Grande Rue": "GR",
                  "Hameau": "HAM",
                  "Halle": "HLE",
                  "Impasse": "IMP",
                  "Lieu-dit": "LD",
                  "Lotissement": "LOT",
                  "Marché": "MAR",
                  "Montée": "MTE",
                  "Passage": "PAS",
                  "Place": "PL",
                  "Plaine": "PLN",
                  "Plateau": "PLT",
                  "Promenade": "PRO",
                  "Parvis": "PRV",
                  "Quartier": "QUA",
                  "Quai": "QUAI",
                  "Résidence": "RES",
                  "Ruelle": "RLE",
                  "Rocade": "ROC",
                  "Rond-point": "RPT",
                  "Route": "RTE",
                  "Rue": "RUE",
                  "Sente": "SEN",
                  "Sentier": "SEN",
                  "Square": "SQ",
                  "Terre-plein": "TPL",
                  "Traverse": "TRA",
                  "Villa": "VLA",
                  "Village": "VLGE"}

    liste_address = []

    for adresse in set(t_ad[col]):
        try:
            res = get_url(f"https://api-adresse.data.gouv.fr/search/?q={adresse}&type=housenumber")
            r = json.loads(res.content)
            if r["features"]:
                df = pd.json_normalize(r, record_path=["features"])
                df = df.loc[df["properties.type"] == "housenumber"]
                if len(df) > 0:
                    smax = df["properties.score"].max()
                    df = df.loc[df["properties.score"] == smax]
                    df["address"] = r["query"]

                    renomme = {}
                    liste_cols = ["properties.label", "properties.housenumber", "properties.name",
                                  "properties.postcode",
                                  "properties.citycode", "properties.city", "properties.street", "address"]
                    for col in list(df.columns):
                        if col in liste_cols:
                            if col not in ["properties.label", "properties.name"]:
                                nom = re.sub("properties.", "", col)
                                renomme[col] = nom
                            elif col == "properties.label":
                                renomme[col] = "label_address"
                            else:
                                renomme[col] = "name_address"

                    agarder = list(renomme.keys())
                    df = df[agarder]
                    df = df.rename(columns=renomme)

                    df.loc[df["street"].isna(), "street"] = df.loc[df["street"].isna(), "name_address"]

                    df["type_lib"] = df["street"].apply(lambda a: a.split(" ")[0])
                    df["libelle"] = df["street"].apply(lambda a: re.sub(f"{df['type_lib'].item()} ", "", a))
                    if df['type_lib'].values[0] in list(type_insee.keys()):
                        df["type_insee"] = type_insee[df['type_lib'].item()]
                    else:
                        df["type_insee"] = df['type_lib'].str.upper()

                    liste_address.append(df)
        except:
            pass

    df_ad = pd.concat(liste_address)

    return df_ad


def req_xml_aws(df_path_fr: pd.DataFrame) -> pd.DataFrame:
    """
    Read XML from AWS to get parties French patents
    """
    logger = get_logger(threading.current_thread().name)
    logger.info("start query xml aws")
    liste = []
    for _, r in df_path_fr.iterrows():
        with open(r.fullpath) as f:
            data = xmltodict.parse(f.read())

        djson = json.dumps(data)
        djson2 = json.loads(djson)
        clef_biblio = list(djson2["fr-patent-document"]["fr-bibliographic-data"])
        clef_parties = list(djson2["fr-patent-document"]["fr-bibliographic-data"]["parties"].keys())
        djson_app = djson2["fr-patent-document"]["fr-bibliographic-data"]["parties"]["applicants"]["applicant"]
        if "fr-owners" in clef_parties:
            djson_own = djson2["fr-patent-document"]["fr-bibliographic-data"]["parties"]["fr-owners"]["fr-owner"]
        elif "fr-owners" in clef_biblio:
            djson_own = djson2["fr-patent-document"]["fr-bibliographic-data"]["fr-owners"]["fr-owner"]

        df_app = pd.json_normalize(data=djson_app)
        df_own = pd.json_normalize(data=djson_own)
        df = pd.concat([df_app, df_own])
        df["publication_number"] = r.publication_number
        df["publication_number_fr"] = r.publication_number_fr
        liste.append(df)

    df_inpi = pd.concat(liste)

    df_inpi = pd.merge(df_path_fr, df_inpi, on=["publication_number", "publication_number_fr"])

    logger.info("end query xml aws")

    return df_inpi


def parse_xml_aws(df_path_fr: pd.DataFrame) -> pd.DataFrame:
    """
        Read XML from AWS to get parties French patents
    """
    logger = get_logger(threading.current_thread().name)
    logger.info("Start parsing files inpi-xmls")
    liste = []
    for _, r in df_path_fr.iterrows():
        with open(r.fullpath) as f:
            data = xmltodict.parse(f.read())

        djson = json.dumps(data)
        djson2 = json.loads(djson)
        clef_biblio = list(djson2["fr-patent-document"]["fr-bibliographic-data"])
        clef_parties = list(djson2["fr-patent-document"]["fr-bibliographic-data"]["parties"].keys())
        djson_app = djson2["fr-patent-document"]["fr-bibliographic-data"]["parties"]["applicants"]["applicant"]
        if "fr-owners" in clef_parties:
            djson_own = djson2["fr-patent-document"]["fr-bibliographic-data"]["parties"]["fr-owners"]["fr-owner"]
        elif "fr-owners" in clef_biblio:
            djson_own = djson2["fr-patent-document"]["fr-bibliographic-data"]["fr-owners"]["fr-owner"]

        df_app = pd.json_normalize(data=djson_app)
        df_app["type"] = "applicant"
        df_own = pd.json_normalize(data=djson_own)
        df_own["type"] = "owner"
        df = pd.concat([df_app, df_own])
        df["publication_number"] = r.publication_number
        liste.append(df)
    logger.info("End parsing files inpi-xmls")

    df_inpi = pd.concat(liste)

    return df_inpi


def siren_inpi_famille(fam_fr: pd.DataFrame, pths_aws: pd.DataFrame) -> pd.DataFrame:
    """
    Get SIREN from INPI patents from te same family
    """
    pths_aws = pths_aws.rename(columns={"publication_number": "publication_number_fr"})

    df_path_fr = pd.merge(fam_fr, pths_aws, on="publication_number_fr", how="inner")
    df_path_fr = df_path_fr.loc[df_path_fr["applt_seq_nr"] > 0]
    df_path_fr = df_path_fr.drop_duplicates()

    subpath_fr = subset_df(df_path_fr)

    df_inpi = res_futures(subpath_fr, req_xml_aws)

    df_inpi = df_inpi.drop(columns="siren")

    df_inpi = df_inpi.drop(
        columns=["@app-type", "@designation", "@sequence", "addressbook.@lang", "@data-format"]).drop_duplicates()

    df_inpi = df_inpi.rename(columns={'addressbook.last-name': 'last-name', 'addressbook.first-name': 'first-name',
                                      'addressbook.middle-name': 'middle-name', 'addressbook.iid': 'siren',
                                      'addressbook.address.address-1': 'address-1', 'addressbook.address.city': 'city',
                                      'addressbook.address.postcode': 'postcode',
                                      'addressbook.address.country': 'country', 'addressbook.orgname': 'orgname'})

    df_inpi[["last-name", "middle-name", "first-name", "orgname"]] = df_inpi[
        ["last-name", "middle-name", "first-name", "orgname"]].fillna("")

    df_inpi["name_multi"] = df_inpi["first-name"].str.strip() + " " + df_inpi["middle-name"].str.strip() + " " + \
                            df_inpi["last-name"].str.strip()

    df_inpi["name_multi"] = df_inpi["name_multi"].str.lstrip().str.rstrip()

    df_inpi["orgname"] = df_inpi["orgname"].str.strip()
    df_inpi["orgname"] = df_inpi["orgname"].str.replace(r"\s{2,}", "", regex=True)

    df_inpi["name_complete"] = df_inpi["orgname"] + df_inpi["name_multi"]

    df_inpi = df_inpi.loc[df_inpi["siren"].notna()]
    df_inpi = df_inpi[df_inpi["country"] == "FR"]
    df_inpi2 = df_inpi[
        ["siren", "address-1", "city", "postcode", "name_complete", "publication_number_fr",
         "publication_number"]].drop_duplicates()

    df_inpi3 = pd.merge(fam_fr.drop(columns=["siren", "publication_number_fr"]), df_inpi2,
                        on="publication_number", how="inner")

    df_inpi3["name_source2"] = df_inpi3["name_source"].apply(lambda a: unidecode(a).lower())
    df_inpi3["name_complete2"] = df_inpi3["name_complete"].apply(lambda a: unidecode(a).lower())

    df_inpi3["distance"] = df_inpi3.apply(
        lambda a: lev.distance(a["name_source2"], a["name_complete2"]),
        axis=1)

    liste_inpi = []
    for pn in set(df_inpi3["publication_number_fr"]):
        minimum = df_inpi3.loc[df_inpi3["publication_number_fr"] == pn, "distance"].min()
        tmp_inpi = df_inpi3.loc[(df_inpi3["publication_number_fr"] == pn) & (df_inpi3["distance"] == minimum)]
        liste_inpi.append(tmp_inpi)

    df_inpi4 = pd.concat(liste_inpi).drop_duplicates()
    df_inpi4["siren"] = df_inpi4["siren"].apply(lambda a: a.replace(" ", ""))
    df_inpi4_0 = df_inpi4.loc[df_inpi4["distance"] == 0].drop(
        columns=["@document-id-type", "publication_number_fr", "address-1", "city", "postcode",
                 "name_complete"]).drop_duplicates()

    df_inpi4_sup = df_inpi4.loc[df_inpi4["distance"] > 0].drop_duplicates()

    df_inpi4_sup["address_2"] = df_inpi4_sup["address_source"].apply(lambda a: a.replace(",", "").split(" "))
    df_inpi4_sup["cp"] = df_inpi4_sup["address_2"].apply(lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b)))
    df_inpi4_sup["cp"] = df_inpi4_sup["cp"].apply(lambda a: [item[0] for item in a if item])
    df_inpi4_sup["cp"] = df_inpi4_sup["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")
    df_inpi4_sup["dep"] = df_inpi4_sup["cp"].apply(lambda a: a[0:2])

    df_address_inpi = address_api(df_inpi4_sup, "address_source")
    df_inpi4_sup_address = pd.merge(df_inpi4_sup, df_address_inpi, left_on="address_source", right_on="address",
                                    how="inner")

    df_inpi4_sup_address_check = df_inpi4_sup_address.copy()
    df_inpi4_sup_address_check["city_source"] = df_inpi4_sup_address_check["city_x"].apply(
        lambda a: unidecode(a).lower())
    df_inpi4_sup_address_check["city_ad"] = df_inpi4_sup_address_check["city_y"].apply(
        lambda a: unidecode(a).lower())
    df_inpi4_sup_address_check["test_cp"] = df_inpi4_sup_address_check["postcode_x"] == df_inpi4_sup_address_check[
        "postcode_y"]

    df_inpi4_sup_address_check["test_city"] = df_inpi4_sup_address_check["city_source"] == df_inpi4_sup_address_check[
        "city_ad"]

    df_inpi4_sup_address_check_true = df_inpi4_sup_address_check.loc[
        (df_inpi4_sup_address_check["test_city"]) & (df_inpi4_sup_address_check["test_cp"])].reset_index().drop(
        columns="index")

    cols = list(set(df_inpi4_sup_address_check_true.columns).intersection(set(df_inpi4_0.columns)))

    df_inpi4_sup_address_check_true = df_inpi4_sup_address_check_true[cols].drop_duplicates()

    df_inpi4_k = pd.concat([df_inpi4_0, df_inpi4_sup_address_check_true]).drop_duplicates()

    return df_inpi4_k

@retry(tries=3, delay=5, backoff=5)
def fam_oeb(tst_na: pd.DataFrame) -> pd.DataFrame:
    """
    Get different publication numbers from patent family from EPO
    """
    print("Start get publication numbers in family", flush=True)
    dict3 = {}

    pub = set(tst_na["publication_number"])

    for pb in pub:
        dict3[pb] = tst_na.loc[tst_na["publication_number"] == pb]

    dict4 = {}

    liste_famille = []

    for pb in pub:
        print(f"Publication number: {pb}")
        time.sleep(1)
        token = get_token_oeb()
        fam = requests.get(f"http://ops.epo.org/3.2/rest-services/family/publication/docdb/EP{pb}.json",
                           headers={"Authorization": token, 'Accept': 'application/json'})
        if fam.status_code == 200:
            print("Success")
            data = fam.json()
            djson3 = data["ops:world-patent-data"]["ops:patent-family"]["ops:family-member"]
            liste = []
            if isinstance(djson3, list):
                for item in djson3:
                    # print(item)
                    tmp = pd.json_normalize(item['publication-reference'], record_path=["document-id"])
                    liste.append(tmp)
            else:
                print(djson3)
                tmp = pd.json_normalize(djson3['publication-reference'], record_path=["document-id"])
                liste.append(tmp)

            df = pd.concat(liste)
            for col in list(df.columns):
                nom = re.sub("\.\$", "", col)
                df = df.rename(columns={col: nom})
            df2 = df.loc[df["@document-id-type"] == "docdb"]
            df2 = df2.loc[df2["country"].isin(["EP", "FR"])]
            df2["pn"] = "EP" + pb
            df2 = df2.loc[df2["pn"] != "EP" + df2["doc-number"]]

            if len(df2) > 0:
                dict4[pb] = df2
                liste_famille.append(df2)
        else:
            print("error")

    df_fam = pd.concat(liste_famille)
    df_fam = df_fam.rename(columns={"country": "appln_auth", "doc-number": "publication-number"})

    print("End get publication numbers in family", flush=True)

    return df_fam

@retry(tries=3, delay=5, backoff=5)
def bodacc(t_address: pd.DataFrame) -> pd.DataFrame:
    """
    Get SIREN from BODACC
    """
    logger = get_logger(threading.current_thread().name)
    logger.info("start query bodacc")
    col_drop = ['personne.typePersonne', 'personne.adresseSiegeSocial.pays', 'personne.formeJuridique',
                'personne.numeroImmatriculation.codeRCS', 'personne.numeroImmatriculation.nomGreffeImmat',
                'personne.adresseSiegeSocial.complGeographique',
                'personne.sigle',
                'personne.administration',
                'personne.capital.devise',
                'personne.capital.montantCapital',
                'personne.activite',
                'personne.capital.capitalVariable',
                'personne.nomCommercial',
                'personne.adressePP.numeroVoie',
                'personne.adressePP.typeVoie',
                'personne.adressePP.nomVoie',
                'personne.adressePP.codePostal',
                'personne.adressePP.ville',
                'personne.adressePP.pays',
                'personne.nom',
                'personne.prenom',
                'personne.nationalite',
                'personne.adresseEtablissementPrincipal.ville',
                'personne.adresseEtablissementPrincipal.pays',
                'personne.adresseEtablissementPrincipal.typeVoie',
                'personne.adresseEtablissementPrincipal.codePostal',
                'personne.adresseEtablissementPrincipal.numeroVoie',
                'personne.adresseEtablissementPrincipal.nomVoie',
                "personne.adresseSiegeSocial.ville", "personne.adresseSiegeSocial.typeVoie",
                "personne.adresseSiegeSocial.nomVoie",
                ]

    lste_bodacc = []
    for _, r in t_address.iterrows():
        cmc = r["name_source"]
        if "city" in t_address.columns:
            dpt = r["dep"]
            cp = r["cp"]
            ville = r["city"]
            requete = f"q=commercant={cmc} numerodepartement={dpt} ville={ville} cp={cp}&rows=1000&facet=registre"
            url = URL + requete
            try:
                response = get_url(url)
                data = json.loads(response.text)
                rec = data["records"]
                fields = [item["fields"] for item in rec]
                personnes = [json.loads(item["listepersonnes"]) for item in fields]
                df = pd.json_normalize(personnes)
                df["name_source"] = cmc
                liste_col = list(df.columns)
                if 'personne.typePersonne' in liste_col:
                    df = df.loc[df["personne.typePersonne"] == "pm"]
                    for col in liste_col:
                        if col in col_drop:
                            df = df.drop(columns=col)
                    df.loc[df["personne.denomination"].notna(), "personne.denomination"] = df.loc[
                        df["personne.denomination"].notna(), "personne.denomination"].apply(lambda a: a.lower())
                    df = df.drop_duplicates().reset_index().drop(columns="index")
                    df["personne.numeroImmatriculation.numeroIdentification"] = df[
                        "personne.numeroImmatriculation.numeroIdentification"].replace(r"\s{1,}", "", regex=True)

                    lste_bodacc.append(df)
                else:
                    pass
            except:
                pass
        elif "dep" and "cp" in t_address.columns:
            dpt = r["dep"]
            cp = r["cp"]
            requete = f"q=commercant={cmc} numerodepartement={dpt} cp={cp}&rows=1000&facet=registre"
            url = URL + requete
            try:
                response = get_url(url)
                data = json.loads(response.text)
                rec = data["records"]
                fields = [item["fields"] for item in rec]
                personnes = [json.loads(item["listepersonnes"]) for item in fields]
                df = pd.json_normalize(personnes)
                df["name_source"] = cmc
                liste_col = list(df.columns)
                if 'personne.typePersonne' in liste_col:
                    df = df.loc[df["personne.typePersonne"] == "pm"]
                    for col in liste_col:
                        if col in col_drop:
                            df = df.drop(columns=col)
                    df.loc[df["personne.denomination"].notna(), "personne.denomination"] = df.loc[
                        df["personne.denomination"].notna(), "personne.denomination"].apply(lambda a: a.lower())
                    df = df.drop_duplicates().reset_index().drop(columns="index")
                    df["personne.numeroImmatriculation.numeroIdentification"] = df[
                        "personne.numeroImmatriculation.numeroIdentification"].replace(r"\s{1,}", "", regex=True)

                    lste_bodacc.append(df)
                else:
                    pass
            except:
                pass
    if lste_bodacc:
        df_bo = pd.concat(lste_bodacc)
    else:
        df_bo = pd.DataFrame()

    logger.info("end query bodacc")

    return df_bo

@retry(tries=3, delay=5, backoff=5)
def req_scanr_stru(df_stru_fuz: pd.DataFrame) -> pd.DataFrame:
    """
    Get SIREN and other ID from ScanR
    """
    # prod scanr
    url_structures = "https://scanr.enseignementsup-recherche.gouv.fr/api/scanr-organizations/_search"

    dict_res = {"person_id": [], "name_source": [], "name_propre": [], "address_source": [], "key_appln_nr_person": [],
                "externalIds": [],
                "label_default": [],
                "label_fr": [], "address_scanr": []}

    logger = get_logger(threading.current_thread().name)
    logger.info("start query scanr structures")

    df_stru_fuz["address_source"] = df_stru_fuz["address_source"].fillna("")

    for _, r in df_stru_fuz.iterrows():
        query1 = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "query": r.name_propre,
                                "default_field": "label.default"
                            }
                        },
                        {
                            "query_string": {
                                "query": r.name_propre,
                                "default_field": "label.fr"
                            }
                        }
                    ]
                }
            }
        }

        res1 = requests.get(url_structures, json=query1, verify=False).json()
        if res1.get("status"):
            pass
        elif len(res1.get("hits").get("hits")) > 0:
            res2 = res1.get("hits").get("hits")
            if isinstance(res2, list):
                dico_source = res2[0]
                if isinstance(dico_source, dict):
                    keys = dico_source.keys()
                    if "_source" in keys:
                        dico_items = dico_source.get("_source")
                        if isinstance(dico_source, dict):
                            list_k_it = dico_items.keys()
                            if "externalIds" in list_k_it:
                                dict_res["person_id"].append(r.person_id)
                                dict_res["name_source"].append(r.name_source)
                                dict_res["name_propre"].append(r.name_propre)
                                dict_res["address_source"].append(r.address_source)
                                dict_res["key_appln_nr_person"].append(r.key_appln_nr_person)
                                dict_res["externalIds"].append(dico_items.get("externalIds"))
                                if "label" in list_k_it:
                                    dic_lab = dico_items.get("label")
                                    klab = dic_lab.keys()
                                    if "default" in klab:
                                        dict_res["label_default"].append(dic_lab.get("default"))
                                    else:
                                        dict_res["label_default"].append("")
                                    if "fr" in klab:
                                        dict_res["label_fr"].append(dic_lab.get("fr"))
                                    else:
                                        dict_res["label_fr"].append("")
                                else:
                                    dict_res["label_default"].append("")
                                    dict_res["label_fr"].append("")
                                if "address" in list_k_it:
                                    dict_res["address_scanr"].append(dico_items.get("address"))
                                else:
                                    dict_res["address_scanr"].append("")

    df = pd.DataFrame(data=dict_res)

    logger.info("end query scanr structures")

    return df

@retry(tries=3, delay=5, backoff=5)
def siren_oeb_bodacc():
    os.chdir(DATA_PATH)
    # Load files

    part_entp_final = pd.read_csv("part_entp_final.csv", sep="|", encoding="utf-8",
                                  dtype=types.part_entp_types, engine="python")

    part_entp_final = part_entp_final.rename(columns={"name": "name_part"})

    p05 = pd.read_csv("part_init_p05.csv", sep="|", encoding="utf-8",
                      dtype=types.part_entp_types, engine="python")

    p05 = p05[["key_appln_nr_person", "name_source", "applt_seq_nr"]].drop_duplicates()

    noms = part_entp_final.copy()
    noms = noms.loc[noms["country_corrected"] == "FR"]
    noms.loc[(noms["siren_psn"].notna()) & (noms["siren"].isna()), "siren"] = noms.loc[
        (noms["siren_psn"].notna()) & (noms["siren"].isna()), "siren_psn"]
    noms = noms.loc[noms["siren"].isna()]
    noms = pd.merge(noms, p05, on=["key_appln_nr_person", "name_source"], how="left")
    noms = noms.loc[noms["applt_seq_nr"] > 0]
    noms = noms.loc[noms["name_source"].notna()]
    ###################################################################################################################
    # subset patents not published by neither EPO nor INPI
    notepfr = noms.loc[(noms["appln_auth"] != "EP") & (noms["appln_auth"] != "FR")]

    # subset patents published by EPO
    ep = noms.loc[noms["appln_auth"] == "EP"]

    # subset patents published by INPI
    fr = noms.loc[noms["appln_auth"] == "FR"].reset_index(drop=True)

    ###################################################################################################################
    # Patents published by INPI
    print("Start paths inpi-xlms", flush=True)

    list_dir = os.listdir(f"{DATA_PATH}INPI/")
    list_dir.sort()

    dirfile = {"fullpath": []}
    for dir in list_dir:
        for dirpath, dirs, files in os.walk(f"{DATA_PATH}INPI/{dir}/", topdown=True):
            if dirpath != f"{DATA_PATH}INPI/{dir}/":
                for item in files:
                    if item not in ["index.xml", "Volumeid"]:
                        flpath = dirpath + "/" + item
                        dirfile["fullpath"].append(flpath)

    paths_aws = pd.DataFrame(data=dirfile)
    paths_aws["file"] = paths_aws["fullpath"].str.split("/")
    paths_aws["publication_number"] = paths_aws["file"].apply(
        lambda a: "".join([x.replace(".xml", "") for x in a if ".xml" in x]))
    paths_aws["publication_number"] = paths_aws["publication_number"].replace("FR", "", regex=False)
    paths_aws = paths_aws.drop(columns="file")

    df_path_fr = pd.merge(fr, paths_aws, on="publication_number", how="inner")
    df_path_fr = df_path_fr.loc[df_path_fr["applt_seq_nr"] > 0]
    df_path_fr = df_path_fr.drop_duplicates()
    print("End paths inpi-xlms", flush=True)

    dict_path_fr = subset_df(df_path_fr)

    df_inpi = res_futures(dict_path_fr, parse_xml_aws)

    df_inpi = df_inpi.drop(
        columns=["@app-type", "@designation", "addressbook.@lang", "@data-format"]).drop_duplicates()

    df_inpi = df_inpi.rename(columns={'addressbook.last-name': 'last-name', 'addressbook.first-name': 'first-name',
                                      'addressbook.middle-name': 'middle-name', 'addressbook.iid': 'siren',
                                      'addressbook.address.address-1': 'address-1', 'addressbook.address.city': 'city',
                                      'addressbook.address.postcode': 'postcode',
                                      'addressbook.address.country': 'country', 'addressbook.orgname': 'orgname',
                                      "@sequence": "sequence"})

    df_inpi[["last-name", "middle-name", "first-name", "orgname"]] = df_inpi[
        ["last-name", "middle-name", "first-name", "orgname"]].fillna("")

    df_inpi["name_multi"] = df_inpi["first-name"].str.strip() + " " + df_inpi["middle-name"].str.strip() + " " + \
                            df_inpi["last-name"].str.strip()

    df_inpi["name_multi"] = df_inpi["name_multi"].str.lstrip().str.rstrip()

    df_inpi["orgname"] = df_inpi["orgname"].str.strip()
    df_inpi["orgname"] = df_inpi["orgname"].str.replace(r"\s{2,}", "", regex=True)

    df_inpi["name_complete"] = df_inpi["orgname"] + df_inpi["name_multi"]

    df_inpi = df_inpi.loc[df_inpi["siren"].notna()]
    df_inpi = df_inpi[df_inpi["country"] == "FR"]
    df_inpi2 = df_inpi[
        ["siren", "address-1", "city", "postcode", "name_complete", "publication_number", "type",
         "sequence"]].drop_duplicates()

    df_inpi3 = pd.merge(fr[['person_id', 'docdb_family_id', 'inpadoc_family_id', 'doc_std_name',
                            'doc_std_name_id', 'earliest_filing_date', 'name_source',
                            'address_source', 'country_source', 'publication_number', 'appln_auth',
                            'key_appln_nr_person', 'applt_seq_nr']], df_inpi2, on="publication_number", how="inner")

    df_inpi3["name_source2"] = df_inpi3["name_source"].apply(
        lambda a: ''.join(filter(str.isalpha, unidecode(a))).lower())
    df_inpi3["name_complete2"] = df_inpi3["name_complete"].apply(
        lambda a: ''.join(filter(str.isalpha, unidecode(a))).lower())

    df_inpi3["distance"] = df_inpi3.apply(
        lambda a: lev.distance(a["name_source2"], a["name_complete2"]),
        axis=1)

    df_app_inpi = df_inpi3.loc[df_inpi3["type"] == "applicant"]
    df_app_inpi["test_seq"] = df_app_inpi["sequence"] == df_app_inpi["applt_seq_nr"].astype(str)
    df_app_inpi = df_app_inpi.loc[(df_app_inpi["distance"] == 0) | (df_app_inpi["test_seq"] == True)]
    df_app_inpi = df_app_inpi.drop_duplicates()

    compte_app_inpi = df_app_inpi[["key_appln_nr_person", "siren"]].groupby(
        "key_appln_nr_person").nunique().reset_index().rename(columns={"siren": "compte"})

    df_app_inpi_ok = df_app_inpi.loc[df_app_inpi["key_appln_nr_person"].isin(
        compte_app_inpi.loc[compte_app_inpi["compte"] == 1, "key_appln_nr_person"])][
        ["publication_number", "key_appln_nr_person", "siren", "name_source"]].drop_duplicates()
    df_app_inpi_multi = df_app_inpi.loc[~df_app_inpi["key_appln_nr_person"].isin(df_app_inpi_ok["key_appln_nr_person"])]
    df_app_inpi_multi = df_app_inpi_multi.drop(columns=["sequence", "applt_seq_nr"]).drop_duplicates()
    df_app_inpi_multi_ok = df_app_inpi_multi.loc[
        (df_app_inpi_multi["distance"] == 0) & (df_app_inpi_multi["test_seq"] == True)][
        ["publication_number", "key_appln_nr_person", "siren", "name_source"]].drop_duplicates()

    fnok = df_app_inpi_multi.loc[
        ~df_app_inpi_multi["publication_number"].isin(df_app_inpi_multi_ok["publication_number"])]

    if len(fnok) > 0:
        liste_fnok = []
        for pn in set(fnok["publication_number"]):
            minimum = fnok.loc[fnok["publication_number"] == pn, "distance"].min()
            tmp_inpi_fnok = fnok.loc[(fnok["publication_number"] == pn) & (fnok["distance"] == minimum)]
            liste_fnok.append(tmp_inpi_fnok)
        if len(liste_fnok) > 0:
            fnok2 = pd.concat(liste_fnok)
            fnok2 = fnok2[["publication_number", "key_appln_nr_person", "siren", "name_source"]].drop_duplicates()

    liste_inpi = []
    for it_inpi in ["df_app_inpi_ok", "df_app_inpi_multi_ok", "fnok2"]:
        if it_inpi in locals():
            liste_inpi.append(locals().get(it_inpi))
    if len(liste_inpi) > 0:
        inpi_ok1 = pd.concat(liste_inpi)
        inpi_ok1["id"] = inpi_ok1["publication_number"] + inpi_ok1["key_appln_nr_person"]
    df_inpi3_reste = df_inpi3.copy()
    df_inpi3_reste["id"] = df_inpi3_reste["publication_number"] + df_inpi3_reste["key_appln_nr_person"]
    df_inpi3_reste = df_inpi3_reste.loc[~df_inpi3_reste["id"].isin(inpi_ok1["id"])]

    liste_inpi = []
    for pn in set(df_inpi3_reste["publication_number"]):
        minimum = df_inpi3_reste.loc[df_inpi3["publication_number"] == pn, "distance"].min()
        tmp_inpi = df_inpi3_reste.loc[
            (df_inpi3_reste["publication_number"] == pn) & (df_inpi3_reste["distance"] == minimum)]
        liste_inpi.append(tmp_inpi)

    df_inpi4 = pd.concat(liste_inpi).drop_duplicates()
    df_inpi4_0 = df_inpi4.loc[df_inpi4["distance"] == 0][
        ["publication_number", "key_appln_nr_person", "siren", "name_source"]].drop_duplicates()

    inpi_ok2 = pd.concat([inpi_ok1, df_inpi4_0])
    inpi_ok2 = inpi_ok2.drop(columns="id")

    fr2 = fr.drop(columns="siren")
    fr2 = pd.merge(fr2, inpi_ok2, on=["publication_number", "key_appln_nr_person", "name_source"],
                   how="left").drop_duplicates()

    fr2["siren"] = fr2["siren"].str.replace(r"\s", "", regex=True)
    fr2.loc[fr2["siren"].notna(), "len_siren"] = fr2.loc[fr2["siren"].notna(), "siren"].apply(lambda a: len(a))
    fr2 = fr2.astype({"len_siren": pd.Int64Dtype()})
    fr2.loc[fr2["len_siren"] == 14, "siret"] = fr2.loc[fr2["len_siren"] == 14, "siren"]
    fr2.loc[fr2["len_siren"] == 14, "siren"] = fr2.loc[fr2["len_siren"] == 14, "siren"].apply(lambda a: a[0:9])
    fr2.loc[~fr2["len_siren"].isin([9, 14]), "siren"] = np.nan
    fr2 = fr2.drop(columns="len_siren").drop_duplicates()
    fr2.loc[(fr2["siren"].isna()) & (fr2["siret"].notna()), "siren"] = fr2.loc[
        (fr2["siren"].isna()) & (fr2["siret"].notna()), "siret"].apply(lambda a: a[0:9])
    unique_siren = fr2[["key_appln_nr_person", "siren"]].groupby("key_appln_nr_person").nunique(
        dropna=False).reset_index()
    multi = unique_siren.loc[unique_siren["siren"] > 1]
    fr2.loc[fr2["key_appln_nr_person"].isin(multi["key_appln_nr_person"]), "siren"] = np.nan
    unique_siret = fr2[["key_appln_nr_person", "siret"]].groupby("key_appln_nr_person").nunique(
        dropna=False).reset_index()
    multi_siret = unique_siret.loc[unique_siret["siret"] == 2]
    trop_siret = unique_siret.loc[unique_siret["siret"] > 2]
    for person in set(multi_siret["key_appln_nr_person"]):
        sir = fr2.loc[fr2["key_appln_nr_person"] == person, "siret"].dropna().item()
        fr2.loc[(fr2["key_appln_nr_person"] == person) & (fr2["siret"].isna()), "siret"] = sir
    if len(trop_siret) > 0:
        for person in set(trop_siret["key_appln_nr_person"]):
            fr2.loc[fr2["key_appln_nr_person"] == person, "siret"] = np.nan

    fr2 = fr2.drop_duplicates()

    reste_fr = fr2.loc[fr2["siren"].isna()]

    if len(fr) != len(fr2):
        print("Attention, différence entre les publications françaises avant et après !", flush=True)
    else:
        print("Pas différence entre les publications françaises avant et après", flush=True)

    ###################################################################################################################
    # Patents published by EPO

    token_url = 'https://ops.epo.org/3.2/auth/accesstoken'
    key = os.getenv("KEY_OPS_OEB")
    data = {'grant_type': 'client_credentials'}
    headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded'}

    r = requests.post(token_url, data=data, headers=headers)

    rs = r.content.decode()
    response = json.loads(rs)

    token = f"Bearer {response.get('access_token')}"

    liste_appln = []
    liste_publn = []

    print("Start requests Register EPO part 1", flush=True)
    for clef in set(ep["publication_number"]):
        dpn = get_part(clef, token)
        if len(dpn) > 0:
            dpn = dpn.astype({"sequence": str})
        liste_appln.append(dpn)
        dd = ep.loc[ep["publication_number"] == clef].reset_index().drop(columns=["index", "siren"]).drop_duplicates()
        dd["pub_ep"] = "EP" + clef
        dd = dd.astype({"applt_seq_nr": str})
        if len(set(dpn["@change-gazette-num"])) == 1:
            dpn3 = pd.merge(dd, dpn, left_on="applt_seq_nr", right_on="sequence", how="inner")
            liste_publn.append(dpn3)
            if len(dpn3) > 0:
                liste_publn.append(dpn3)
            elif len(set(dpn.loc[dpn["@change-gazette-num"] != "N/P"])) == 1:
                dpn2 = dpn.loc[dpn["@change-gazette-num"] != "N/P"]
                dpn3 = pd.merge(dd, dpn2, left_on="applt_seq_nr", right_on="sequence", how="inner")
                liste_publn.append(dpn3)
            if len(dpn3) > 0:
                liste_publn.append(dpn3)
            else:
                d1 = dd[["name_source", "applt_seq_nr", "pub_ep"]].drop_duplicates()
                d1 = d1.astype({"applt_seq_nr": str})
                dpn2 = pd.merge(d1, dpn, left_on=["applt_seq_nr", "name_source"], right_on=["sequence", "name"],
                                how="inner")
                dpn2 = dpn2.drop(columns=["name_source", "applt_seq_nr", "pub_ep"])
                if len(dpn2) == 1:
                    dpn3 = pd.merge(dd, dpn2, left_on="applt_seq_nr", right_on="sequence", how="inner")
                    liste_publn.append(dpn3)
                if len(dpn2) > 1:
                    dpn2["@change-date2"] = dpn2["@change-date"].apply(pd.to_datetime)
                    mdate = min(dpn2["@change-date"])
                    if len(set(dpn2.loc[dpn2["@change-date2"] != mdate, "@change-gazette-num"])) == 1:
                        dpn2 = dpn2.loc[dpn2["@change-date2"] != mdate]
                        dpn3 = pd.merge(dd, dpn2, left_on="applt_seq_nr", right_on="sequence", how="inner")
                        dpn3 = dpn3.drop(columns="@change-date2")
                        liste_publn.append(dpn3)

    print("End requests Register EPO part 1", flush=True)

    appln = pd.concat(liste_appln)

    publn = pd.concat(liste_publn)

    diff = list(set(ep["publication_number"]) - set(publn["publication_number"]))

    print("Start requests Register EPO part 2", flush=True)
    for clef in diff:
        dpn = get_part(clef, token)
        dpn["clef"] = clef
        dd = ep.loc[ep["publication_number"] == clef].reset_index().drop(columns=["index", "siren"]).drop_duplicates()
        dpn2 = dpn.loc[dpn["@change-gazette-num"] != "N/P"]
        dpn2 = dpn2.astype({"sequence": str})
        dd = dd.astype({"applt_seq_nr": str})
        dd["pub_ep"] = "EP" + clef
        dd2 = pd.merge(dd, dpn2, left_on=["pub_ep", "applt_seq_nr"], right_on=["clef", "sequence"],
                       how="inner").drop(columns="clef").drop_duplicates()
        publn = pd.concat([publn, dd2])
    print("End requests Register EPO part 2", flush=True)

    publn_bodacc = publn.copy().reset_index(drop=True).drop_duplicates()

    publn_bodacc["address_2"] = publn_bodacc["address"].replace(",", "")
    publn_bodacc["address_2"] = publn_bodacc["address_2"].apply(lambda a: a.split(" "))
    publn_bodacc["cp"] = publn_bodacc["address_2"].apply(lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b)))
    publn_bodacc["cp"] = publn_bodacc["cp"].apply(lambda a: [item[0] for item in a if item])
    publn_bodacc["cp"] = publn_bodacc["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")
    publn_bodacc["dep"] = publn_bodacc["cp"].apply(lambda a: a[0:2])

    col_address = []
    col_address2 = []
    for col in list(publn_bodacc.columns):
        if col.startswith("address-"):
            col_address.append(col)

    for col in col_address:
        publn_bodacc[col + "_2"] = publn_bodacc[col].apply(
            lambda a: a.replace(",", "").split(" ") if isinstance(a, str) else a)
        publn_bodacc = publn_bodacc.drop(columns=col)
        col_address.remove(col)

    for col in list(publn_bodacc.columns):
        if col.startswith("address-") and col.endswith("_2") and col not in col_address:
            col_address2.append(col)

    for col in col_address2:
        publn_bodacc.loc[publn_bodacc["cp"] == "", "mtch"] = publn_bodacc.loc[publn_bodacc["cp"] == "", col].apply(
            lambda b: len(list(map(lambda a: re.match(r"[C|c]edex", a), b) if isinstance(b, list) else list())))
    if len(publn_bodacc.loc[publn_bodacc["cp"] == "", "mtch"]) > 0:
        publn_bodacc.loc[publn_bodacc["cp"] == "", "cp"] = publn_bodacc.loc[publn_bodacc["cp"] == "", col].apply(
            lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b) if isinstance(b, list) else ""))
    publn_bodacc["cp"] = publn_bodacc["cp"].apply(
        lambda a: [item[0] for item in a if item] if isinstance(a, list) else a)
    publn_bodacc["cp"] = publn_bodacc["cp"].apply(lambda a: a[0] if isinstance(a, list) and len(a) == 1 else a).apply(
        lambda a: "" if isinstance(a, list) else a)

    publn_bodacc = publn_bodacc.drop(columns="mtch")

    print("Start requests API adresse.gouv.fr EP", flush=True)

    df_ad_publn = address_api(publn_bodacc, "address")
    publn_bodacc = pd.merge(publn_bodacc, df_ad_publn, on="address", how="left")
    print("End requests API adresse.gouv.fr EP", flush=True)

    print("Start requests API BODACC EP", flush=True)
    sub_publn_bodacc = subset_df(publn_bodacc)
    df_bodacc = res_futures(sub_publn_bodacc, bodacc)
    print("End requests API BODACC EP", flush=True)

    publn_bodacc = pd.merge(publn_bodacc, df_bodacc, on="name_source", how="left")

    publn_bodacc.loc[:, "housenumber2"] = ""
    publn_bodacc.loc[:, "nbr_min"] = 0
    publn_bodacc.loc[:, "nbr_max"] = 0
    for i, r in publn_bodacc.loc[publn_bodacc["housenumber"].notna()].iterrows():
        chaine = r.housenumber
        ch = split_hs(chaine)
        publn_bodacc.at[i, "housenumber2"] = ch
        if ch:
            mini = int(min(ch))
            maxi = int(max(ch)) + 1
            publn_bodacc.at[i, "nbr_min"] = mini
            publn_bodacc.at[i, "nbr_max"] = maxi

    publn_bodacc["numeroVoie2"] = ""
    publn_bodacc["no_min"] = 0
    publn_bodacc["no_max"] = 0
    for i, r in publn_bodacc.loc[
        publn_bodacc["personne.adresseSiegeSocial.numeroVoie"].notna()].iterrows():
        chaine2 = str(r["personne.adresseSiegeSocial.numeroVoie"])
        ch2 = split_hs(chaine2)
        publn_bodacc.at[i, "numeroVoie2"] = ch2
        if ch2 != []:
            mini2 = int(min(ch2))
            maxi2 = int(max(ch2)) + 1
            publn_bodacc.at[i, "no_min"] = mini2
            publn_bodacc.at[i, "no_max"] = maxi2

    publn_bodacc = publn_bodacc.astype({"nbr_min": pd.Int64Dtype(), "nbr_max": pd.Int64Dtype(),
                                        "no_min": pd.Int64Dtype(), "no_max": pd.Int64Dtype()})

    publn_bodacc.loc[publn_bodacc["housenumber2"].notna(), "nbr_et"] = publn_bodacc.loc[
        publn_bodacc["housenumber2"].notna(), ["nbr_min", "nbr_max"]].apply(
        lambda a: [x for x in range(a.nbr_min, a.nbr_max) if x % 2 == 0] if a.nbr_min % 2 == 0 else [x
                                                                                                     for x in
                                                                                                     range(a.nbr_min,
                                                                                                           a.nbr_max) if
                                                                                                     x % 2 > 0], axis=1)

    publn_bodacc.loc[publn_bodacc["numeroVoie2"].notna(), "no_et"] = publn_bodacc.loc[
        publn_bodacc["numeroVoie2"].notna(), ["no_min", "no_max"]].apply(
        lambda a: [x for x in range(a.no_min, a.no_max) if x % 2 == 0] if a.no_min % 2 == 0 else [x for
                                                                                                  x in range(a.no_min,
                                                                                                             a.no_max)
                                                                                                  if x % 2 > 0], axis=1)

    publn_bodacc.loc[publn_bodacc["housenumber2"].notna(), "len_nr"] = publn_bodacc.loc[
        publn_bodacc["housenumber2"].notna(), "housenumber2"].apply(lambda a: len(a))
    publn_bodacc.loc[publn_bodacc["numeroVoie2"].notna(), "len_no"] = publn_bodacc.loc[
        publn_bodacc["numeroVoie2"].notna(), "numeroVoie2"].apply(lambda a: len(a))

    publn_bodacc = publn_bodacc.astype({"len_nr": pd.Int64Dtype(), "len_no": pd.Int64Dtype()})

    publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (publn_bodacc["len_no"] == 1), "ep"] = \
        publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (publn_bodacc["len_no"] == 1), "housenumber"] == \
        publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (publn_bodacc["len_no"] == 1),
        "personne.adresseSiegeSocial.numeroVoie"]

    publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (publn_bodacc["len_no"] > 1), "ep"] = \
        publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (
                publn_bodacc["len_no"] > 1)].apply(lambda a: a["housenumber2"][0] in a["numeroVoie2"], axis=1)

    publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (publn_bodacc["len_no"] > 1), "ep"] = \
        publn_bodacc.loc[(publn_bodacc["len_nr"] == 1) & (
                publn_bodacc["len_no"] > 1)].apply(lambda a: a["housenumber2"][0] in a["numeroVoie2"],
                                                   axis=1)

    publn_bodacc.loc[(publn_bodacc["len_nr"] > 1) & (publn_bodacc["len_no"] == 1), "ep"] = \
        publn_bodacc.loc[
            (publn_bodacc["len_nr"] > 1) & (publn_bodacc["len_no"] == 1)].apply(
            lambda a: a["numeroVoie2"][0] in a["housenumber2"], axis=1)

    publn_bodacc = publn_bodacc.loc[publn_bodacc["person_id"].notna()]

    publn_bodacc = publn_bodacc.drop(
        columns=["housenumber2", "numeroVoie2", "nbr_min", "nbr_max", "no_min", "no_max", "nbr_et",
                 "no_et", "len_nr", "len_no"])

    publn_bodacc.loc[:, "ep2"] = publn_bodacc.loc[:, "ep"].apply(lambda a: 1 if a is True else 0)

    publn_bodacc = publn_bodacc.loc[publn_bodacc["ep2"] == 1].drop(columns="ep")

    for col in list(publn_bodacc.columns):
        if col.startswith("address-") and col.endswith("_2"):
            publn_bodacc = publn_bodacc.drop(columns=col)
    publn_bodacc = publn_bodacc.drop(columns="address_2")

    publn_bodacc = publn_bodacc.drop_duplicates()

    publn_bodacc["name2"] = publn_bodacc["name_source"].apply(lambda a: a.lower())
    publn_bodacc["distance"] = publn_bodacc.apply(lambda a: lev.distance(a["name2"], a["personne.denomination"]),
                                                  axis=1)

    liste_publn_bodacc = []
    for name in set(publn_bodacc["name_source"]):
        df = publn_bodacc.loc[publn_bodacc["name_source"] == name]
        dmin = min(df["distance"])
        df = df.loc[df["distance"] == dmin]
        df = df.drop(columns=["distance", "name2"])
        df = df.loc[df["person_id"].notna()]
        df = df.loc[df["personne.numeroImmatriculation.numeroIdentification"].notna()]
        df = df.rename(columns={"personne.numeroImmatriculation.numeroIdentification": "siren"})
        df = df.astype({"applt_seq_nr": int})
        liste_publn_bodacc.append(df)

    publn_bodacc2 = pd.concat(liste_publn_bodacc)

    cmpte = publn_bodacc2[["person_id", "docdb_family_id", "inpadoc_family_id", "name_source", "publication_number",
                           "key_appln_nr_person", "applt_seq_nr", "siren"]].copy()

    cmpte["id"] = cmpte["person_id"].astype(str) + cmpte["docdb_family_id"].astype(str) + cmpte[
        "inpadoc_family_id"].astype(str) + cmpte["name_source"] + \
                  cmpte["publication_number"] + cmpte["key_appln_nr_person"] + cmpte["applt_seq_nr"].astype(str)
    cmpte = cmpte.drop_duplicates().reset_index(drop=True)

    cmpte_cmp = cmpte[["id", "siren"]].groupby("id").nunique().reset_index().rename(columns={"siren": "compte"})

    cmpte = pd.merge(cmpte, cmpte_cmp, on="id", how="left")

    cmpte = cmpte.drop(columns=["id", "siren"]).drop_duplicates()

    publn_bodacc3 = pd.merge(publn_bodacc2, cmpte,
                             on=["person_id", "docdb_family_id", "inpadoc_family_id", "name_source",
                                 "publication_number",
                                 "key_appln_nr_person", "applt_seq_nr"], how="right")

    publn_bodacc3.loc[publn_bodacc3["compte"] > 1, "siren"] = np.nan

    publn_bodacc3 = publn_bodacc3.drop(columns="compte")
    publn_bodacc3 = publn_bodacc3.loc[publn_bodacc3["siren"].notna()]
    publn_bodacc3 = publn_bodacc3[ep.columns].drop_duplicates()

    reste_ep = ep.loc[~ep["key_appln_nr_person"].isin(publn_bodacc3["key_appln_nr_person"])]

    ###################################################################################################################
    # Patents published neither by EPO nor INPI
    nepfr_address = notepfr.loc[
        (~notepfr["key_appln_nr_person"].isin(publn["key_appln_nr_person"])) & (notepfr["address_source"].notna())]
    nepfr_address = nepfr_address.rename(columns={"address_source": "address"})
    nepfr_address = nepfr_address.drop(columns="siren")

    nepfr_address["address" + "_2"] = nepfr_address["address"].apply(lambda a: a.replace(",", "").split(" "))
    nepfr_address["cp"] = nepfr_address["address_2"].apply(lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b)))
    nepfr_address["cp"] = nepfr_address["cp"].apply(lambda a: [item[0] for item in a if item])
    nepfr_address["cp"] = nepfr_address["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")
    nepfr_address["dep"] = nepfr_address["cp"].apply(lambda a: a[0:2])

    print("Start requests API adresse.gouv.fr NEPFR", flush=True)

    df_ad_nepfr = address_api(nepfr_address, "address")
    print("End requests API adresse.gouv.fr NEPFR", flush=True)

    nepfr_address = pd.merge(nepfr_address, df_ad_nepfr, on="address", how="left")

    col_address = []
    col_address2 = []
    nepfr_address["address_2"] = nepfr_address["address"].apply(lambda a: a.replace(",", "").split(" "))
    for col in list(nepfr_address.columns):
        if col.startswith("address-"):
            col_address.append(col)

    for col in col_address:
        nepfr_address[col + "_2"] = nepfr_address[col].apply(
            lambda a: a.replace(",", "").split(" ") if isinstance(a, str) else a)
        nepfr_address = nepfr_address.drop(columns=col)
        col_address.remove(col)

    for col in list(nepfr_address.columns):
        if col.startswith("address-") and col.endswith("_2") and col not in col_address:
            col_address2.append(col)

    nepfr_address["cp"] = nepfr_address["address_2"].apply(
        lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b) if isinstance(b, list) else list()))
    nepfr_address["cp"] = nepfr_address["cp"].apply(lambda a: [item[0] for item in a if item])
    nepfr_address["cp"] = nepfr_address["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")

    nepfr_address["dep"] = nepfr_address["cp"].apply(lambda a: a[0:2])

    nepfr_address = nepfr_address.loc[nepfr_address["housenumber"].notna()]

    print("Start requests API BODACC NEPFR", flush=True)
    sub_nepfr_address = subset_df(nepfr_address)
    df_bodacc = res_futures(sub_nepfr_address, bodacc)
    print("End requests API BODACC NEPFR", flush=True)

    if len(df_bodacc) > 0:
        nepfr_address = pd.merge(nepfr_address, df_bodacc, on="name_source", how="left")
        nepfr_address = nepfr_address.loc[nepfr_address["personne.numeroImmatriculation.numeroIdentification"].notna()]

        if len(nepfr_address) > 0:
            nepfr_address.loc[:, "housenumber2"] = ""
            nepfr_address.loc[:, "nbr_min"] = 0
            nepfr_address.loc[:, "nbr_max"] = 0
            for i, r in nepfr_address.loc[nepfr_address["housenumber"].notna()].iterrows():
                chaine = r.housenumber
                ch = split_hs(chaine)
                nepfr_address.at[i, "housenumber2"] = ch
                if ch:
                    mini = int(min(ch))
                    maxi = int(max(ch)) + 1
                    nepfr_address.at[i, "nbr_min"] = mini
                    nepfr_address.at[i, "nbr_max"] = maxi

            nepfr_address["numeroVoie2"] = ""
            nepfr_address["no_min"] = 0
            nepfr_address["no_max"] = 0
            for i, r in nepfr_address.loc[
                nepfr_address["personne.adresseSiegeSocial.numeroVoie"].notna()].iterrows():
                chaine2 = str(r["personne.adresseSiegeSocial.numeroVoie"])
                ch2 = split_hs(chaine2)
                nepfr_address.at[i, "numeroVoie2"] = ch2
                if ch2 != []:
                    mini2 = int(min(ch2))
                    maxi2 = int(max(ch2)) + 1
                    nepfr_address.at[i, "no_min"] = mini2
                    nepfr_address.at[i, "no_max"] = maxi2

            nepfr_address = nepfr_address.astype({"nbr_min": pd.Int64Dtype(), "nbr_max": pd.Int64Dtype(),
                                                  "no_min": pd.Int64Dtype(), "no_max": pd.Int64Dtype()})

            nepfr_address.loc[nepfr_address["housenumber2"].notna(), "nbr_et"] = nepfr_address.loc[
                nepfr_address["housenumber2"].notna(), ["nbr_min", "nbr_max"]].apply(
                lambda a: [x for x in range(a.nbr_min, a.nbr_max) if x % 2 == 0] if a.nbr_min % 2 == 0 else [x
                                                                                                             for x in
                                                                                                             range(
                                                                                                                 a.nbr_min,
                                                                                                                 a.nbr_max)
                                                                                                             if
                                                                                                             x % 2 > 0],
                axis=1)

            nepfr_address.loc[nepfr_address["numeroVoie2"].notna(), "no_et"] = nepfr_address.loc[
                nepfr_address["numeroVoie2"].notna(), ["no_min", "no_max"]].apply(
                lambda a: [x for x in range(a.no_min, a.no_max) if x % 2 == 0] if a.no_min % 2 == 0 else [x for
                                                                                                          x in
                                                                                                          range(
                                                                                                              a.no_min,
                                                                                                              a.no_max)
                                                                                                          if x % 2 > 0],
                axis=1)

            nepfr_address.loc[nepfr_address["housenumber2"].notna(), "len_nr"] = nepfr_address.loc[
                nepfr_address["housenumber2"].notna(), "housenumber2"].apply(lambda a: len(a))
            nepfr_address.loc[nepfr_address["numeroVoie2"].notna(), "len_no"] = nepfr_address.loc[
                nepfr_address["numeroVoie2"].notna(), "numeroVoie2"].apply(lambda a: len(a))

            nepfr_address = nepfr_address.astype({"len_nr": pd.Int64Dtype(), "len_no": pd.Int64Dtype()})

            nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (nepfr_address["len_no"] == 1), "ep"] = \
                nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (nepfr_address["len_no"] == 1), "housenumber"] == \
                nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (nepfr_address["len_no"] == 1),
                "personne.adresseSiegeSocial.numeroVoie"]

            nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (nepfr_address["len_no"] > 1), "ep"] = \
                nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (
                        nepfr_address["len_no"] > 1)].apply(lambda a: a["housenumber2"][0] in a["numeroVoie2"], axis=1)

            nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (nepfr_address["len_no"] > 1), "ep"] = \
                nepfr_address.loc[(nepfr_address["len_nr"] == 1) & (
                        nepfr_address["len_no"] > 1)].apply(lambda a: a["housenumber2"][0] in a["numeroVoie2"],
                                                            axis=1)

            nepfr_address.loc[(nepfr_address["len_nr"] > 1) & (nepfr_address["len_no"] == 1), "ep"] = \
                nepfr_address.loc[
                    (nepfr_address["len_nr"] > 1) & (nepfr_address["len_no"] == 1)].apply(
                    lambda a: a["numeroVoie2"][0] in a["housenumber2"], axis=1)

            nepfr_address = nepfr_address.loc[nepfr_address["person_id"].notna()]

            nepfr_address = nepfr_address.drop(
                columns=["housenumber2", "numeroVoie2", "nbr_min", "nbr_max", "no_min", "no_max", "nbr_et",
                         "no_et", "len_nr", "len_no"])

            nepfr_address.loc[:, "ep2"] = nepfr_address.loc[:, "ep"].apply(lambda a: 1 if a is True else 0)

            nepfr_address = nepfr_address.loc[nepfr_address["ep2"] == 1].drop(columns="ep")

            if len(nepfr_address) > 0:
                nepfr_address["name2"] = nepfr_address["name_source"].apply(lambda a: a.lower())
                nepfr_address["distance"] = nepfr_address.apply(
                    lambda a: lev.distance(a["name2"], a["personne.denomination"]),
                    axis=1)

                liste_publn_nepfr = []
                for name in set(nepfr_address["name_source"]):
                    df = nepfr_address.loc[nepfr_address["name_source"] == name]
                    dmin = min(df["distance"])
                    df = df.loc[df["distance"] == dmin]
                    df = df.drop(columns=["distance", "name2"])
                    df = df.loc[df["person_id"].notna()]
                    df = df.loc[df["personne.numeroImmatriculation.numeroIdentification"].notna()]
                    df = df.rename(columns={"personne.numeroImmatriculation.numeroIdentification": "siren"})
                    df = df.astype({"applt_seq_nr": int})
                    liste_publn_nepfr.append(df)

                nepfr_address2 = pd.concat(liste_publn_nepfr)

                cmpte = nepfr_address2[
                    ["person_id", "docdb_family_id", "inpadoc_family_id", "name_source", "publication_number",
                     "key_appln_nr_person", "applt_seq_nr", "siren"]].copy()

                cmpte["id"] = cmpte["person_id"].astype(str) + cmpte["docdb_family_id"].astype(str) + cmpte[
                    "inpadoc_family_id"].astype(str) + cmpte["name_source"] + \
                              cmpte["publication_number"] + cmpte["key_appln_nr_person"] + cmpte["applt_seq_nr"].astype(
                    str)
                cmpte = cmpte.drop_duplicates().reset_index(drop=True)

                cmpte_cmp = cmpte[["id", "siren"]].groupby("id").nunique().reset_index().rename(
                    columns={"siren": "compte"})

                cmpte = pd.merge(cmpte, cmpte_cmp, on="id", how="left")

                cmpte = cmpte.drop(columns=["id", "siren"]).drop_duplicates()

                nepfr_address3 = pd.merge(nepfr_address2, cmpte,
                                          on=["person_id", "docdb_family_id", "inpadoc_family_id", "name_source",
                                              "publication_number",
                                              "key_appln_nr_person", "applt_seq_nr"], how="right")

                nepfr_address3.loc[nepfr_address3["compte"] > 1, "siren"] = np.nan

                nepfr_address3 = nepfr_address3.drop(columns="compte")
                nepfr_address3 = nepfr_address3.loc[nepfr_address3["siren"].notna()]
                com_cols = list(set(nepfr_address3.columns).intersection(set(notepfr.columns)))
                nepfr_address3 = nepfr_address3[com_cols].drop_duplicates()

        if "nepfr_address3" in locals():
            if len(nepfr_address3) > 0:
                reste_notepfr = notepfr.loc[~notepfr["key_appln_nr_person"].isin(nepfr_address3["key_appln_nr_person"])]
            else:
                reste_notepfr = notepfr.copy()
        else:
            reste_notepfr = notepfr.copy()

    ###################################################################################################################
    # Patents EP not already found
    df_famille = fam_oeb(reste_ep)

    # récupère numéros de publication de la famille pour retrouver adresse/siren

    ### Publications françaises dans les familles

    df_famille_fr = df_famille.loc[df_famille["appln_auth"] == "FR"].drop_duplicates()
    df_famille_fr2 = df_famille_fr.rename(columns={"publication-number": "publication_number_fr"})
    df_famille_fr2["publication_number"] = df_famille_fr2["pn"].str.replace("EP", "")
    df_famille_fr2 = df_famille_fr2.drop(columns=["kind", "date"]).drop_duplicates()
    df_famille_fr2 = pd.merge(reste_ep, df_famille_fr2, on="publication_number",
                              how="inner").drop_duplicates()

    df_famille_fr_ok = siren_inpi_famille(df_famille_fr2, paths_aws)

    liste_df_famille_fr_ok = []
    for key in set(df_famille_fr_ok["key_appln_nr_person"]):
        df = df_famille_fr_ok.loc[df_famille_fr_ok["key_appln_nr_person"] == key]
        dmin = min(df["distance"])
        df = df.loc[df["distance"] == dmin]
        liste_df_famille_fr_ok.append(df)

    df_famille_fr_ok2 = pd.concat(liste_df_famille_fr_ok)
    df_famille_fr_ok2 = df_famille_fr_ok2.drop(columns="appln_auth_y").rename(columns={"appln_auth_x": "appln_auth"})

    com_col = list(set(df_famille_fr_ok2.columns).intersection(set(noms.columns)))
    df_famille_fr_ok2 = df_famille_fr_ok2[com_col]

    reste_ep_fr = reste_ep.loc[(~reste_ep["key_appln_nr_person"].isin(df_famille_fr_ok2["key_appln_nr_person"])) & (
        reste_ep["publication_number"].isin(df_famille_fr2["publication_number"]))]

    notpn = list(set(df_famille_fr_ok2["publication_number"]).union(set(reste_ep_fr["publication_number"])))

    ### Publications EP dans les familles

    df_famille_ep = df_famille.copy()
    df_famille_ep["publication_number"] = df_famille_ep["pn"].str.replace("EP", "", regex=False)
    df_famille_ep = df_famille_ep.loc[
        (df_famille_ep["appln_auth"] == "EP") & (~df_famille_ep["publication_number"].isin(notpn))].drop_duplicates()
    df_famille_ep2 = df_famille_ep.rename(columns={"publication-number": "publication_number_ep"})
    df_famille_ep2 = pd.merge(reste_ep, df_famille_ep2, on="publication_number",
                              how="inner").drop_duplicates()
    df_famille_ep2 = df_famille_ep2.drop(columns="appln_auth_y").rename(columns={"appln_auth_x": "appln_auth"})

    token_url = 'https://ops.epo.org/3.2/auth/accesstoken'
    key = os.getenv("KEY_OPS_OEB")
    data = {'grant_type': 'client_credentials'}
    headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded'}

    print("Start registry OEB publication number family", flush=True)

    r = requests.post(token_url, data=data, headers=headers)

    rs = r.content.decode()
    response = json.loads(rs)

    token = f"Bearer {response.get('access_token')}"

    liste_oeb = []

    df_famille_ep2["pub_ep"] = df_famille_ep2["publication_number_ep"].apply(lambda a: "EP" + a)

    for pub in set(df_famille_ep2["publication_number_ep"]):
        time.sleep(1)
        token = get_token_oeb()
        dpn = get_part(pub, token)
        dpn = dpn.astype({"sequence": str})
        dpn["pub_ep"] = "EP" + pub
        dpn = dpn.loc[dpn["country"] == "FR"]
        if len(dpn) > 0:
            dpn2 = pd.merge(dpn, df_famille_ep2.loc[df_famille_ep2["publication_number_ep"] == pub], on="pub_ep",
                            how="left")
            dpn2["name_source2"] = dpn2["name_source"].apply(lambda a: unidecode(a).lower())
            dpn2["name2"] = dpn2["name"].apply(lambda a: unidecode(a).lower())

            dpn2["distance"] = dpn2.apply(
                lambda a: lev.distance(a["name_source2"], a["name2"]),
                axis=1)
            dmin = min(dpn2["distance"])
            dpn2 = dpn2.loc[dpn2["distance"] == dmin]
            dpn2 = dpn2.drop(columns=["distance", "name2", "name_source2"]).drop_duplicates()
            liste_oeb.append(dpn2)

    df_oeb = pd.concat(liste_oeb)

    df_oeb = df_oeb.drop(columns="siren")

    print("End get registry OEB publication number family", flush=True)

    print("Start requests API adresse.gouv.fr OEB", flush=True)

    df_address_oeb = address_api(df_oeb, "address")

    df_address_oeb = pd.merge(df_oeb, df_address_oeb, on="address", how="left")
    print("End requests API adresse.gouv.fr OEB", flush=True)

    df_address_oeb["address" + "_2"] = df_address_oeb["address"].apply(lambda a: a.replace(",", "").split(" "))
    df_address_oeb["cp"] = df_address_oeb["address_2"].apply(lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b)))
    df_address_oeb["cp"] = df_address_oeb["cp"].apply(lambda a: [item[0] for item in a if item])
    df_address_oeb["cp"] = df_address_oeb["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")
    df_address_oeb["dep"] = df_address_oeb["cp"].apply(lambda a: a[0:2])

    sub_address_oeb = subset_df(df_address_oeb)
    df_oeb_bodacc = res_futures(sub_address_oeb, bodacc)

    if len(df_oeb_bodacc) > 0:
        df_address_oeb = pd.merge(df_address_oeb, df_oeb_bodacc, on="name_source", how="left")

        df_address_oeb.loc[:, "housenumber2"] = ""
        df_address_oeb.loc[:, "nbr_min"] = 0
        df_address_oeb.loc[:, "nbr_max"] = 0
        for i, r in df_address_oeb.loc[df_address_oeb["housenumber"].notna()].iterrows():
            chaine = r.housenumber
            ch = split_hs(chaine)
            df_address_oeb.at[i, "housenumber2"] = ch
            if ch:
                mini = int(min(ch))
                maxi = int(max(ch)) + 1
                df_address_oeb.at[i, "nbr_min"] = mini
                df_address_oeb.at[i, "nbr_max"] = maxi

        df_address_oeb["numeroVoie2"] = ""
        df_address_oeb["no_min"] = 0
        df_address_oeb["no_max"] = 0
        for i, r in df_address_oeb.loc[
            df_address_oeb["personne.adresseSiegeSocial.numeroVoie"].notna()].iterrows():
            chaine2 = str(r["personne.adresseSiegeSocial.numeroVoie"])
            ch2 = split_hs(chaine2)
            df_address_oeb.at[i, "numeroVoie2"] = ch2
            if ch2 != []:
                mini2 = int(min(ch2))
                maxi2 = int(max(ch2)) + 1
                df_address_oeb.at[i, "no_min"] = mini2
                df_address_oeb.at[i, "no_max"] = maxi2

        df_address_oeb = df_address_oeb.astype({"nbr_min": pd.Int64Dtype(), "nbr_max": pd.Int64Dtype(),
                                                "no_min": pd.Int64Dtype(), "no_max": pd.Int64Dtype()})

        df_address_oeb.loc[df_address_oeb["housenumber2"].notna(), "nbr_et"] = df_address_oeb.loc[
            df_address_oeb["housenumber2"].notna(), ["nbr_min", "nbr_max"]].apply(
            lambda a: [x for x in range(a.nbr_min, a.nbr_max) if x % 2 == 0] if a.nbr_min % 2 == 0 else [x
                                                                                                         for
                                                                                                         x
                                                                                                         in
                                                                                                         range(
                                                                                                             a.nbr_min,
                                                                                                             a.nbr_max)
                                                                                                         if
                                                                                                         x % 2 > 0],
            axis=1)

        df_address_oeb.loc[df_address_oeb["numeroVoie2"].notna(), "no_et"] = df_address_oeb.loc[
            df_address_oeb["numeroVoie2"].notna(), ["no_min", "no_max"]].apply(
            lambda a: [x for x in range(a.no_min, a.no_max) if x % 2 == 0] if a.no_min % 2 == 0 else [x for
                                                                                                      x in
                                                                                                      range(
                                                                                                          a.no_min,
                                                                                                          a.no_max)
                                                                                                      if
                                                                                                      x % 2 > 0],
            axis=1)

        df_address_oeb.loc[df_address_oeb["housenumber2"].notna(), "len_nr"] = df_address_oeb.loc[
            df_address_oeb["housenumber2"].notna(), "housenumber2"].apply(
            lambda a: len(a))
        df_address_oeb.loc[df_address_oeb["numeroVoie2"].notna(), "len_no"] = df_address_oeb.loc[
            df_address_oeb["numeroVoie2"].notna(), "numeroVoie2"].apply(
            lambda a: len(a))

        df_address_oeb = df_address_oeb.astype({"len_nr": pd.Int64Dtype(), "len_no": pd.Int64Dtype()})

        df_address_oeb.loc[(df_address_oeb["len_nr"] == 1) & (df_address_oeb["len_no"] == 1), "ep"] = \
            df_address_oeb.loc[(df_address_oeb["len_nr"] == 1) & (df_address_oeb["len_no"] == 1), "housenumber"] == \
            df_address_oeb.loc[(df_address_oeb["len_nr"] == 1) & (df_address_oeb["len_no"] == 1),
            "personne.adresseSiegeSocial.numeroVoie"]

        df_address_oeb.loc[(df_address_oeb["len_nr"] == 1) & (df_address_oeb["len_no"] > 1), "ep"] = \
            df_address_oeb.loc[
                (df_address_oeb["len_nr"] == 1) & (
                        df_address_oeb["len_no"] > 1)].apply(
                lambda a: a["housenumber2"][0] in a["numeroVoie2"],
                axis=1)

        df_address_oeb.loc[(df_address_oeb["len_nr"] == 1) & (df_address_oeb["len_no"] > 1), "ep"] = \
            df_address_oeb.loc[
                (df_address_oeb["len_nr"] == 1) & (
                        df_address_oeb["len_no"] > 1)].apply(
                lambda a: a["housenumber2"][0] in a["numeroVoie2"],
                axis=1)

        df_address_oeb.loc[(df_address_oeb["len_nr"] > 1) & (df_address_oeb["len_no"] == 1), "ep"] = \
            df_address_oeb.loc[
                (df_address_oeb["len_nr"] > 1) & (df_address_oeb["len_no"] == 1)].apply(
                lambda a: a["numeroVoie2"][0] in a["housenumber2"], axis=1)

        df_address_oeb = df_address_oeb.loc[df_address_oeb["person_id"].notna()]

        df_address_oeb = df_address_oeb.drop(
            columns=["housenumber2", "numeroVoie2", "nbr_min", "nbr_max", "no_min", "no_max", "nbr_et",
                     "no_et", "len_nr", "len_no"])

        df_address_oeb.loc[:, "ep2"] = df_address_oeb.loc[:, "ep"].apply(lambda a: 1 if a is True else 0)

        df_address_oeb = df_address_oeb.loc[df_address_oeb["ep2"] == 1].drop(columns="ep")

        df_address_oeb["name2"] = df_address_oeb["name_source"].apply(lambda a: a.lower())
        df_address_oeb["distance"] = df_address_oeb.apply(
            lambda a: lev.distance(a["name2"], a["personne.denomination"]),
            axis=1)
        dmin = min(df_address_oeb["distance"])
        df_address_oeb = df_address_oeb.loc[df_address_oeb["distance"] == dmin]
        df_address_oeb = df_address_oeb.drop(columns=["distance", "name2"])
        df_address_oeb = df_address_oeb.loc[df_address_oeb["person_id"].notna()]
        df_address_oeb = df_address_oeb.loc[
            df_address_oeb["personne.numeroImmatriculation.numeroIdentification"].notna()]
        df_address_oeb = df_address_oeb.rename(columns={"personne.numeroImmatriculation.numeroIdentification": "siren"})
        df_address_oeb = df_address_oeb.astype({"applt_seq_nr": int})
        df_address_oeb = df_address_oeb.drop(columns="address_2").drop_duplicates()
        df_address_oeb2 = df_address_oeb[com_col].drop_duplicates()

    ###################################################################################################################
    # concatenate all the df with SIREN we already have
    if "nepfr_address3" in locals():
        nepfr_address3 = pd.merge(noms[["key_appln_nr_person", "address_source"]], nepfr_address3,
                                  on="key_appln_nr_person",
                                  how="inner")

    if "nepfr_address3" in locals():
        if "df_address_oeb2" in locals():
            ok = pd.concat(
                [fr2.loc[fr2["siren"].notna()], publn_bodacc3, nepfr_address3, df_famille_fr_ok2, df_address_oeb2])
        else:
            ok = pd.concat(
                [fr2.loc[fr2["siren"].notna()], publn_bodacc3, nepfr_address3, df_famille_fr_ok2])
    else:
        if "df_address_oeb2" in locals():
            ok = pd.concat([fr2.loc[fr2["siren"].notna()], publn_bodacc3, df_famille_fr_ok2, df_address_oeb2])
        else:
            ok = pd.concat([fr2.loc[fr2["siren"].notna()], publn_bodacc3, df_famille_fr_ok2])

    ok = ok.drop_duplicates()

    ## cases where key_appln_nr_person appears twice (2 and because 2 different applt_seq_nr)

    # check which SIREN are still missing
    reste = noms.loc[~noms["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    # find SIREN based on what the name_source and address_source with associated SIREN we already have
    # when single SIREN associated with a pair of name_source and address_source
    reste_name_ad = reste[["name_source", "address_source"]].drop_duplicates()
    reste_name_ad["id"] = reste_name_ad["name_source"] + str(reste_name_ad["address_source"])
    ok_ex = ok[["name_source", "address_source", "siren"]].drop_duplicates()
    ok_ex["id"] = ok_ex["name_source"] + str(ok_ex["address_source"])
    compte_ok_ex = ok_ex[["id", "siren"]].groupby("id").nunique().reset_index().rename(columns={"siren": "compte"})

    ok_ex = ok_ex.loc[ok_ex["id"].isin(compte_ok_ex.loc[compte_ok_ex["compte"] == 1, "id"])]
    ok_ex = ok_ex.drop(columns="id").drop_duplicates()
    reste_ok = pd.merge(reste_name_ad, ok_ex, on=["name_source", "address_source"], how="inner").drop(
        columns="id").drop_duplicates()
    reste_ok2 = reste.copy()
    reste_ok2 = reste_ok2.drop(columns="siren").drop_duplicates()
    reste_ok2 = pd.merge(reste_ok2, reste_ok, on=["name_source", "address_source"], how="inner")

    # new df with correct SIREN
    ok = pd.concat([ok, reste_ok2])
    ok = ok.drop_duplicates()

    # check which SIREN are still missing
    reste2 = noms.loc[~noms["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    ###################################################################################################################
    reste2_address = reste2.rename(columns={"address_source": "address"})
    reste2_address = reste2_address.drop(columns="siren")

    reste2_address["address" + "_2"] = reste2_address["address"].fillna("").apply(
        lambda a: a.replace(",", "").split(" "))
    reste2_address["cp"] = reste2_address["address_2"].apply(lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b)))
    reste2_address["cp"] = reste2_address["cp"].apply(lambda a: [item[0] for item in a if item])
    reste2_address["cp"] = reste2_address["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")
    reste2_address["dep"] = reste2_address["cp"].apply(lambda a: a[0:2])

    print("Start requests API BODACC reste2", flush=True)
    sub_reste2_address = subset_df(reste2_address)
    df_bodacc_reste2 = res_futures(sub_reste2_address, bodacc)
    print("End requests API BODACC reste2", flush=True)
    df_bodacc_reste2 = df_bodacc_reste2.rename(columns={"personne.numeroImmatriculation.numeroIdentification": "siren"})
    df_bodacc_reste2 = df_bodacc_reste2.loc[df_bodacc_reste2["siren"].notna()]
    df_bodacc_reste2 = df_bodacc_reste2.loc[df_bodacc_reste2["personne.denomination"].notna()]

    if len(df_bodacc_reste2) > 0:
        reste2_address = pd.merge(reste2_address, df_bodacc_reste2, on="name_source", how="inner")

    reste2_address["name2"] = reste2_address["name_source"].apply(lambda a: a.lower())
    reste2_address["distance"] = reste2_address.apply(lambda a: lev.distance(a["name2"], a["personne.denomination"]),
                                                      axis=1)

    reste2_address_ok = reste2_address.loc[reste2_address["distance"] == 0].drop(
        columns=['address_2',
                 'personne.adresseSiegeSocial.codePostal',
                 'personne.adresseSiegeSocial.numeroVoie',
                 'personne.adresseSiegeSocial.localite']).drop_duplicates()

    for item in ["personne.adresseEtablissementPrincipal.complGeographique",
                 "personne.adresseEtablissementPrincipal.localite", "personne.nonInscrit", "personne.enseigne",
                 "personne.inscriptionRM.numeroDepartement", "personne.inscriptionRM.codeRM",
                 "personne.inscriptionRM.numeroIdentificationRM", "personne", "personne.adresseSiegeSocial.BP",
                 "personne.nomUsage", "personne.adressePP.complGeographique", "personne.adressePP.localite",
                 "personne.adressePP.BP"]:
        if item in reste2_address_ok.columns:
            reste2_address_ok = reste2_address_ok.drop(columns=[item]).drop_duplicates()


    compte_reste2 = reste2_address_ok[["key_appln_nr_person", "siren"]].groupby(
        "key_appln_nr_person").nunique().reset_index().rename(columns={"siren": "compte"})

    reste2_address_ok.loc[reste2_address_ok["key_appln_nr_person"].isin(
        compte_reste2.loc[compte_reste2["compte"] > 1, "key_appln_nr_person"]), "siren"] = np.nan
    reste2_address_ok = reste2_address_ok.loc[reste2_address_ok["siren"].notna()]
    reste2_address_ok = reste2_address_ok.rename(columns={"address": "address_source"})
    reste2_address_ok = reste2_address_ok[reste2.columns].drop_duplicates()

    ok = pd.concat([ok, reste2_address_ok])

    ok_compte = ok[["key_appln_nr_person", "siren"]].groupby(
        "key_appln_nr_person").nunique().reset_index().rename(columns={"siren": "compte"})

    ok.loc[ok["key_appln_nr_person"].isin(
        ok_compte.loc[ok_compte["compte"] > 1, "key_appln_nr_person"]), "siren"] = np.nan
    ok = ok.loc[ok["siren"].notna()]

    reste2_reste = reste2.loc[~reste2["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    ok_test = ok.copy()
    ok_test["id"] = ok_test["name_source"] + str(ok_test["address_source"])
    reste2_reste_test = reste2_reste.copy()
    reste2_reste_test["id"] = reste2_reste_test["name_source"] + str(reste2_reste_test["address_source"])
    test = pd.merge(reste2_reste_test[["id", "key_appln_nr_person"]], ok_test[["id", "siren"]], on="id", how="inner")
    if len(test) > 0:
        reste2_reste_ok = reste2_reste.loc[reste2_reste["key_appln_nr_person"].isin(test["key_appln_nr_person"])]
        reste2_reste_ok = reste2_reste_ok.drop(columns="siren")
        reste2_reste_ok = pd.merge(reste2_reste_ok, test[["key_appln_nr_person", "siren"]], on="key_appln_nr_person",
                                   how="inner")

        ok = pd.concat([ok, reste2_reste_ok])

        ok_compte2 = ok[["key_appln_nr_person", "siren"]].groupby(
            "key_appln_nr_person").nunique().reset_index().rename(columns={"siren": "compte"})

        ok.loc[ok["key_appln_nr_person"].isin(
            ok_compte2.loc[ok_compte2["compte"] > 1, "key_appln_nr_person"]), "siren"] = np.nan
        ok = ok.loc[ok["siren"].notna()]

    reste3 = reste2.loc[~reste2["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    reste3["name_source3"] = reste3["name_source"].apply(lambda a: a.encode("ascii", "ignore").decode().strip())

    noms_siren = part_entp_final.copy()
    noms_siren = noms_siren.loc[noms_siren["country_corrected"] == "FR"]
    noms_siren.loc[(noms_siren["siren_psn"].notna()) & (noms_siren["siren"].isna()), "siren"] = noms_siren.loc[
        (noms_siren["siren_psn"].notna()) & (noms_siren["siren"].isna()), "siren_psn"]
    noms_siren = noms_siren.loc[noms_siren["siren"].notna()]
    noms_siren2 = noms_siren[["name_source", "address_source", "siren"]].drop_duplicates()

    noms_siren2["id"] = noms_siren2["name_source"].astype(str) + noms_siren2["address_source"].astype(str)
    compte_siren2 = noms_siren2[["id", "siren"]].groupby(
        "id").nunique().reset_index().rename(columns={"siren": "compte"})

    noms_siren2.loc[noms_siren2["id"].isin(
        compte_siren2.loc[compte_siren2["compte"] > 1, "id"]), "siren"] = np.nan
    noms_siren2 = noms_siren2.loc[noms_siren2["siren"].notna()]

    reste3_2 = reste3.copy()
    reste3_2 = reste3_2.drop(columns="siren")
    reste3_2["id"] = reste3_2["name_source"].astype(str) + reste3_2["address_source"].astype(str)
    reste3_2 = pd.merge(reste3_2[["name_source", "address_source", "key_appln_nr_person", "id"]],
                        noms_siren2[["id", "siren"]], on="id", how="inner")

    reste3_reste_ok = reste3.loc[reste3["key_appln_nr_person"].isin(reste3_2["key_appln_nr_person"])]
    reste3_reste_ok = reste3_reste_ok.drop(columns="siren")
    reste3_reste_ok = pd.merge(reste3_reste_ok, reste3_2[["key_appln_nr_person", "siren"]], on="key_appln_nr_person",
                               how="inner")

    reste3_reste_ok = reste3_reste_ok.drop(columns="name_source3")

    ok = pd.concat([ok, reste3_reste_ok])

    compte_ok = ok[["key_appln_nr_person", "siren"]].groupby("key_appln_nr_person").nunique().reset_index().rename(
        columns={"siren": "compte"})

    ok.loc[ok["key_appln_nr_person"].isin(
        compte_ok.loc[compte_ok["compte"] > 1, "key_appln_nr_person"]), "siren"] = np.nan
    ok = ok.loc[ok["siren"].notna()]

    reste4 = reste3.loc[~reste3["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    noms_siren3 = noms_siren[["name_source", "siren"]].drop_duplicates()
    compte_siren3 = noms_siren3.groupby("name_source").nunique().reset_index()
    compte_siren3 = compte_siren3.loc[compte_siren3["siren"] == 1]
    noms_siren3 = noms_siren3.loc[noms_siren3["name_source"].isin(compte_siren3["name_source"])]

    reste4_ok = pd.merge(reste4[["name_source", "key_appln_nr_person", "address_source"]],
                         noms_siren3[["name_source", "siren"]], on="name_source", how="inner")

    ok = pd.concat([ok, reste4_ok])

    reste5 = reste4.loc[reste4["name_source3"] != ""]
    # ScanR
    reste5["name_propre"] = reste5["name_source"].copy()
    reste5["name_propre"] = reste5["name_propre"].apply(lambda a: a.replace("ï¿½", ""))
    reste5["name_propre"] = reste5["name_propre"].apply(
        lambda a: "".join([letter for letter in a if letter.isprintable()]))

    dico_l = {}
    for i, r in reste5.iterrows():
        chaine = r.name_propre
        try:
            cod = chaine.encode("latin-1").decode()
            if chaine != cod:
                dico_l[chaine] = cod
        except:
            pass

    for key in dico_l.keys():
        reste5.loc[reste5["name_propre"] == key, "name_propre"] = dico_l[key]
    sub_structure = subset_df(reste5)

    futures_stru = res_futures(sub_structure, req_scanr_stru)
    futures_stru.to_csv("futures_stru.csv", sep="|", encoding="utf-8", index=False)

    futures_stru["name_propre2"] = futures_stru["name_propre"].apply(lambda a: unidecode(a).lower())
    futures_stru["label_default2"] = futures_stru["label_default"].apply(lambda a: unidecode(a).lower())

    futures_stru["distance"] = futures_stru.apply(lambda a: lev.distance(a["name_propre2"], a["label_default2"]),
                                                  axis=1)

    liste_scanr = []
    liste_scanr_ad = []
    for key in set(futures_stru["key_appln_nr_person"]):
        minimum = futures_stru.loc[futures_stru["key_appln_nr_person"] == key, "distance"].min()
        tmp_scanr = futures_stru.loc[
            (futures_stru["key_appln_nr_person"] == key) & (futures_stru["distance"] == minimum)]
        liste_scanr.append(tmp_scanr)

    df_scanr = pd.concat(liste_scanr)

    dict_ids = {"key_appln_nr_person": [], "sirene": [], "grid": [], "idref": [], "siret": []}

    for _, r in df_scanr.iterrows():
        ids = r.externalIds
        if ids is not None:
            dict_ids["key_appln_nr_person"].append(r.key_appln_nr_person)
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

            keys = [k for k in list(lab_id2.keys()) if k in ["sirene", "grid", "idref", "siret"]]
            not_keys = list({"sirene", "grid", "idref", "siret"} - set(keys))

            for k in [id.get("type") for id in ids]:
                if k not in keys:
                    for item in ids:
                        if item["type"] == k:
                            ids.remove(item)

            for item in ids:
                typ = item.get("type")
                id = item.get("id")
                dict_ids[typ].append(id)

            for typ in not_keys:
                dict_ids[typ].append("")

        ad = r.address_scanr
        if ad is not None:
            if ad != "":
                tmp = pd.DataFrame(data=ad)
                tmp["key_appln_nr_person"] = r.key_appln_nr_person
                liste_scanr_ad.append(tmp)

    df_ids = pd.DataFrame(data=dict_ids)
    df_ids = df_ids.drop_duplicates()
    df_scanr_ad = pd.concat(liste_scanr_ad)
    df_scanr_ad = df_scanr_ad[["key_appln_nr_person", "address", "postcode", "city"]]
    df_scanr_ad = df_scanr_ad.drop_duplicates()

    df_scanr2 = pd.merge(df_scanr.drop(columns=["externalIds", "address_scanr"]), df_ids, on="key_appln_nr_person",
                         how="inner")
    df_scanr2 = df_scanr2.drop_duplicates()
    df_scanr2_ad = pd.merge(df_scanr2, df_scanr_ad, on="key_appln_nr_person", how="inner")
    df_scanr2_ad = df_scanr2_ad.drop_duplicates()
    liste = []
    for key in set(df_scanr2_ad["key_appln_nr_person"]):
        if len(df_scanr2_ad.loc[df_scanr2_ad["key_appln_nr_person"] == key]) > 1:
            df = df_scanr2_ad.loc[df_scanr2_ad["key_appln_nr_person"] == key]
            liste.append(df)

    df_scanr_0 = df_scanr2_ad.loc[df_scanr2_ad["distance"] == 0]
    df_scanr_0 = df_scanr_0.drop(columns=["address", "postcode", "city"]).drop_duplicates()

    reste5_ok = pd.merge(reste5.drop(columns=["siren", "siret", "grid", "idref"]),
                         df_scanr_0[["key_appln_nr_person", "sirene", "siret", "grid", "idref"]],
                         on="key_appln_nr_person", how="inner").rename(columns={"sirene": "siren"}).drop_duplicates()
    reste5_ok = reste5_ok.drop(columns="name_source3")
    ok = pd.concat([ok, reste5_ok])

    ok = ok.loc[ok["person_id"].notna()]

    df_scanr_sup = df_scanr2_ad.loc[df_scanr2_ad["distance"] > 0]

    df_scanr_sup["address_scanr"] = df_scanr_sup["address"] + " " + df_scanr_sup["postcode"] + " " + df_scanr_sup[
        "city"]

    df_scanr_sup["address_2"] = df_scanr_sup["address_source"].apply(lambda a: a.replace(",", "").split(" "))
    df_scanr_sup["cp"] = df_scanr_sup["address_2"].apply(lambda b: list(map(lambda a: re.findall(r"\d{5}", a), b)))
    df_scanr_sup["cp"] = df_scanr_sup["cp"].apply(lambda a: [item[0] for item in a if item])
    df_scanr_sup["cp"] = df_scanr_sup["cp"].apply(lambda a: a if len(a) > 0 else "").apply(
        lambda a: a[0] if len(a) == 1 else "")

    cp = df_scanr_sup.loc[(df_scanr_sup["cp"] != "") & (df_scanr_sup["cp"] == df_scanr_sup["postcode"])]

    cp = cp[list(df_scanr_0.columns)]

    reste5_cp_ok = pd.merge(reste5.drop(columns=["siren", "siret", "grid", "idref"]),
                            cp[["key_appln_nr_person", "sirene", "siret", "grid", "idref"]],
                            on="key_appln_nr_person", how="inner").rename(columns={"sirene": "siren"}).drop_duplicates()

    ok = pd.concat([ok, reste5_cp_ok])

    ok = ok.loc[ok["person_id"].notna()]

    ok = ok.drop(columns=["name_propre", "name_source3"])

    reste6 = reste5.loc[~reste5["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    ok5 = pd.concat([cp, df_scanr_0])

    autres_noms_j = ok5.loc[ok5["name_source"].isin(reste6["name_source"])]
    autres_noms_j = autres_noms_j[["name_source", "sirene", "grid", "idref", "siret"]]
    reste6_ok = pd.merge(reste6.drop(columns=["siren", "grid", "idref", "siret"]), autres_noms_j, on="name_source",
                         how="inner").rename(columns={"sirene": "siren"}).drop_duplicates()

    reste6_ok = reste6_ok[list(ok.columns)]
    ok = pd.concat([ok, reste6_ok])

    reste7 = reste6.loc[~reste6["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    name_address = reste7[["name_source", "name_propre", "address_source"]].drop_duplicates()

    ###################################################################################################################
    # INSEE
    res = requests.post("https://api.insee.fr/token",
                        data="grant_type=client_credentials",
                        auth=(os.getenv("SIRENE_API_KEY"), os.getenv("SIRENE_API_SECRET")))

    liste_etab = []

    token = res.json()['access_token']
    api_call_headers = {'Authorization': 'Bearer ' + token}

    for _, r in name_address.iterrows():
        url = f'https://api.insee.fr/entreprises/sirene/V3.11/siret?q=denominationUniteLegale:"{r.name_propre}"'
        rinit = requests.get(url, headers=api_call_headers, verify=False)
        if rinit.status_code == 200:
            rep = pd.json_normalize(rinit.json()["etablissements"])
            rep = rep[["siren", "uniteLegale.denominationUniteLegale"]].drop_duplicates()
            rep["name_source"] = r.name_source
            rep["name_propre"] = r.name_propre
            rep["address_source"] = r.address_source
            liste_etab.append(rep)

    etab = pd.concat(liste_etab)
    compte_etab = etab[["siren", "uniteLegale.denominationUniteLegale"]].groupby(
        "uniteLegale.denominationUniteLegale").nunique().reset_index()
    compte_etab = compte_etab.rename(columns={"siren": "compte"})

    etab = pd.merge(etab, compte_etab, on="uniteLegale.denominationUniteLegale", how="left")

    etab_single = etab.loc[etab["compte"] == 1,
    ["siren", "uniteLegale.denominationUniteLegale", "name_source", "name_propre",
     "address_source"]].drop_duplicates().rename(columns={
        "uniteLegale.denominationUniteLegale": "name_insee"})
    etab_single_c = etab_single[["name_insee",
                                 "name_source"]].groupby("name_source").nunique().reset_index().rename(
        columns={"name_insee": "compte"})
    etab_single = pd.merge(etab_single, etab_single_c, on="name_source", how="left")
    etab_single["name_source2"] = etab_single["name_source"].apply(lambda a: unidecode(a).lower()).replace(r"\s|-|'",
                                                                                                           "",
                                                                                                           regex=True)
    etab_single["name_insee2"] = etab_single["name_insee"].apply(lambda a: unidecode(a).lower()).replace(r"\s|-|'", "",
                                                                                                         regex=True)
    etab_single["distance"] = etab_single.apply(
        lambda a: lev.distance(a["name_source2"], a["name_insee2"]),
        axis=1)

    etab_single_0 = etab_single.loc[etab_single["distance"] == 0]
    etab_single_0_c = etab_single_0[["name_source", "siren"]].groupby("name_source").nunique().reset_index().rename(
        columns={"siren": "compte"})

    etab_single_0 = etab_single_0.loc[
        etab_single_0["name_source"].isin(etab_single_0_c.loc[etab_single_0_c["compte"] == 1, "name_source"])]

    reste7_ok = pd.merge(reste7.drop(columns="siren"),
                         etab_single_0.drop(columns=["compte", "name_source2", "name_insee2", "distance",
                                                     "name_insee"]),
                         on=["name_source", "name_propre", "address_source"])

    compte = reste7_ok[["key_appln_nr_person", "siren"]].groupby("key_appln_nr_person").nunique().reset_index().rename(
        columns={"siren": "compte"})

    reste7_ok = reste7_ok.loc[
        reste7_ok["key_appln_nr_person"].isin(compte.loc[compte["compte"] == 1, "key_appln_nr_person"])]

    ok = pd.concat([ok, reste7_ok])

    ok = ok.loc[ok["person_id"].notna()]

    reste8 = reste7.loc[~reste7["key_appln_nr_person"].isin(ok["key_appln_nr_person"])]

    autres_noms_j = ok.loc[ok["name_source"].isin(reste8["name_source"])]
    autres_noms_j = autres_noms_j[["name_source", "siren"]]
    reste8_ok = pd.merge(reste8.drop(columns="siren"), autres_noms_j, on="name_source",
                         how="inner").drop_duplicates()

    ok = pd.concat([ok, reste8_ok])

    ok = ok.loc[ok["person_id"].notna()]

    compte = ok[["key_appln_nr_person", "siren"]].groupby("key_appln_nr_person").nunique().reset_index().rename(
        columns={"siren": "compte"})

    ok = ok.loc[ok["key_appln_nr_person"].isin(compte.loc[compte["compte"]==1, "key_appln_nr_person"])]
    ok = ok.drop(columns=["name_source3", "name_propre", "applt_seq_nr"]).drop_duplicates()

    ok_merge = ok[["key_appln_nr_person", "siren", "siret", "grid", "idref"]].drop_duplicates()

    ok_merge = ok_merge.rename(columns={"siren": "siren2", "siret": "siret2", "grid": "grid2", "idref": "idref2"})

    part_entp_final2 = pd.merge(part_entp_final, ok_merge, on="key_appln_nr_person", how="left")
    for col in ["siren", "siret", "grid", "idref"]:
        col2 = col + "2"
        part_entp_final2.loc[(part_entp_final2[col].isna()) & (part_entp_final2[col2].notna()),
        col] = part_entp_final2.loc[(part_entp_final2[col].isna()) & (part_entp_final2[col2].notna()),
        col2]

    part_entp_final2.loc[(part_entp_final2["siren_psn"].notna()) & (part_entp_final2["siren"].isna()), "siren"] = \
    part_entp_final2.loc[
        (part_entp_final2["siren_psn"].notna()) & (part_entp_final2["siren"].isna()), "siren_psn"]

    part_entp_final2 = part_entp_final2.drop(columns=["siren2", "siret2", "grid2", "idref2"])
    part_entp_final2 = part_entp_final2.rename(columns={"name_part": "name"})

    part_entp_final2.to_csv("part_entp_final2.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'part_entp_final2.csv')






