import requests
import json
import time
import os
from pathlib import Path
import pandas as pd
from utils import swift
from retry import retry
from collections import Counter

from patstat import dtypes_patstat_declaration as types

CACHE_FILE = "/data/openalex_cache.json"
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")
PAYSAGE_API_DUMP = os.getenv("PAYSAGE_API_DUMP")
openalex_cache = {}


def fetch_openalex(dois: list, reset_cache: False):
    cache = {}
    dois = list(set(dois))
    if not reset_cache and Path(CACHE_FILE).exists():
        with open(CACHE_FILE) as f:
            cache = json.load(f)

    missing = [doi for doi in dois if doi not in cache]
    print(missing)

    for i in range(0, len(missing), 50):
        batch = missing[i:i + 50]
        filter_str = "|".join(batch)

        for attempt in range(3):
            try:
                print(f"fetching 50 DOIs")
                r = requests.get(
                    "https://api.openalex.org/works",
                    params={"filter": f"doi:{filter_str}", "per-page": 50, "api_key": OPENALEX_API_KEY},
                    timeout=30
                )
                r.raise_for_status()
                for work in r.json().get("results", []):
                    doi = work.get("doi", "").replace("https://doi.org/", "")
                    cache[doi] = work
                break
            except Exception as e:
                print(f"Attempt {attempt + 1}/3 failed: {e}")
                time.sleep(2 ** attempt)  # backoff exponentiel : 1s, 2s, 4s

        time.sleep(0.1)

    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)
    return cache


def get_info_openalex(doi):
    global openalex_cache
    if len(openalex_cache) == 0:
        openalex_cache = json.load(open(CACHE_FILE))
        print(f"{len(openalex_cache)} DOIs loaded from OpenAlex cache")
    if doi.lower() in openalex_cache:
        return openalex_cache[doi.lower()]
    else:
        print(f"{doi} not in OpenAlex cache")
    return None


@retry(tries=3, delay=5, backoff=5)
def ids_paysage():
    rres_data = []

    res = requests.get(
        "https://api.paysage.dataesr.ovh/dump/structures",
        headers={"Content-Type": "application/json", "X-API-KEY": PAYSAGE_API_DUMP})

    with open("dump_paysage.txt", "w") as f:
        f.write(res.text)

    with open("dump_paysage.txt", "r") as f:
        dump_paysage = f.readlines()

    dump_paysage = [json.loads(item) for item in dump_paysage]

    for etab in dump_paysage:
        dict_res = {}
        chcols = "name_corrected|name_corrige|id_paysage|category_libelle|esri|siren|siret|grid|ror|idref"
        chvals = ""
        current = etab.get("currentName")
        name = current.get("usualName")
        chvals = chvals + f"{name}|{name}|"
        res_data = etab.get("category")
        dict_res["id_paysage"] = etab.get("id")
        pid = etab.get("id")
        chvals = chvals + f"{pid}|"
        dict_res["usualName"] = name
        if len(res_data) > 0:
            cat = res_data.get("usualNameFr")
            if len(cat) > 0:
                print(cat)
                dict_res["category"] = cat
                if "étrang" in cat:
                    print("étranger")
                    dict_res["category_libelle"] = "Personne morale"
                    dict_res["esri"] = "AUTRE"
                    chvals = chvals + "Personne morale|AUTRE|"
                elif cat in ["Petite & moyenne entreprise (SIRENE)", "Start-Up", "Entreprise", "Entreprise (ROR)",
                             "Organisation professionnelle"]:
                    print("Personne morale")
                    dict_res["category_libelle"] = "Personne morale"
                    dict_res["esri"] = "AUTRE"
                    chvals = chvals + "Personne morale|AUTRE|"
                elif cat in ["Grand établissement relevant d'un autre département ministériel",
                             'Institut ou école extérieur aux universités', 'Organisme de recherche',
                             'Institut pour la transition énergétique', 'Centre de lutte contre le cancer',
                             'Université', 'Centre hospitalier', "Communauté d'universités et établissements",
                             'Grand établissement']:
                    print("ESRI")
                    dict_res["category_libelle"] = cat
                    dict_res["esri"] = "ESRI"
                    chvals = chvals + f"{cat}|ESRI|"
                else:
                    print("Autre")
                    dict_res["category_libelle"] = ""
                    dict_res["esri"] = ""
                    chvals = chvals + "||"
            identifiers = etab.get("identifiers")
            types = [item.get("type") for item in identifiers]
            decompte = Counter(types)
            if "siret" in types:
                if decompte["siret"] > 1:
                    for item in identifiers:
                        if pid == "9ZF2M":
                            siret = "19341087500010"
                            siren = "193410875"
                            dict_res["siren"] = siren
                            dict_res["siret"] = siret
                            chvals = chvals + f"{siren}|{siret}|"
                        elif item.get("type") == "siret":
                            if item.get("active") == True:
                                siret = item.get("value")
                                siren = siret[0:9]
                                dict_res["siren"] = siren
                                dict_res["siret"] = siret
                                chvals = chvals + f"{siren}|{siret}|"
                elif decompte["siret"] == 1:
                    for item in identifiers:
                        if item.get("type") == "siret":
                            siret = item.get("value")
                            siren = siret[0:9]
                            dict_res["siren"] = siren
                            dict_res["siret"] = siret
                            chvals = chvals + f"{siren}|{siret}|"
            else:
                dict_res["siren"] = ""
                dict_res["siret"] = ""
                chvals = chvals + "||"
            if "grid" in types:
                if decompte["grid"] > 1:
                    for item in identifiers:
                        if item.get("type") == "grid":
                            if item.get("active") == True:
                                grid = item.get("value")
                                dict_res["grid"] = grid
                                chvals = chvals + f"{grid}|"
                elif decompte["grid"] == 1:
                    for item in identifiers:
                        if item.get("type") == "grid":
                            grid = item.get("value")
                            dict_res["grid"] = grid
                            chvals = chvals + f"{grid}|"
            else:
                dict_res["grid"] = ""
                chvals = chvals + "|"
            if "ror" in types:
                for item in identifiers:
                    if item.get("type") == "ror":
                        ror = item.get("value")
                        dict_res["ror"] = ror
                        chvals = chvals + f"{ror}|"
            else:
                dict_res["ror"] = ""
                chvals = chvals + "|"
            if "idref" in types:
                for item in identifiers:
                    if item.get("type") == "idref":
                        idref = item.get("value")
                        idref = "idref" + idref
                        dict_res["idref"] = idref
                        chvals = chvals + idref
            else:
                dict_res["idref"] = ""
            dict_res["cols"] = chcols
            dict_res["vals"] = chvals
        rres_data.append(dict_res)

    df_res = pd.DataFrame(rres_data)
    df_res = df_res.drop_duplicates().reset_index(drop=True)
    df_res = df_res.loc[df_res["category"].notna()]
    df_res = df_res.loc[df_res["category"] != ""]
    df_res = df_res.loc[df_res["ror"].notna()]
    df_res = df_res.loc[df_res["ror"] != ""]
    compte_id_paysage = df_res[["ror", "id_paysage"]].groupby("ror").nunique(dropna=False).reset_index()
    df_res = df_res.loc[df_res["ror"].isin(compte_id_paysage.loc[compte_id_paysage["id_paysage"] == 1, "ror"])]

    return df_res


def get_infos_from_oa():
    os.chdir(DATA_PATH)
    publi = pd.read_csv("publi_brevets.csv", sep="|", encoding="utf-8", engine="python",
                            dtype=types.tls214_types)

    dois_uniques = list(publi["doi"].unique())
    dois_uniques2 = list(publi["doi_clean"].unique())

    res_cache = fetch_openalex(dois_uniques, False)

    authors = []
    for doi in dois_uniques2:
        if doi in res_cache:
            res = res_cache[doi]
        for l in range(len(res["authorships"])):
            dic = res["authorships"][l]["author"]
            print("doi")
            dic["doi"] = res["doi"]
            print("display_name_title")
            dic["display_name_title"] = res["display_name"]
            print("title")
            dic["title"] = res["title"]
            dic["id_pub"] = res["id"]
            dic["language"] = res["language"]
            dic["type"] = res["type"]
            dic["is_retracted"] = res["is_retracted"]
            dic["publication_year"] = res["publication_year"]
            dic["publication_date"] = res["publication_date"]
            dic["is_oa"] = res["open_access"]["is_oa"]
            dic["volume"] = res["biblio"]["volume"]
            dic["issue"] = res["biblio"]["issue"]
            dic["first_page"] = res["biblio"]["first_page"]
            dic["last_page"] = res["biblio"]["last_page"]
            dic["pdf_url"] = res["primary_location"]["pdf_url"]
            dic["raw_type"] = res["primary_location"]["raw_type"]
            dic["is_accepted"] = res["primary_location"]["is_accepted"]
            dic["is_published"] = res["primary_location"]["is_published"]
            print("raw_source_name")
            dic["raw_source_name"] = res["primary_location"]["raw_source_name"]
            if "source" in res["primary_location"]:
                if isinstance(res["primary_location"]["source"], dict):
                    print("id_source")
                    dic["id_source"] = res["primary_location"]["source"]["id"]
                    print("issn_l")
                    dic["issn_l"] = res["primary_location"]["source"]["issn_l"]
                    print("host_organization_name")
                    dic["host_organization_name"] = res["primary_location"]["source"][
                            "host_organization_name"]
            else:
                print("id_source None")
                dic["id_source"] = None
                print("issn_l None")
                dic["issn_l"] = None
                print("id_source None")
                dic["host_organization_name"] = None
            keys_ins = []
            if len(res["authorships"][l]["institutions"]) > 0:
                for li in range(len(res["authorships"][l]["institutions"])):
                    for key in res["authorships"][l]["institutions"][li].keys():
                        if key != "lineage":
                            key2 = key + "_ins"
                            keys_ins.append(key2)
                            dic[key2] = res["authorships"][l]["institutions"][li][key]
            for item in ["author", "institutions", "raw_affiliation_strings"]:
                if item in dic.keys():
                    del dic[item]
            authors.append(dic)

    data = pd.DataFrame(data=authors)
    data = data.drop_duplicates().reset_index(drop=True)

    data2 = pd.merge(publi[["key_appln_nr", "appln_id", "docdb_family_id", "pat_publn_id", "doi_clean", "doi"]],
                     data, on="doi", how="inner")
    data2 = data2.drop_duplicates().reset_index(drop=True)
    data2["publication_year"] = data2["publication_year"].astype(str)
    data_missing = publi.loc[~publi["doi"].isin(data2["doi"])]
    data_missing = data_missing.drop_duplicates().reset_index(drop=True)

    data_missing.loc[data_missing["npl_type"] == "b", "type"] = "book-citation"
    data_missing.loc[data_missing["npl_type"] == "s", "type"] = "serial/journal/periodical citation"
    data_missing.loc[data_missing["npl_type"] == "w", "type"] = "world wide web/internet search citation"
    data_missing.loc[data_missing["npl_publn_date"] == "20880630", "npl_publn_date"] = "20080601"
    data_missing.loc[data_missing["npl_publn_date"] == "10160618", "npl_publn_date"] = "20160624"
    data_missing.loc[data_missing["npl_publn_date"].notna(), "publication_year"] = data_missing.loc[
        data_missing["npl_publn_date"].notna(), "npl_publn_date"].apply(lambda a: a[0:4])
    data_missing.loc[data_missing["npl_publn_date"].notna(), "len_date"] = data_missing.loc[
        data_missing["npl_publn_date"].notna(), "npl_publn_date"].apply(lambda a: len(a))
    data_missing.loc[data_missing["len_date"] == 8, "npl_publn_date"] = data_missing.loc[
        data_missing["len_date"] == 8, "npl_publn_date"].apply(
        lambda a: a[0:4] if "0000" in a else a[0:4] + "-" + a[4:6] + "-" + a[6:8])
    data_missing.loc[data_missing["len_date"] == 6, "npl_publn_date"] = data_missing.loc[
        data_missing["len_date"] == 6, "npl_publn_date"].apply(lambda a: a[0:4] + "-" + a[4:6])

    data_missing = data_missing.rename(columns={"npl_title1": "display_name_title", "npl_title2": "raw_source_name",
                                                "npl_author": "display_name", "online_availability": "pdf_url",
                                                "npl_issn": "issn_l", "npl_publisher": "host_organization_name",
                                                "npl_publn_date": "publication_date", "npl_issue": "issue",
                                                "npl_volume": "volume", "npl_page_first": "first_page",
                                                "npl_page_last": "last_page"})
    data_missing = data_missing.drop(columns=["npl_publn_id", "xp_nr", "npl_type", "npl_biblio", "npl_editor",
                                              "npl_publn_end_date", "npl_abstract_nr", "npl_doi", "npl_isbn",
                                              "online_classification", "online_search_date", "len_date"])

    data_missing = data_missing.drop_duplicates().reset_index(drop=True)

    oa = pd.concat([data2, data_missing], ignore_index=True)

    df_paysage = ids_paysage()
    df_paysage = df_paysage.drop(columns=["category", "category_libelle", "esri", "cols", "vals"]).rename(
        columns={"idref": "idref_ins"})

    oa2 = pd.merge(oa, df_paysage, on="ror", how="left")

    oa2.to_csv("publi_oa.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'publi_oa.csv')

