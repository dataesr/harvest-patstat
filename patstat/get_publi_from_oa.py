import gzip
import json
import os
import re
import shutil
import time
import html
from collections import Counter
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import numpy as np
import requests
from retry import retry

from patstat import dtypes_patstat_declaration as types
from utils import swift

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
CACHE_FILE = "/data/openalex_cache.json"
PAYSAGE = os.getenv("PAYSAGE_API_DUMP")
OPENALEX_API_KEY = os.getenv("OPENALEX_API_KEY")
openalex_cache = {}

LATEX_SYMBOLS = {r"\alpha": "α", r"\beta": "β", r"\gamma": "γ", r"\delta": "δ", r"\epsilon": "ε", r"\theta": "θ",
                 r"\lambda": "λ", r"\mu": "μ", r"\pi": "π", r"\sigma": "σ", r"\phi": "φ", r"\omega": "ω",
                 r"\Delta": "Δ", r"\Sigma": "Σ", r"\int": "∫", r"\sum": "Σ", r"\sqrt": "√", r"\pm": "±"}

SUPERSCRIPTS = {"0": "⁰", "1": "¹", "2": "²", "3": "³", "4": "⁴", "5": "⁵", "6": "⁶", "7": "⁷", "8": "⁸", "9": "⁹",
                "+": "⁺", "-": "⁻", "=": "⁼", "(": "⁽", ")": "⁾", "n": "ⁿ", "i": "ⁱ"}

SUBSCRIPTS = {"0": "₀", "1": "₁", "2": "₂", "3": "₃", "4": "₄", "5": "₅", "6": "₆", "7": "₇", "8": "₈", "9": "₉",
              "+": "₊", "-": "₋", "=": "₌", "(": "₍", ")": "₎", "i": "ᵢ", "j": "ⱼ", "k": "ₖ", "n": "ₙ"}


def parse_latex(expr):
    # Recursive LaTeX parser to clean strings in publications variables
    if not isinstance(expr, str):
        return expr

    expr = expr.strip()

    # remove $...$
    expr = re.sub(r"\$(.*?)\$", r"\1", expr)

    # correction SigmaDelta
    expr = re.sub(r"SigmaDelta", "ΣΔ", expr)

    # symbols
    for k, v in LATEX_SYMBOLS.items():
        expr = expr.replace(k, v)

    def replace_frac(text: str) -> str:
        # fractions
        pattern = r"\\frac\{([^{}]+)\}\{([^{}]+)\}"
        while re.search(pattern, text):
            text = re.sub(pattern, lambda m: f"{parse_latex(m.group(1))}/{parse_latex(m.group(2))}", text)

        return text

    expr = replace_frac(expr)

    # squares
    expr = re.sub(r"√\{([^{}]+)\}", lambda m: f"√({parse_latex(m.group(1))})", expr)

    def sup_repl(match):
        # subscript
        content = parse_latex(match.group(1))

        return "".join(SUPERSCRIPTS.get(c, f"^{c}") for c in content)

    expr = re.sub(r"\^\{([^{}]+)\}", sup_repl, expr)
    expr = re.sub(r"\^(\w+)", lambda m: "".join(SUPERSCRIPTS.get(c, c) for c in m.group(1)), expr)

    def sub_repl(match):
        # subscripts
        content = parse_latex(match.group(1))

        return "".join(SUBSCRIPTS.get(c, f"_{c}") for c in content)

    expr = re.sub(r"_\{([^{}]+)\}", sub_repl, expr)
    expr = re.sub(r"_(\w+)", lambda m: "".join(SUBSCRIPTS.get(c, c) for c in m.group(1)), expr)

    # remove {}
    expr = expr.replace("{", "").replace("}", "")

    return expr


def replace_tex_blocks(text: str) -> str:
    # remove TeX blocks
    def repl(match):
        latex = match.group(1)
        parsed = parse_latex(latex)

        return f" {parsed} "  # keep spaces

    return re.sub(r"<tex[^>]*>(.*?)</tex>", repl, text)


def clean_text(value):
    # cleaning function that applies all the other cleaning functions
    if not isinstance(value, str):
        return value

    # HTML decode (multi-pass)
    prev = None
    while prev != value:
        prev = value
        value = html.unescape(value)

    # URL decode
    value = unquote(value)

    # TeX blocks
    value = replace_tex_blocks(value)

    # parse LaTeX global
    value = parse_latex(value)

    # remove invisible characters
    value = value.replace('\r', ' ')
    value = value.replace('\n', ' ')
    value = value.replace('\t', ' ')
    value = re.sub(r'[\x00-\x1F\x7F]', '', value)

    # remove multiple spaces
    value = re.sub(r"\s+", " ", value)

    return value.strip()


@retry(tries=3, delay=5, backoff=5)
def get_person_scanr() -> pd.DataFrame:
    # Query to get dump persons scanr
    url = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/persons_denormalized.jsonl.gz"
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open('persons_denormalized.jsonl.gz', "wb") as f:
            for chunk in r.iter_content(chunk_size=1000):
                if chunk:
                    f.write(chunk)
        with gzip.open("persons_denormalized.jsonl.gz", "rb") as f_in, open("persons_denormalized.jsonl",
                                                                            "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
        os.remove("persons_denormalized.jsonl.gz")
    except Exception as e:
        print(f"Attempt loading dump persons scanr failed: {e}")


@retry(tries=3, delay=5, backoff=5)
def fetch_openalex(dois: list, reset_cache: False) -> pd.DataFrame:
    """
    Query of OpenAlex to get info on publications and theirs authors based on DOIs
    If reset_cache is set to False, re-use results from previous queries else queries for all the DOIs
    Structure data from OpenAlex and create DataFrame

    """
    # get cached data from OpenAlex if reset_cache is set to False or cache file doesn't exist
    cache = {}
    dois = list(set(dois))
    if not reset_cache and Path(CACHE_FILE).exists():
        with open(CACHE_FILE) as f:
            cache = json.load(f)

    missing = [doi for doi in dois if doi not in cache]
    print(missing)

    # query only missing DOIs
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

    # write results query as a json file -> cache file
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f)

    # structuration data from OpenAlex
    authors = []
    for doi in cache:
        res = cache[doi]
        if "10.1039/x0xx00000x" not in doi:
            for l in range(len(res["authorships"])):
                dic = res["authorships"][l]["author"]
                dic["doi"] = res["doi"]
                dic["display_name_title"] = res["display_name"]
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
                dic["raw_source_name"] = res["primary_location"]["raw_source_name"]
                if "source" in res["primary_location"]:
                    if isinstance(res["primary_location"]["source"], dict):
                        dic["id_source"] = res["primary_location"]["source"]["id"]
                        dic["issn_l"] = res["primary_location"]["source"]["issn_l"]
                        dic["host_organization_name"] = res["primary_location"]["source"][
                            "host_organization_name"]
                else:
                    dic["id_source"] = None
                    dic["issn_l"] = None
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

    df = pd.DataFrame(data=authors)
    df = df.drop_duplicates().reset_index(drop=True)
    df["ror"] = df["ror_ins"].str.replace("https://ror.org/", "", regex=False)
    ror_oa = df.loc[df["ror"].notna()]
    nb = len(ror_oa)
    print(f"1. : Il y a {nb} ROR disponibles dans la requête OpenAlex.", flush=True)
    liste_oa_ror = list(ror_oa["ror"].unique())
    print(f"2. : Exemples de ROR de la jointure df_authors2 et data_missing {liste_oa_ror[0:5]}", flush=True)

    return df


@retry(tries=3, delay=5, backoff=5)
def ids_paysage() -> pd.DataFrame:
    """
    Query to get extra IDs for affiliation institutions based on ROR
    Get dump structures from Paysage
    Structure data from Paysage and create DataFrame
    """
    rres_data = []

    res = requests.get(
        "https://api.paysage.dataesr.ovh/dump/structures",
        headers={"Content-Type": "application/json", "X-API-KEY": PAYSAGE})

    with open("dump_paysage.txt", "w") as f:
        f.write(res.text)

    with open("dump_paysage.txt", "r") as f:
        dump_paysage = f.readlines()

    dump_paysage = [json.loads(item) for item in dump_paysage]

    for etab in dump_paysage:
        dict_res = {}
        current = etab.get("currentName")
        name = current.get("usualName")
        res_data = etab.get("category")
        dict_res["id_paysage"] = etab.get("id")
        pid = etab.get("id")
        dict_res["usualName"] = name
        if len(res_data) > 0:
            identifiers = etab.get("identifiers")
            typs = [item.get("type") for item in identifiers]
            decompte = Counter(typs)
            if "siret" in typs:
                if decompte["siret"] > 1:
                    for item in identifiers:
                        if pid == "9ZF2M":
                            siret = "19341087500010"
                            siren = "193410875"
                            dict_res["siren"] = siren
                            dict_res["siret"] = siret
                        elif item.get("type") == "siret":
                            if item.get("active") == True:
                                siret = item.get("value")
                                siren = siret[0:9]
                                dict_res["siren"] = siren
                                dict_res["siret"] = siret
                elif decompte["siret"] == 1:
                    for item in identifiers:
                        if item.get("type") == "siret":
                            siret = item.get("value")
                            siren = siret[0:9]
                            dict_res["siren"] = siren
                            dict_res["siret"] = siret
            else:
                dict_res["siren"] = ""
                dict_res["siret"] = ""
            if "grid" in typs:
                if decompte["grid"] > 1:
                    for item in identifiers:
                        if item.get("type") == "grid":
                            if item.get("active") == True:
                                grid = item.get("value")
                                dict_res["grid"] = grid
                elif decompte["grid"] == 1:
                    for item in identifiers:
                        if item.get("type") == "grid":
                            grid = item.get("value")
                            dict_res["grid"] = grid
            else:
                dict_res["grid"] = ""
            if "ror" in typs:
                for item in identifiers:
                    if item.get("type") == "ror":
                        ror = item.get("value")
                        dict_res["ror"] = ror
            else:
                dict_res["ror"] = ""
            if "idref" in typs:
                for item in identifiers:
                    if item.get("type") == "idref":
                        idref = item.get("value")
                        idref = "idref" + idref
                        dict_res["idref_ins"] = idref
            else:
                dict_res["idref_ins"] = ""
        rres_data.append(dict_res)

    df_res = pd.DataFrame(rres_data)
    df_res = df_res.drop_duplicates().reset_index(drop=True)

    # keep only structures with ROR
    df_res = df_res.loc[df_res["ror"].notna()]
    df_res = df_res.loc[df_res["ror"] != ""]
    compte_id_paysage = df_res[["ror", "id_paysage"]].groupby("ror").nunique(dropna=False).reset_index()
    df_res = df_res.loc[df_res["ror"].isin(compte_id_paysage.loc[compte_id_paysage["id_paysage"] == 1, "ror"])]
    ror_pays = df_res.loc[df_res["ror"].notna()]
    nb = len(ror_pays)
    print(f"3. : Il y a {nb} ROR disponibles dans la requête Paysage.", flush=True)
    liste_pays_ror = list(ror_pays["ror"].unique())
    print(f"4. : Exemples de ROR de la jointure df_authors2 et data_missing {liste_pays_ror[0:5]}", flush=True)

    return df_res


def get_info_publi():
    # set working directory
    os.chdir(DATA_PATH)

    publi = pd.read_csv("publi_brevets.csv", sep="|", encoding="utf-8", engine="python",
                        dtype=types.tls214_types)

    dois_uniques = list(publi["doi_clean"].unique())

    df_authors = fetch_openalex(dois_uniques, False)

    df_authors2 = pd.merge(publi[["key_appln_nr", "appln_id", "docdb_family_id", "pat_publn_id", "doi_clean", "doi"]],
                           df_authors, on="doi", how="inner")
    df_authors2 = df_authors2.drop_duplicates().reset_index(drop=True)
    df_authors2["publication_year"] = df_authors2["publication_year"].astype(pd.Int64Dtype())
    data_missing = publi.loc[~publi["doi"].isin(df_authors2["doi"])]
    data_missing = data_missing.drop_duplicates().reset_index(drop=True)

    data_missing.loc[data_missing["npl_type"] == "b", "type"] = "book"
    data_missing.loc[data_missing["npl_type"] == "s", "type"] = "journal-article"
    data_missing.loc[data_missing["npl_type"] == "w", "type"] = "other"
    data_missing.loc[data_missing["npl_publn_date"] == "20880630", "npl_publn_date"] = "20080601"
    data_missing.loc[data_missing["npl_publn_date"] == "10160618", "npl_publn_date"] = "20160624"
    data_missing.loc[data_missing["npl_publn_date"].notna(), "publication_year"] = data_missing.loc[
        data_missing["npl_publn_date"].notna(), "npl_publn_date"].apply(lambda a: a[0:4])
    data_missing["publication_year"] = data_missing["publication_year"].astype(pd.Int64Dtype())
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

    df_paysage = ids_paysage()

    oa = pd.concat([df_authors2, data_missing], ignore_index=True)
    oa_ror = oa.loc[oa["ror"].notna()]
    nb = len(oa_ror)
    print(f"5. : Il y a {nb} ROR disponibles après la jointure df_authors2 et data_missing.", flush=True)
    liste_oa_ror = list(oa_ror["ror"].unique())
    print(f"6. : Exemples de ROR de la jointure df_authors2 et data_missing {liste_oa_ror[0:5]}", flush=True)

    oa2 = pd.merge(oa, df_paysage, on="ror", how="left")
    oa2.loc[(oa["raw_type"].str.contains("master", case=False)) & (oa["type"] == "dissertation"), "type"] = "other"
    oa2.loc[oa["type"] == "dissertation", "type"] = "these"
    oa2.loc[oa2["type"] == 'article', "type"] = "journal-article"
    oa2.loc[oa2["type"] == 'book-citation', "type"] = "book"
    oa2.loc[oa2["type"] == 'editorial', "type"] = "other"
    oa2.loc[oa2["type"] == 'erratum', "type"] = "other"
    oa2.loc[oa2["type"] == 'letter', "type"] = "other"
    oa2.loc[oa2["type"] == 'paratext', "type"] = "other"
    oa2.loc[oa2["type"] == 'preprint', "type"] = "other"
    oa2.loc[oa2["type"] == 'retraction', "type"] = "other"
    oa2.loc[oa2["type"] == 'review', "type"] = "other"

    typ_pub = ["book", "book-chapter", "book-part", "book-section", "book-track", "component", "dataset",
               "journal-article",
               "journal-issue", "lecture", "map", "mem", "monograph", "multimedia", "other", "patent", "peer-review",
               "posted-content", "poster", "presconf", "proceedings", "proceedings-article", "proceedings-series",
               "reference-book", "reference-entry", "report", "report-series", "software", "standard", "thesis",
               "these", "video", "ongoing_thesis"]

    oa2.loc[~oa2["type"].isin(typ_pub), "type"] = "other"

    oa2["year"] = oa2["publication_year"].astype(pd.Int64Dtype())

    columns = ["doi_clean", "id", "orcid", "pdf_url", "id_source", "id_ins", "ror_ins", "display_name",
                "display_name_title", "language", "type", "volume", "issue", "raw_source_name", "issn_l",
                "host_organization_name", "display_name_ins", "country_code_ins",
                "type_ins", "idref_ins"]

    for col in columns:
        oa2.loc[oa2[col].notna(), col] = oa2.loc[oa2[col].notna(), col].apply(clean_text)

    oa_orcid = oa2.loc[oa2["orcid"].notna()]
    oa_orcid2 = list(oa_orcid["orcid"].unique())
    print(f"Exemples d'ORCID à trouver : {oa_orcid2[0:5]}", flush=True)
    oa_orcid2 = [x.replace("https://orcid.org/", "") for x in oa_orcid2]

    print("Début du chargement du dump de scanr", flush=True)
    get_person_scanr()
    print("Fin du chargement du dump de scanr", flush=True)

    print("Début de la lecture du dump de scanr", flush=True)
    scanr = []
    with open("persons_denormalized.jsonl", "r") as file:
        for ligne in file:
            author = {}
            res = json.loads(ligne)
            extern = res.get("externalIds")
            scanr.append(extern)
            for item in extern:
                if item.get("type") == "idref":
                    author["idref"] = ["idref" + item.get("id")]
                else:
                    author[item.get("type")] = item.get("id")
            df = pd.DataFrame(data=author)
            if "orcid" in df.columns:
                scanr.append(df)

    scanr2 = [item for item in scanr if not isinstance(item, list)]
    idref_orcid = pd.concat(scanr2)
    idref_orcid = idref_orcid.loc[idref_orcid["orcid"].notna()]
    idref_orcid = idref_orcid[["idref", "orcid"]]
    idref_orcid = idref_orcid.drop_duplicates().reset_index(drop=True)
    idref_orcid2 = idref_orcid.loc[idref_orcid["orcid"].isin(oa_orcid2)]

    oa2 = oa2.rename(columns={"orcid": "orcid_url"})
    oa2["orcid"] = oa2["orcid_url"].str.replace("https://orcid.org/", "", regex=True)
    oa3 = pd.merge(oa2, idref_orcid2, on="orcid", how="left")
    oa3 = oa3.drop(columns="orcid").rename(columns={"orcid_url": "orcid"})
    print("Fin de la lecture du dump de scanr", flush=True)
    print(f"Taille liste scanr: {len(scanr)}", flush=True)

    oa3.to_csv("publi_oa.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'publi_oa.csv')
