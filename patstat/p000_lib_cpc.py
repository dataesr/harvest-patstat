# !/usr/bin/env python
# coding: utf-8
import os
import re
import zipfile
from io import BytesIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

CPC = "https://www.cooperativepatentclassification.org/"


def request(str_url: str) -> object:
    """
    This function takes a URL (string), gets its content, creates a zipfile object and returns this object.
    """
    req = requests.get(str_url)
    zf = zipfile.ZipFile(BytesIO(req.content))
    return zf


def btsp(xml_parse) -> pd.DataFrame:
    """
    This function takes a XML file, parses it and extract text from title-part tags.
    If there is no text directly under title-part, it takes text from CPC-specific-text. Reference text is not included.
    Recursive=False is used to get only the current level of the tree and not deeper levels.
    For each symbol in the XML, we get the corresponding attributes and title.
    This function returns a dataframe.
    """
    soup = BeautifulSoup(xml_parse, "xml")

    symbols = [s for s in soup.find_all("classification-item") if "sort-key" in s.attrs]

    data = []

    for symbol in symbols:
        infos = symbol.attrs.copy()
        liste = []
        sym = symbol.find("class-title")
        if sym:
            for part in sym.find_all("title-part", recursive=False):
                li = [it for it in part.contents if it != "\n"]
                ln = [n.name for n in li]
                if "text" in ln:
                    for prt in part:
                        if prt.name == "text":
                            lis = prt.get_text().strip()
                            liste.append(lis)
                else:
                    for prt in part:
                        if prt.name == "CPC-specific-text":
                            cpc = prt.find_all("text", recursive=False)
                            for c in cpc:
                                lis = c.get_text().strip()
                                liste.append(lis)
            if liste:
                infos["titleParts"] = liste
            data.append(infos)

    df = pd.DataFrame(data)

    return df


def new_level(df: pd.DataFrame, knd: list, lvl: str, srt: str) -> pd.DataFrame:
    """
    This function gives corresponding higher level symbols (level up to "8" included) to symbols deeper in the tree
    (level from "9" and upwards).
    With these symbols, we can get the corresponding titles and avoid too many details.
    """
    symbl = []
    sym = ""

    for _, r in df.iterrows():
        if r[lvl] in knd:
            sym = r[srt]
            symbl.append(sym)
        else:
            symbl.append(sym)

    df.insert(0, srt + "2", symbl, True)
    return df


def lib_cpc():
    # set working directory
    os.chdir(DATA_PATH)

    # /!\ CPC Scheme XML file changes often. Check the current file and URL /!\

    url_scheme = f"{CPC}sites/default/files/cpc/bulk/CPCSchemeXML202208.zip"
    zf_scheme = request(url_scheme)

    # gets all the filenames in the zipped folder
    filenames_scheme = [file_name for file_name in zf_scheme.namelist()]

    # selects only the XLM files with the scheme
    xml_scheme = [name for name in filenames_scheme if re.match(r"^cpc-scheme-[A-Z]", name)]

    tmp_scheme = []

    for names in xml_scheme:
        # Each zipped XML scheme file in the folder is opened and parsed.
        # Symbols, attributes and tittles are collected. The results are then turned into a dataframe.
        df_schme = btsp(zf_scheme.open(names))
        tmp_scheme.append(df_schme)

    df_scheme = pd.concat(tmp_scheme)

    # Levels "3" and "6" correspond to "general category" titles and are not to be kept
    df_scheme = df_scheme.loc[~df_scheme["level"].isin(["3", "6"])]

    # turns titleParts lists into strings
    df_scheme["texte"] = df_scheme["titleParts"].apply(lambda a: " ".join(a))

    # keeps only level, sort-key, ipc-concordant and texte; then drop duplicates
    df_scheme = df_scheme.drop(
        columns=["breakdown-code", "not-allocatable", "additional-only", "date-revised", "status", "link-file",
                 "definition-exists", "c-set-base-allowed", "c-set-subsequent-allowed", "titleParts"]).drop_duplicates()

    # splits symbol into its different components: section, class, subclass, group and subgroup
    # in order to sort the dataframe and match the corresponding higher level symbols to symbols deeper in the tree
    # in this way, we can get title from higher level symbols, indicate the hierarchy and avoid giving too much details
    df_scheme["section"] = df_scheme["sort-key"].str[0]
    df_scheme["class"] = df_scheme["sort-key"].str[1:3]
    df_scheme["subclass"] = df_scheme["sort-key"].str[3]
    df_scheme["group"] = df_scheme["sort-key"].str.replace(r"[A-Z]\d{0,2}[A-Z]{0,1}|\/\d{0,}", "", regex=True)
    df_scheme["subgroup"] = df_scheme["sort-key"].str.replace(r"[A-Z]\d{0,2}[A-Z]{0,1}\d{0,}\/?", "", regex=True)
    df_scheme = df_scheme.fillna("")
    df_scheme = df_scheme.sort_values(["section", "class", "subclass", "group", "subgroup"])

    # get the higher level symbol for symbols from level "8" and upwards
    # with these, we can pick the first part of the title which indicates the hierarchy
    df_scheme["CPC joint"] = ""

    for _, r in df_scheme.iterrows():
        if r["level"] in ["2", "4", "5", "7"]:
            r["CPC joint"] = r["sort-key"]
        else:
            r["CPC joint"] = r["section"] + r["class"] + r["subclass"] + r["group"] + "/00"

    kind_cpc = ["2", "4", "5", "7", "8"]

    # get the higher level symbol for symbols from level "9" and upwards
    # with these, we can pick a higher level title and limit details
    df_scheme = new_level(df_scheme, kind_cpc, "level", "sort-key")

    # get "pre-title" for hierarchy
    df_scheme_title = df_scheme.loc[
        df_scheme["sort-key"].isin(set(df_scheme["CPC joint"])), ["texte", "CPC joint"]].rename(
        columns={"texte": "pre_title"})

    # get "second_title" with less details
    df_scheme_title2 = df_scheme.loc[
        df_scheme["sort-key"].isin(set(df_scheme["sort-key2"])), ["texte", "sort-key2"]].rename(
        columns={"texte": "second_title"})

    df_scheme = pd.merge(df_scheme, df_scheme_title, on="CPC joint", how="left")
    df_scheme = pd.merge(df_scheme, df_scheme_title2, on="sort-key2", how="left")

    df_scheme["title"] = ""

    for _, r in df_scheme.iterrows():
        if r["level"] not in ["2", "4", "5", "7"]:
            r["title"] = r["pre_title"] + " - " + r["second_title"]
        else:
            r["title"] = r["texte"]

    df_scheme = df_scheme.drop(
        columns=["ipc-concordant", "texte", "section", "class", "subclass", "group", "subgroup", "CPC joint",
                 "pre_title",
                 "second_title"])

    df_scheme = df_scheme[["sort-key", "level", "sort-key2", "title"]]
    df_scheme = df_scheme.rename(columns={"sort-key": "symbol", "sort-key2": "ref", "title": "libelle"})

    df_scheme.to_csv("lib_cpc.csv", sep=',', encoding="utf-8", index=False)
