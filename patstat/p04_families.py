#!/usr/bin/env python
# coding: utf-8

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types
from utils import swift

import numpy as np
import os
import pandas as pd
import re

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
DICT = {"patstat": {"sep": ",", "chunksize": 5000000},
        "get_cpc_family_codes": {"sep": ",", "chunksize": 5000000, "dtype": types.tls225_types}
        }


def get_earliest_in_family(patent_table: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    This function gets the earliest date for an event in a family

    :param patent_table: df with patents data
    :param col: column of patent_table with date - must have family identifier "docdb_family_id"
    :return: df with 2 columns docdb_family_id and the earliest date of the event in each family
    """
    if col == "earliest_filing_date":
        text = "earliest_application_date"
    elif col == "earliest_publn_date":
        text = "earliest_publication_date"
    else:
        text = col

    family_first = patent_table.groupby("docdb_family_id", as_index=False)[
        [col]].min().rename(columns={"earliest_filing_date": text})

    return family_first


def get_family_grant(patent_table: pd.DataFrame) -> pd.DataFrame:
    """
    This function gets the first date where an application in the family has been granted
    :param patent_table: df with patents data - must have family identifier "docdb_family_id"
    :return: df with 3 columns docdb_family_id, granted (boolean) and date first granted (NA if not granted)
    """

    patent_table["granted"] = patent_table["granted"].apply(lambda x: 0 if x == "N" else 1)

    _family_grant = patent_table.groupby(["docdb_family_id"]).agg({"granted": max, "grant_publn_date": min}) \
        .rename(columns={"granted": "is_granted", "grant_publn_date": "date_first_granted"})

    return _family_grant


def get_family_phases(patent_table: pd.DataFrame) -> pd.DataFrame:
    """
    This function says if the family is in an European and/or international phase

    :param patent_table: df with patents data - must have family identifier "docdb_family_id"
    :return: df with 3 columns docdb_family_id, is_oeb (boolean) and is_international (boolean)
    """

    _family_phases = patent_table.groupby("docdb_family_id", as_index=False)[["oeb", "international"]].max().rename(
        columns={"oeb": "is_oeb", "international": "is_international"})

    return _family_phases


def get_family_pi_types(patent_table: pd.DataFrame) -> pd.DataFrame:
    """
    This function groups on families and industrial property types
    to get the number of each type of industrial property

    param patent_table: df with patents data - must have family identifier "docdb_family_id"

    :return: df with 4 columns: docdb_family_id and 3 variables each representing the number of applications in one
    industrial property type

    """

    _family_pi_types = patent_table.groupby(["docdb_family_id", "ipr_type"], as_index=True)[["appln_nr_epodoc"]] \
        .count() \
        .pivot_table(index="docdb_family_id", columns="ipr_type", values="appln_nr_epodoc",
                     aggfunc=np.sum, fill_value=0) \
        .rename(columns={"PI": "patents_count", "UM": "utility_models_count", "DP": "design_patents_count"})

    return _family_pi_types


def get_family_tit_abstract(type_publn: str, pat: pd.DataFrame) -> pd.DataFrame:
    """
    Select the title and abstract attached to each DOCDB family ID. Transform authorities and languages into ordered
    categorical values : WO (0) > EP (1) > FR (2) > rest (3) and German (0) > other Romance languages* (1) >
    English and French (2) > rest (99). Pick which language gets first position for each application and
    create a dataframe with 5 variables : DOCDB family ID, title/abstract in English, title/abstract in French,
    default language and title/abstract in default language
    * Romance languages that have been found in PATSTAT Global since 2010.
    :param type_publn: string ("title" or "abstract")
    :param pat: dataframe (patent df)
    :return: dataframe
    """
    _pat = pat.copy()
    _pat["tri_type_office"] = _pat["appln_auth"].apply(
        lambda x: 0 if x == "WO" else 1 if x == "EP" else 2 if x == "FR" else 3)

    description_lg = f"appln_{type_publn}_lg"
    description_wd = f"appln_{type_publn}"
    description_order = f"tri_{type_publn}_lg"
    var_order = ["appln_filing_date", "tri_type_office"]
    var_to_select = [description_lg, description_wd]

    _pat[description_order] = _pat[description_lg].apply(
        lambda x: 0 if x == "de" else 1 if x in ["es", "it", "pt", "ro"] else 2 if x in ["en", "fr"] else 99)

    def fam_desc_language(tmp: pd.DataFrame, vorder: list) -> pd.DataFrame:
        """
        This function groups on families and gets the first values of the variables in list "var_to_select" after
        ordering by the variables in "var_order" list
        :param tmp: subset of patent dataframe - must have family identifier "docdb_family_id"
        :param vorder: variables which will serve to order data
        :return: ordered dataframe
        """

        fam_desc = tmp[["docdb_family_id"] + vorder + var_to_select] \
            .sort_values(["docdb_family_id"] + vorder) \
            .groupby(["docdb_family_id"], as_index=False).first() \
            .drop(columns=vorder)

        return fam_desc

    # order data for title/abstract in English then French
    desc_en = fam_desc_language(_pat[_pat[description_lg] == "en"], var_order)
    desc_fr = fam_desc_language(_pat[_pat[description_lg] == "fr"], var_order)

    # remove English, French and NA and use "language" order for other languages
    tmpother = _pat[(~_pat[description_lg].isin(["en", "fr"])) & (~pd.isna(_pat[description_lg]))]
    var_order_other = [description_order, "appln_filing_date"]

    # order data for title/abstract in other languages
    desc_other = fam_desc_language(tmpother, var_order_other)

    # keep only DOCDB family IDs in the other languages df if the ID is not found in English AND French df
    desc_other = desc_other[(~desc_other["docdb_family_id"].isin(desc_en["docdb_family_id"])) & (
        ~desc_other["docdb_family_id"].isin(desc_fr["docdb_family_id"]))].dropna()

    # merge all the dataframes to get the title/abstract and main languages attached to each DOCDB family
    desc_col = _pat[["docdb_family_id"]] \
        .merge(desc_en, how="left", on="docdb_family_id").drop(columns=description_lg) \
        .rename(columns={description_wd: "{}_en".format(type_publn)}) \
        .merge(desc_fr, how="left", on="docdb_family_id").drop(columns=description_lg) \
        .rename(columns={description_wd: "{}_fr".format(type_publn)}) \
        .merge(desc_other, how="left", on="docdb_family_id") \
        .rename(columns={description_wd: "{}_default".format(type_publn),
                         description_lg: "{}_default_language".format(type_publn)}) \
        .drop_duplicates()

    return desc_col


def fam(pat: pd.DataFrame) -> pd.DataFrame:
    """
    This function gets all the info on docdb families
    :param pat: df with patents data -  must have family identifier "docdb_family_id"
    :return: df with 19 columns: docdb_family_id, earliest publication date, earliest application date
    is_oeb, is_international, design patents count, patent counts, utility models counts, is_granted,
    date first granted, title en, title fr, title default language, title default, abstract en, abstract fr,
    abstract default language and abstract default
    """
    family_appln = get_earliest_in_family(pat, "earliest_filing_date")

    family_publn = get_earliest_in_family(pat, "earliest_publn_date")

    family_grant = get_family_grant(pat)

    family_phases = get_family_phases(pat)

    family_pi_types = get_family_pi_types(pat)

    family_titles = get_family_tit_abstract("title", pat)

    family_abstracts = get_family_tit_abstract("abstract", pat)

    _fam = pd.merge(pat[["docdb_family_id", "inpadoc_family_id"]].drop_duplicates(),
                    family_publn, how="left",
                    on="docdb_family_id").merge(family_appln, how="left",
                                                on="docdb_family_id").merge(family_phases,
                                                                            how="left",
                                                                            on="docdb_family_id").merge(
        family_pi_types, how="left", on="docdb_family_id").merge(family_grant, how="left", on="docdb_family_id").merge(
        family_titles, how="left", on="docdb_family_id").merge(family_abstracts, how="left",
                                                               on="docdb_family_id").sort_values("docdb_family_id")

    _fam = _fam.fillna("")
    return _fam


def fam_ipc_cpc(pat: pd.DataFrame, colfilter: str, tls: str, dicttype: dict) -> pd.DataFrame:
    """

    :param pat: df with patents data
    :param colfilter: column that serves as a filter (appln_id or docdb_family_id)
    :param tls: table directory
    :param dicttype: dictionary with data types (DICT["patstat"] or ["get_cpc_family_codes"]
    :return: a df with 3 or 4 columns : filtering column (appln_id or docdb_family_id), level, code
    (and classif for tables 224 and 209)
    """
    print(f"Start fam_ipc_cpc for {tls}.")

    # load filtered data from PATSTAT tables
    table_techno = cfq.filtering(tls, pat, colfilter, dicttype)

    # identify variable info on codes and whose name contains "_class_symbol"
    # either cpc_class_symbol or ipc_class_symbol
    col_class_symbol = [item for item in list(table_techno.columns) if re.findall(r".+_class_symbol", item)][0]

    # extracts all levels of info on techno
    table_techno["groupe"] = table_techno[col_class_symbol].str.replace(" ", "")
    table_techno["ss_classe"] = table_techno["groupe"].str[0:4]
    table_techno["classe"] = table_techno["groupe"].str[0:3]
    table_techno["section"] = table_techno["groupe"].str[0:1]

    df_level = list(map(lambda a: table_techno.melt(id_vars=[colfilter], value_vars=[a], var_name="level",
                                                    value_name="code")
                        .drop_duplicates(), ["groupe", "ss_classe", "classe", "section"]))
    df_cpc_patent_codes = pd.concat(df_level)

    # create column "classif" with info on CPC or IPC
    if tls in ("tls224", "tls209"):
        df_cpc_patent_codes["classif"] = col_class_symbol[0:3]

    print(f"End fam_ipc_cpc for {tls}.")

    return df_cpc_patent_codes


def table_cpc_ipc(pat, tls: list) -> pd.DataFrame:
    """
    This function applies function fam_ipc_cpc to a list of tables
    :param pat: df with patents data
    :param tls: list of directories (as strings)
    :return: a df with 3 or 4 columns : filtering column (appln_id or docdb_family_id), level, code
    (and classif for tables 224 and 209)
    """
    table = list(map(lambda a: fam_ipc_cpc(pat, "appln_id", a, DICT["patstat"]), tls))
    df_table = pd.concat(table)
    return df_table


def unify_technos_family_codes(tech_pat_codes: pd.DataFrame, cpc_fam_codes: pd.DataFrame,
                               cpc_cat_names: pd.DataFrame) -> pd.DataFrame:
    """
    This function gets from application and family identifiers the technology codes associated.
    :param tech_pat_codes: df with techno codes by application
    :param cpc_fam_codes: df with techno codes by family
    :param cpc_cat_names: df with techno codes names
    :return:
    """

    # techno codes tls 224 and 209 grouped by family instaed of by patent
    fam_ipc_cpc_from_patents = tech_pat_codes.drop(columns=["classif"]).drop_duplicates()

    # merge all techno codes by family
    fam_technos = pd.concat([fam_ipc_cpc_from_patents, cpc_fam_codes]).drop_duplicates()

    # get techno code names - IPC is a subset of CPC so names from CPC are enough
    fam_technos_with_names = fam_technos.merge(cpc_cat_names, how="left", on="code")

    return fam_technos_with_names


def unify(pat: pd.DataFrame, cpc_cat_names: pd.DataFrame) -> pd.DataFrame:
    """
    This function applies unify_technos_family_codes to tables 224, 209 and 225
    :param pat: df with patent data
    :param cpc_cat_names: df with CPC codes and names
    :return:
    """
    print("Start unifying.")
    # load tls 224 and 209 and apply table_cpc_ipc
    cpc_ipc_table = table_cpc_ipc(pat, ["tls224", "tls209"])

    # create df with techno codes by application and family
    technos_patent_codes = pd.merge(pat[["docdb_family_id", "appln_id"]], cpc_ipc_table,
                                    on="appln_id", how="inner")[["docdb_family_id", "level", "code", "classif"]] \
        .drop_duplicates()

    # load tls 225 and apply table_cpc_ipc
    cpc_family_codes = fam_ipc_cpc(pat, "docdb_family_id", "tls225", DICT["get_cpc_family_codes"])

    # apply unify_technos_family_codes to our data
    fam_tech_codes = unify_technos_family_codes(technos_patent_codes, cpc_family_codes, cpc_cat_names)
    print("End unifying.")
    return fam_tech_codes


def getfam():
    # set working directory
    os.chdir(DATA_PATH)

    patent = pd.read_csv("patent.csv", sep="|",
                         parse_dates=["appln_filing_date", "earliest_filing_date", "earliest_publn_date",
                                      "appln_publn_date", "grant_publn_date"], dtype=types.patent_types)

    swift.download_object('patstat', 'lib_cpc.csv', 'lib_cpc.csv')
    cpc_category_names = pd.read_csv("lib_cpc.csv",
                                     sep=",").rename(columns={"symbol": "code"}).drop(columns=["level", "ref"])

    families = fam(patent)

    families.to_csv("families.csv", sep="|", index=False)

    family_technos_codes = unify(patent, cpc_category_names)

    family_technos_codes.to_csv("families_technologies.csv", sep="|", index=False)
