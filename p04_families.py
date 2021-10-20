#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import numpy as np
import os
import pandas as pd
import re

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"
DICT = {"patstat": {"sep": ",", "chunksize": 5000000},
        "get_cpc_family_codes": {"sep": ",", "chunksize": 5000000, "dtype": types.tls225_types}
        }

# set working directory
os.chdir(DATA_PATH)


# On reprend la table patent générale


def get_family_first_application(patent_table):
    """   This function groups on families and gets the earliest filling date for one family

    param patent_table: Table with application informations - T201 or part of -
    must have family identifier "docdb_family_id"
    and "earliest_filing_date"
    type patent_table: dataframe

    :return: a dataframe with families identifiers and their earliest application date

    """

    family_first_application = patent_table.groupby("docdb_family_id", as_index=False)[
        ["earliest_filing_date"]].min().rename(columns={"earliest_filing_date": "earliest_application_date"})

    return family_first_application


def get_family_publi(patent_table):
    """   This function groups on families and gets the earliest publication date for one family

    param patent_table:  Table with application informations - T201 or part of -
    must have family identifier "docdb_family_id"
    and "earliest_publn_date"
    type patent_table: dataframe

    :return: a dataframe with families identifierss and their earliest publication date

    """

    family_publi = patent_table.groupby("docdb_family_id", as_index=False)[["earliest_publn_date"]].min().rename(
        columns={"earliest_publn_date": "earliest_publication_date"})

    return family_publi


def get_family_grant(patent_table):
    """   This function groups on families and gets informations on the first grant for one family

    param patent_table:  Table with application informations - T201 or part of -
    must have family identifier "docdb_family_id",
    "grant_publn_date" and "granted"
    type patent_table: dataframe

    :return: a dataframe with families identifiers and grant informations

    """

    patent_table["granted"] = patent_table["granted"].apply(lambda x: 0 if x == "N" else 1)

    _family_grant = patent_table.groupby(["docdb_family_id"]).agg({"granted": max, "grant_publn_date": min}) \
        .rename(columns={"granted": "is_granted", "grant_publn_date": "date_first_granted"})

    return _family_grant


def get_family_phases(patent_table):
    """   This function groups on families and gets informations on the procedures types

    param patent_table:  Table with application informations - T201 or part of -
    must have "family identifier "docdb_family_id",
    "oeb" and "international"
    type patent_table: dataframe

    :return: a dataframe with families identifiers and procedures informations

    """

    _family_phases = patent_table.groupby("docdb_family_id", as_index=False)[["oeb", "international"]].max().rename(
        columns={"oeb": "is_oeb", "international": "is_international"})

    return _family_phases


def get_family_pi_types(patent_table):
    """  This function groups on families and industrial property types
    to get the number of each type of industrial property

    param patent_table:  Table with application informations - T201 or part of -
    must have "family identifier "docdb_family_id",
    application identifier "appln_nr_epodoc" and "ipr_type"
    type patent_table: dataframe

    :return: a dataframe with families identifiers and 3 variables each representing the number of applications in one
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
    English and French (2) > rest (3). Pick which language gets first position for each application and
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


def fam(pat):
    family_appln = get_family_first_application(pat)

    family_publn = get_family_publi(pat)

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
    """   This function gets the technologies in the table "name_table_techno" related to the patents in "set_patent"

        param set_patent:  set of application identifiers "appln_id"
        type set_patent: set
        param name_table_techno:  Table with technology informations - must have application identifier "appln_id"
        type name_table_techno: dataframe

        :return: a dataframe with one line for each group : patent/level of technology/technology code

        """

    # cette partie permet de lire des tables de technologies de patstat et en extraire seulement les technos des brevets
    # de notre périmètre (set_patent)
    def lect_patstat_table_from_appln_id(chunk):
        """   This query extracts from any patstat table the informations related to the patent applications in
        "set_patent"

        param chunk:   any dataframe containing the application identifier "appln_id"
        type chunk: dataframe
        param set_patent:  set containing the applications identifiers "appln_id" to keep
        type set_patent: set

        :return: a dataframe, an extract of T204 table for the application identifiers present in patent_table loaded

        """

        query = chunk[chunk[colfilter].isin(pat[colfilter])]

        return query

    table_techno = cfq.multi_csv_files_querying(tls, lect_patstat_table_from_appln_id, dicttype)

    # on identifie la variable qui contient les codes et contient "class_symbol"
    # ( ça peut être cpc_class_symbol ou ipc_class_symbol)
    col_class_symbol = [item for item in list(table_techno.columns) if re.findall(r".+_class_symbol", item)][0]
    # on démultiplie l"information sur les codes technos pour l"avoir à tous les niveaux

    table_techno["groupe"] = table_techno[col_class_symbol].str.replace(" ", "")
    table_techno["ss_classe"] = table_techno["groupe"].str[0:4]
    table_techno["classe"] = table_techno["groupe"].str[0:3]
    table_techno["section"] = table_techno["groupe"].str[0:1]

    df_level = list(map(lambda a: table_techno.melt(id_vars=[colfilter], value_vars=[a], var_name="level",
                                                    value_name="code")
                        .drop_duplicates(), ["groupe", "ss_classe", "classe", "section"]))
    df_cpc_patent_codes = pd.concat(df_level)

    if tls in ("tls224", "tls209"):
        df_cpc_patent_codes["classif"] = col_class_symbol[0:3]

    return df_cpc_patent_codes


def table_cpc_ipc(pat, tls: list) -> pd.DataFrame:
    table = list(map(lambda a: fam_ipc_cpc(pat, "appln_id", a, DICT["patstat"]), tls))
    df_table = pd.concat(table)
    return df_table


def unify_technos_family_codes(tech_pat_codes: pd.DataFrame, cpc_fam_codes: pd.DataFrame,
                               cpc_cat_names: pd.DataFrame) -> pd.DataFrame:
    """   This function gets from one table with application and family identifiers the technology codes related.

    param patent_table:  Table with application informations - T201 or part of - must have family identifier
    "docdb_family_id"
    type patent_table: dataframe

    :return: a dataframe with families identifiers and : the type of classification, the level,
    the code and the label of technologies related

    """

    # Tout d"abord on récupère les codes technos provenant des table 224 et 209 (qui sont par patent)
    # et on les groupe par famille
    fam_ipc_cpc_from_patents = tech_pat_codes.drop(columns=["classif"]).drop_duplicates()

    # On fusionne en une table avec tous les codes technos par famille
    fam_technos = pd.concat([fam_ipc_cpc_from_patents, cpc_fam_codes]).drop_duplicates()

    # Enfin on récupère les libellés liés à ces codes
    # (les libellés IPC sont les mêmes que cpc car la classification est une étendue (beaucoup plus volumineuse)
    # de la classification IPC : elle reprend la même classification de base

    fam_technos_with_names = fam_technos.merge(cpc_cat_names, how="left", on="code")

    return fam_technos_with_names


def unify(pat, cpc_cat_names):
    cpc_ipc_table = table_cpc_ipc(pat, ["tls224", "tls209"])

    technos_patent_codes = pd.merge(pat[["docdb_family_id", "appln_id"]], cpc_ipc_table,
                                    on="appln_id", how="inner")[["docdb_family_id", "level", "code", "classif"]] \
        .drop_duplicates()

    cpc_family_codes = fam_ipc_cpc(pat, "docdb_family_id", "tls225", DICT["get_cpc_family_codes"])

    fam_tech_codes = unify_technos_family_codes(technos_patent_codes, cpc_family_codes, cpc_cat_names)
    return fam_tech_codes


def main():
    patent = pd.read_csv("patent.csv", sep="|",
                         parse_dates=["appln_filing_date", "earliest_filing_date", "earliest_publn_date",
                                      "appln_publn_date", "grant_publn_date"], dtype=types.patent_types)

    cpc_category_names = pd.read_csv("lib_cpc.csv",
                                     sep=",").rename(columns={"symbol": "code"}).drop(columns=["level", "ref"])

    families = fam(patent)

    families.to_csv("families.csv", sep="|", index=False)

    family_technos_codes = unify(patent, cpc_category_names)

    family_technos_codes.to_csv("families_technologies.csv", sep="|", index=False)


if __name__ == "__main__":
    main()
