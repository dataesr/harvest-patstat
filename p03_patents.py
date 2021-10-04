#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import numpy as np
import os
import pandas as pd

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"

# dictionary with pd.read_csv parameters
DICT = {"tls204": {"sep": ",", "chunksize": 5000000, "dtype": types.tls204_types},
        "tls211": {"sep": ",", "chunksize": 5000000, "dtype": types.tls211_types}}

# set working directory
os.chdir(DATA_PATH)


# On crée les variables des phases de dossiers et de délivrance

def add_application_phases(patent_table: pd.DataFrame) -> pd.DataFrame:
    pat_phase = patent_table.copy()
    pat_phase["reg_phase"] = np.where(pat_phase["reg_phase"] == "N", 0, 1)
    pat_phase["nat_phase"] = np.where(pat_phase["nat_phase"] == "N", 0, 1)
    pat_phase["oeb"] = np.where((pat_phase["reg_phase"] == 1) & (pat_phase["appln_auth"] == "EP"), 1, 0)
    pat_phase["international"] = np.where(pat_phase["int_phase"] == "Y", 1, 0)

    return pat_phase


# On filtre sur la table T211 des publications toutes les publis liées à nos brevets

def get_publications_from_appln_publn(t211directory: str, patent_table: pd.DataFrame) -> pd.DataFrame:
    set_applnid = patent_table["appln_id"]
    set_publnid = patent_table["earliest_pat_publn_id"]

    def query_publications_from_appln_publn(chunk: pd.DataFrame) -> pd.DataFrame:
        publn_chunk = chunk[(chunk["appln_id"].isin(set_applnid)) |
                            (chunk["pat_publn_id"].isin(set_publnid))]
        return publn_chunk

    _publications = cfq.multi_csv_files_querying(t211directory, query_publications_from_appln_publn,
                                                 DICT.get(t211directory))

    return _publications


# Dans cette table "publications", publications filtrées sur notre périmètre, on récupère les informations
# liées au dépôt : première publication, identifiée dans la table T201 par la varialbe "earliest_pat_publn_id":
# on récupère l"identifiant "publn_id" lié à cette première publication ainsi que l"identifiant international "publn_nr"
# et également la date de prmière publication.

def get_first_publication(patent_table: pd.DataFrame, publn_table: pd.DataFrame) -> pd.DataFrame:
    """ This function gets from an application table and a publication table the information on the earliest publication

    param patent_table:  a table with application identifier and related first publication identifier
    type patent_table: dataframe
    param publn_table:  Table with publication informations - T211 or part of
    type publn_table: dataframe

    :return: a dataframe with added publication informations : the patstat publication identifier,
    the international publication identifier and the publication date,
    all related to the earliest publication of the application

    """

    depot = pd.merge(patent_table[["appln_id", "earliest_pat_publn_id"]],
                     publn_table[["pat_publn_id", "publn_nr", "publn_date", "publn_kind"]],
                     how="inner", left_on="earliest_pat_publn_id", right_on="pat_publn_id") \
        .rename(
        columns={"publn_nr": "appln_publn_number",
                 "publn_date": "appln_publn_date",
                 "pat_publn_id": "appln_publn_id"}).drop(columns=["earliest_pat_publn_id"])

    return depot


def get_first_grant(patent_table: pd.DataFrame, publn_table: pd.DataFrame) -> pd.DataFrame:
    """  This function gets from an application table and a publication table the first grant"s information

    param patent_table:  a table with application identifier
    type patent_table: dataframe
    param publn_table:  Table with publication informations - T211 or part of
    type publn_table: dataframe

    :return: a dataframe with grant informations related to application identifiers "appln_id"

    """

    grant = pd.merge(patent_table[["appln_id"]],
                     publn_table[publn_table["publn_first_grant"] == "Y"][
                         ["appln_id", "pat_publn_id", "publn_nr", "publn_date"]],
                     how="inner", on="appln_id") \
        .rename(
        columns={"publn_nr": "grant_publn_number", "publn_date": "grant_publn_date", "pat_publn_id": "grant_publn_id"})

    return grant


# Gestion des priorités


def get_priorities(t204directory: str, patent_table: pd.DataFrame) -> pd.DataFrame:
    """ This function gets from an application table and a repertory with priority tables the informations on priorities

    param t204directory:   path of directory containing one or a few patstat"s priority tables (csv)
    type t204directory: string
    param patent_table:  Table with application informations - T211 or part of - must have "appln_id"
    type patent_table: dataframe
    param nrows: allows to specify a number of lines only to extract for tests
    type nrows: integer

    :return: a dataframe, an extract of T204 table for the application identifiers present in patent_table loaded

    """

    set_applnid = patent_table["appln_id"]

    def lect_patstat_table_from_appln_id(chunk: pd.DataFrame) -> pd.DataFrame:
        """   This query extracts from any patstat table the informations related to the patent applications in
        "set_patent"

        param chunk:   any dataframe containing the application identifier "appln_id"
        type chunk: dataframe
        param set_patent:  set containing the applications identifiers "appln_id" to keep
        type set_patent: set

        :return: a dataframe, an extract of T204 table for the application identifiers present in patent_table loaded

        """

        query = chunk[chunk["appln_id"].isin(set_applnid)]

        return query

    t204 = cfq.multi_csv_files_querying(t204directory, lect_patstat_table_from_appln_id, DICT.get(t204directory))

    return t204


def get_ispriority_byfamily(patent_table: pd.DataFrame) -> pd.DataFrame:
    """   This function creates from an application table the indicator on the priority in the family

    param patent_table:  Table with application informations - T211 or part of - must have "appln_id" identifier and
    family identifier "docdb_family_id"
    type patent_table: dataframe

    :return: a dataframe with a boolean variable added indicating if the application is a priority in his family or not.

    """
    priority_table = get_priorities("tls204", patent_table)
    # cette étape récupère les priorités d"une application
    prior = pd.merge(patent_table[["appln_id", "docdb_family_id"]],
                     priority_table[["appln_id", "prior_appln_id"]],
                     how="inner", on="appln_id") \
        .rename(columns={"docdb_family_id": "fam", "appln_id": "appln_id_orig"})

    # cette étape récupère la famille de la priorité
    prior_fam = pd.merge(prior, patent_table[["appln_id", "docdb_family_id"]],
                         how="left", left_on="prior_appln_id", right_on="appln_id") \
        .rename(columns={"docdb_family_id": "fam_prior"}) \
        .drop(columns=["appln_id"])

    # cette étape filtre les priorités seulement sur les priorités de la même famille
    set_priority = set(prior_fam[prior_fam["fam"] == prior_fam["fam_prior"]]["prior_appln_id"])

    _ispriority = patent_table.copy()[["appln_id"]]
    _ispriority["ispriority"] = np.where(_ispriority["appln_id"].isin(set_priority), 1, 0)

    return _ispriority


patent_scope = pd.read_csv("patent_scope.csv", sep="|", dtype=types.tls201_types)
titles = pd.read_csv("titles.csv", sep="|", dtype={"appln_id": np.int64})
abstracts = pd.read_csv("abstracts.csv", sep="|", dtype={"appln_id": np.int64})
pat = add_application_phases(patent_scope)
publications = get_publications_from_appln_publn("tls211", pat)
# publications.to_csv("publications.csv", sep="|", index=False)
first_application = get_first_publication(pat, publications)
first_grant = get_first_grant(pat, publications)
ispriority = get_ispriority_byfamily(pat)
patent = pd.merge(pat, first_application, how="left", on="appln_id").merge(first_grant, how="left",
                                                                           on="appln_id").merge(ispriority, how="left",
                                                                                                on="appln_id").merge(
    titles, how="left", on="appln_id").merge(abstracts, how="left", on="appln_id")

# patent.to_csv("patent.csv", sep="|", index=False)
