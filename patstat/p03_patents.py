#!/usr/bin/env python
# coding: utf-8

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types
import numpy as np
import os
import pandas as pd
from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# dictionary with pd.read_csv parameters
DICT = {"tls204": {"sep": ",", "chunksize": 5000000, "dtype": types.tls204_types},
        "tls211": {"sep": ",", "chunksize": 5000000, "dtype": types.tls211_types}}


def add_application_phases(patent_table: pd.DataFrame) -> pd.DataFrame:
    """
    np.where : if x==y: z else: a
    np.where...N, 0, 1 -> missing values are considered as Y and get 1
    np.where...Y, 1, 0 -> missing values are considered as N and get 0
    copy to avoid side effect
    :param patent_table: patent_scope
    :return: dataframe where reg_phase, nat_phase are turned into dummy variables & where 2 other dummy variables
    are created : oeb (in or have been in EP phase) international (in or have been in international phase)
    """
    pat_phase = patent_table.copy()
    pat_phase["reg_phase"] = pat_phase["reg_phase"].apply(lambda x: 0 if x == "N" else 1)
    pat_phase["nat_phase"] = pat_phase["nat_phase"].apply(lambda x: 0 if x == "N" else 1)
    pat_phase["oeb"] = np.where((pat_phase["reg_phase"] == 1) & (pat_phase["appln_auth"] == "EP"), 1, 0)
    pat_phase["international"] = pat_phase["int_phase"].apply(lambda x: 1 if x == "Y" else 0)

    return pat_phase


def get_publications_from_appln_publn(t211directory: str, patent_table: pd.DataFrame) -> pd.DataFrame:
    """
    :param t211directory: directory where tls 211 files are
    :param patent_table: output from add_application_phases
    :return: dataframe with publication info for French application ID
    """
    print("Start get publication from application ID")

    def query_publications_from_appln_publn(chunk: pd.DataFrame) -> pd.DataFrame:
        publn_chunk = chunk[(chunk["appln_id"].isin(patent_table["appln_id"])) |
                            (chunk["pat_publn_id"].isin(patent_table["earliest_pat_publn_id"]))]
        return publn_chunk

    _publications = cfq.multi_csv_files_querying(t211directory, query_publications_from_appln_publn,
                                                 DICT.get(t211directory))

    print("End get publication from application ID")

    return _publications


def get_first_application(patent_table: pd.DataFrame, publn_table: pd.DataFrame) -> pd.DataFrame:
    """
    param patent_table:  output from add_application_phases
    param publn_table:  output from get_publications_from_appln_publn
    :return: a dataframe with publication info for the first application registered
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
    """
    param patent_table:  output from add_application_phases
    param publn_table:  output from get_publications_from_appln_publn
    :return: a dataframe with grant info for the first application registered
    """

    grant = pd.merge(patent_table[["appln_id"]],
                     publn_table[publn_table["publn_first_grant"] == "Y"][
                         ["appln_id", "pat_publn_id", "publn_nr", "publn_date"]],
                     how="inner", on="appln_id") \
        .rename(
        columns={"publn_nr": "grant_publn_number", "publn_date": "grant_publn_date", "pat_publn_id": "grant_publn_id"})

    return grant


# Gestion des priorités


def get_priorities(t204directory: str, pat_tab: pd.DataFrame, colfilter: str, dict_param_load: dict) -> pd.DataFrame:
    """
    param t204directory:   directory where tls 204 files are
    param patent_table:  output from add_application_phases
    :return: dataframe with the French applications which have a priority claim
    """

    print("Start get priorities from application ID")

    t204 = cfq.filtering(t204directory, pat_tab, colfilter, dict_param_load)

    print("End get priorities from application ID")

    return t204


def get_ispriority_byfamily(_priority_table: pd.DataFrame, patent_table: pd.DataFrame) -> pd.DataFrame:
    """
    :param _priority_table: output from get_priority
    :param patent_table: output from add_application_phases
    :return: dataframe
    """

    # we keep only applications and their DOCDB family with priorities
    # prior_appln_id refers to the application of which the priority is claimed
    prior = pd.merge(patent_table[["appln_id", "docdb_family_id"]],
                     _priority_table[["appln_id", "prior_appln_id"]],
                     how="inner", on="appln_id") \
        .rename(columns={"docdb_family_id": "fam", "appln_id": "appln_id_orig"})

    # by merging on the tls 204 prior_appln_id and tls 201 appln_id, we get the DOCDB family of the original application
    prior_fam = pd.merge(prior, patent_table[["appln_id", "docdb_family_id"]],
                         how="left", left_on="prior_appln_id", right_on="appln_id") \
        .rename(columns={"docdb_family_id": "fam_prior"}) \
        .drop(columns=["appln_id"])

    # get prior_appln_id for applications which have the same DOCDB family
    # both in the original and subsequent application
    set_priority = prior_fam[prior_fam["fam"] == prior_fam["fam_prior"]]["prior_appln_id"]

    # create dataframe with appln_id and a dummy variable "ispriority" with 1 if both the original
    # and subsequent application have the same DOCDB family, else 0
    _ispriority = patent_table.copy()[["appln_id"]]
    _ispriority["ispriority"] = np.where(_ispriority["appln_id"].isin(set_priority), 1, 0)

    return _ispriority


def get_pubpat():
    # set working directory
    os.chdir(DATA_PATH)
    patent_scope = pd.read_csv("patent_scope.csv", sep="|", dtype=types.tls201_types)
    titles = pd.read_csv("titles.csv", sep="|", dtype={"appln_id": np.int64})
    abstracts = pd.read_csv("abstracts.csv", sep="|", dtype={"appln_id": np.int64})
    pat = add_application_phases(patent_scope)
    publications = get_publications_from_appln_publn("tls211", pat)
    publications.to_csv("publications.csv", sep="|", index=False)
    first_application = get_first_application(pat, publications)
    first_grant = get_first_grant(pat, publications)
    priority_table = get_priorities("tls204", pat, "appln_id", DICT["tls204"])
    ispriority = get_ispriority_byfamily(priority_table, pat)
    patent = pd.merge(pat,
                      first_application,
                      how="left",
                      on="appln_id").merge(first_grant,
                                           how="left",
                                           on="appln_id").merge(ispriority,
                                                                how="left",
                                                                on="appln_id").merge(titles,
                                                                                     how="left",
                                                                                     on="appln_id").merge(abstracts,
                                                                                                          how="left",
                                                                                                          on="appln_id")

    patent.to_csv("patent.csv", sep="|", index=False)
    swift.upload_object('patstat', 'patent.csv')
