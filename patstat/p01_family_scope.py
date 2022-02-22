#!/usr/bin/env python
# coding: utf-8

import os

import pandas as pd

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

COL = {"patent_appln_columns": ["appln_id", "appln_kind", "earliest_filing_year", "ipr_type", 'earliest_publn_year'],
       "person_columns": ["person_id", "person_ctry_code"],
       "usecols": ["appln_id", "docdb_family_id"]}

DICT = {
    "tls201_post2010": {'sep': ',', 'chunksize': 20000000, 'usecols': COL.get("patent_appln_columns"),
                        'dtype': types.tls201_types},
    "tls206_french_person_id": {'sep': ',', 'chunksize': 5000000, 'usecols': COL.get("person_columns"),
                                'dtype': types.tls206_types},
    "tls207_get_scope_applications": {'sep': ',', 'chunksize': 20000000, 'dtype': types.tls207_types},
    "tls201_families_from_set_patent": {'sep': ',', 'chunksize': 20000000, 'dtype': types.tls201_types,
                                        'usecols': COL.get("usecols")},
    "tls201_patent_from_set_family": {'sep': ',', 'chunksize': 2000000, 'dtype': types.tls201_types}
}

# set working directory
os.chdir(DATA_PATH)


# tls 201 : get ID from only patent post 2010 (9999 is for missing date)
def get_patent_post2010_appln_id(t201directory: str) -> pd.DataFrame:
    """
    :param t201directory: takes directory where tls201 files are
    :return: filtered dataframe with only one column "appln_id"
    """
    print("Start get ID patent post 2010")
    dict_param_load = DICT.get("tls201_post2010")

    def query_patent_applications_t201(chunk_t201: pd.DataFrame) -> pd.DataFrame:
        """
        :param chunk_t201: a chunk from CSV
        :return: dataframe with filtered "appln_id"
        """
        patent_applications_query = chunk_t201[(chunk_t201['appln_kind'].isin(['A ', 'W ', 'P '])) &
                                               (chunk_t201['earliest_filing_year'] < 9999) &
                                               (chunk_t201['earliest_publn_year'] < 9999) &
                                               (chunk_t201['earliest_filing_year'] > 2009) &
                                               (chunk_t201['ipr_type'] == "PI")][['appln_id']]
        return patent_applications_query

    patent_appln_chunk = cfq.multi_csv_files_querying(t201directory, query_patent_applications_t201, dict_param_load)
    print("End get ID patent post 2010")
    return patent_appln_chunk


# tls 206 : ne garde que les ID des personnes "françaises"
def get_french_person_id(t206directory: str) -> pd.DataFrame:
    """
    :param t206directory: directory where tls206 files are
    :return: a dataframe with only one column "person_id"
    """
    print("Start get French person ID")
    dict_param_load = DICT.get("tls206_french_person_id")

    def query_french_persons_t206(chunk_t206: pd.DataFrame) -> pd.DataFrame:
        """
        :param chunk_t206: a chunk of read_csv and list of codes fro France in Patstat
        :return: only the rows of the dataframe with French codes
        """
        # liste des pays français avec DOM-TOM
        list_french_codes = ["FR", "GP", "GF", "MQ", "RE", "YT", "PM", "MF", "BL", "WF", "TF", "NC", "PF"]
        french_query = chunk_t206.loc[
            chunk_t206['person_ctry_code'].isin(list_french_codes), ['person_id']].drop_duplicates()
        return french_query

    french_chunk = cfq.multi_csv_files_querying(t206directory, query_french_persons_t206, dict_param_load)
    french_chunk = french_chunk[["person_id"]]
    print("End get French person ID")
    return french_chunk


# tls 207 : charge les applications et ne garde que celles dont l'application des personnes qui demandent le brevet

def get_scope_applications(t207directory: str, patent_app_id: pd.DataFrame, french_id: pd.DataFrame) -> pd.DataFrame:
    """
    :param t207directory: directory where tls 207 files are
    :param patent_app_id: result from get_patent_post2010_appln_id - only patent post 2010
    :param french_id: result from get_french_person_id - only French patent
    :return: a dataframe with patent ID post 2010 from only French persons
    """
    print("Start get scope applications")

    def chunk_query(chunk: pd.DataFrame) -> pd.DataFrame:
        """
        :param chunk: a chunk from CSV
        :return: a dataframe with only applicants, not inventors or others...
        """
        chunk_trie = chunk[(chunk['applt_seq_nr'] > 0)]
        return chunk_trie

    dict_param_load = DICT.get("tls207_get_scope_applications")
    _scope_app = cfq.multi_csv_files_querying(t207directory, chunk_query, dict_param_load)
    _scope_app = _scope_app[["person_id", "appln_id"]]
    scope_patent = pd.merge(patent_app_id, _scope_app, how="inner", on="appln_id", sort=False,
                            validate="1:m")
    _scope_applications = pd.merge(french_id, scope_patent, how="inner", on="person_id", sort=False,
                                   validate="1:m")
    print("End get scope applications")
    return _scope_applications


# tls 201
def get_families_from_set_patent(t201directory: str, _set_application: list) -> pd.DataFrame:
    """
    :param t201directory: directory where tls 201 files are
    :param _set_application: application ID from get_scope_applications
    :return: a dataframe with families for the application ID
    """
    print("Start get families from set patent")

    def chunk_query(chunk: pd.DataFrame) -> pd.DataFrame:
        """
        :param chunk: a chunk from CSV
        :return: a dataframe with only one column docdb family ID which correspond to application ID
        """
        chunk_trie = chunk[(chunk['appln_id'].isin(_set_application))][
            ['docdb_family_id']].drop_duplicates()
        return chunk_trie

    dict_param_load = DICT.get("tls201_families_from_set_patent")
    _family_scope = cfq.multi_csv_files_querying(t201directory, chunk_query, dict_param_load)
    print("End get families from set patent")
    return _family_scope


def get_patent_from_set_family(t201directory: str, fam_sc: pd.DataFrame, colfilter: str,
                               dict_param_load: dict) -> pd.DataFrame:
    """

    :param t201directory: directory where tls 201 files are
    :param fam_sc: df where filtering info are
    :param colfilter: column which serves as a filter
    :param dict_param_load: dictionary with loading parameters
    :return: load table with filtered values
    """

    print("Start get patent from set family")

    _patent_scope = cfq.filtering(t201directory, fam_sc, colfilter, dict_param_load)

    print("End get patent from set family")
    return _patent_scope


def main():
    patent_appln_id = get_patent_post2010_appln_id("tls201")
    french_person_id = get_french_person_id("tls206")
    scope_applications = get_scope_applications("tls207", patent_appln_id, french_person_id)
    family_scope = get_families_from_set_patent("tls201", scope_applications['appln_id'])
    family_scope.to_csv("docdb_family_scope.csv", sep='|', index=False)
    patent_scope = get_patent_from_set_family("tls201", family_scope, "docdb_family_id",
                                              DICT.get("tls201_patent_from_set_family"))
    patent_scope["key_appln_nr"] = patent_scope["appln_auth"] + patent_scope["appln_nr"] + \
                                   patent_scope["appln_kind"] + patent_scope["receiving_office"]
    patent_scope.to_csv("patent_scope.csv", sep='|', index=False)

    return patent_scope


if __name__ == "__main__":
    main()
