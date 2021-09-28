#!/usr/bin/env python
# coding: utf-8

from datetime import datetime as dt
import dtypes_patstat_declaration as types
import glob
import os
import pandas as pd

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"

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


# Chargement des données multi-CSV et requêtes
def multi_csv_files_querying(files_directory, chunk_query, dict_param_load_csv):
    files_list = glob.glob(files_directory + "/*.csv")
    df = list(map(lambda a: csv_file_querying(a, chunk_query, dict_param_load_csv), files_list))
    ending_dataframe = pd.concat(df)
    return ending_dataframe


def csv_file_querying(csv_file, chunk_query, dict_param_load_csv):
    df_chunk = pd.read_csv(csv_file, sep=dict_param_load_csv.get('sep'), chunksize=dict_param_load_csv.get('chunksize'),
                           usecols=dict_param_load_csv.get('usecols'), dtype=dict_param_load_csv.get('dtype'),
                           nrows=dict_param_load_csv.get('nrows'))
    print("query beginning : ", dt.now())
    chunk_result_list = list(map(lambda chunk: chunk_query(chunk), df_chunk))
    print("end of query number at : ", dt.now())
    dataframe_result = pd.concat(chunk_result_list)

    return dataframe_result


# tls 201 ne garde que les brevets après 2010
def get_patent_post2010_appln_id(t201directory):
    dict_param_load = DICT.get("tls201_post2010")

    patent_appln_chunk = multi_csv_files_querying(t201directory, query_patent_applications_t201, dict_param_load)

    return patent_appln_chunk


def query_patent_applications_t201(chunk_t201):
    patent_applications_query = chunk_t201[(chunk_t201['appln_kind'].isin(['A ', 'W ', 'P '])) &
                                           (chunk_t201['earliest_filing_year'] < 9999) &
                                           (chunk_t201['earliest_publn_year'] < 9999) &
                                           (chunk_t201['earliest_filing_year'] > 2009) &
                                           (chunk_t201['ipr_type'] == "PI")][['appln_id']]
    return patent_applications_query


patent_appln_id = get_patent_post2010_appln_id("tls201")


# tls 206 : ne garde que les ID des personnes "françaises"
def get_french_person_id(t206directory):
    person_columns = ["person_id", "person_ctry_code"]
    dict_param_load = {'sep': ',', 'chunksize': 5000000, 'usecols': person_columns, 'dtype': types.tls206_types}

    french_chunk = multi_csv_files_querying(t206directory, query_french_persons_t206, dict_param_load)
    # multi_csv_files_querying get all csv files from a directory and applies the query

    return french_chunk


def query_french_persons_t206(chunk_t206):
    # liste des pays français avec DOM-TOM
    list_french_codes = ["FR", "GP", "GF", "MQ", "RE", "YT", "PM", "MF", "BL", "WF", "TF", "NC", "PF"]
    french_query = query_country_t206(chunk_t206, list_french_codes)
    return french_query


def query_country_t206(chunk_t206, country_list):
    country_query = chunk_t206.loc[chunk_t206['person_ctry_code'].isin(country_list), ['person_id']].drop_duplicates()
    return country_query


french_person_id = get_french_person_id("tls206")

french_person_id = french_person_id[["person_id"]]


# tls 207 : charge les applications et ne garde que celles dont l'application des personnes qui demandent le brevet

def get_scope_applications(t207directory):
    def chunk_query(chunk):
        return query_patent_applicant_fr(chunk)
    dict_param_load = {'sep': ',', 'chunksize': 20000000, 'dtype': types.tls207_types}
    _scope_applications = multi_csv_files_querying(t207directory, chunk_query, dict_param_load)

    return scope_applications


def query_patent_applicant_fr(chunk_t207):
    chunk_trie = chunk_t207[(chunk_t207['applt_seq_nr'] > 0)]
    return chunk_trie


scope_app = get_scope_applications("tls207")
scope_app = scope_app[["person_id", "appln_id"]]

scope_2 = pd.merge(patent_appln_id, scope_app, how="inner", on="appln_id", sort=False,
                   validate="1:m")

# ne garde que les ID des applications des personnes "françaises"
# en faisant une jointure sur les ID personnes "françaises"
scope_applications = pd.merge(french_person_id, scope_2, how="inner", on="person_id", sort=False,
                              validate="1:m")


# tls 201

def query_families_from_set_patent(chunk_t201, set_patent):
    chunk_trie = chunk_t201[(chunk_t201['appln_id'].isin(set_patent))][['docdb_family_id']].drop_duplicates()
    return chunk_trie


def get_families_from_set_patent(t201directory, _set_patent):
    def chunk_query(chunk, _set_patent):
        return query_families_from_set_patent(chunk, _set_patent)
    usecols = ["appln_id", "docdb_family_id"]
    dict_param_load = {'sep': ',', 'chunksize': 20000000, 'dtype': types.tls201_types, 'usecols': usecols}
    _family_scope = multi_csv_files_querying(t201directory, chunk_query, dict_param_load)

    return _family_scope


family_scope = get_families_from_set_patent("tls201", scope_applications['appln_id'])


def get_patent_from_set_family(t201directory, set_family, nrows=None):
    def chunk_query(chunk):
        return query_patent_from_set_family(chunk, set_family)
    dict_param_load = {'sep': ',', 'chunksize': 2000000, 'dtype': types.tls201_types, 'nrows': nrows}
    _patent_scope = multi_csv_files_querying(t201directory, chunk_query, dict_param_load)

    return _patent_scope


def query_patent_from_set_family(chunk_t201, set_family):
    chunk_trie = chunk_t201[chunk_t201['docdb_family_id'].isin(set_family)]

    return chunk_trie


patent_scope = get_patent_from_set_family("tls201", family_scope["docdb_family_id"])
