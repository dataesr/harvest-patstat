#!/usr/bin/env python
# coding: utf-8

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types
from utils import swift
import os
import pandas as pd

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# Columns to select in tls201
COL = {"patent_appln_columns": ["appln_id", "appln_nr_epodoc"]}
COL2 = {"key_id": ["appln_id", "appln_nr_epodoc", "appln_auth", "appln_nr", "appln_kind", "receiving_office"]}


DICT = {"tls201_all": {'sep': ',', 'chunksize': 20000000, 'usecols': COL.get("patent_appln_columns"),
                        'dtype': types.tls201_types},
        "tls201_id": {'sep': ',', 'chunksize': 20000000, 'usecols': COL2.get("key_id"),
                      'dtype': types.tls201_types},
        "tls207": {'sep': ',', 'chunksize': 10000000, 'dtype': types.tls207_types}
        }

# set working directory
os.chdir(DATA_PATH)

# appln_nr_epodoc is deprecated but serves as the ID for old_part.
# Attempt to find the application ID (appln_id) corresponding to the appln_nr_epodoc ID
# in order to add the alternate key from tls 201 (appln_auth, appln_nr, appln_king and receiving_office).

# load old_part file

swift.download_object('patstat', 'partfin.csv', 'partfin.csv')
old_part = pd.read_csv("partfin.csv",
                       sep='|',
                       dtype=types.partfin_types)

# load the columns appln_id and appln_nr_epodoc from tls201 without any filter on rows
def get_appln_id_eopdoc(t201directory: str) -> pd.DataFrame:
    """
    :param t201directory: takes directory where tls201 files are
    :return: filtered dataframe with only one column "appln_id"
    """
    print("Start get ID application & application nr EPODOC")
    dict_param_load = DICT.get("tls201_all")

    def query_patent_applications_t201(chunk_t201: pd.DataFrame) -> pd.DataFrame:
        """
        :param chunk_t201: a chunk from CSV
        :return: dataframe with filtered "appln_id"
        """
        patent_applications_query = chunk_t201[(chunk_t201['appln_id'].isin(chunk_t201['appln_id']))]
        return patent_applications_query

    patent_appln_chunk = cfq.multi_csv_files_querying(t201directory, query_patent_applications_t201, dict_param_load)
    print("End get ID application & application nr EPODOC")
    return patent_appln_chunk

tls201 = get_appln_id_eopdoc("tls201")

# rename old_part columns to make merging easier
old_part.rename(columns={"id_patent": "appln_nr_epodoc", "name_source": "person_name"}, inplace=True)

# merge old_part and data from tls201
# left join because we want to keep only the ID corresponding to the ones in old_part
df = pd.merge(old_part,
              tls201[["appln_id", "appln_nr_epodoc"]], on="appln_nr_epodoc",
              how="left")

# turn appln_id into pandas' Int64 because common int can't handle NAs
df["appln_id"] = df["appln_id"].astype(pd.Int64Dtype())

# select appln_id with missing values : 32 cases where no corresponding appln_nr_epodoc in tls201 and so no appln_id
nas = df["appln_id"][df["appln_id"].isna()==True]
nas = list(nas.index)
epodoc = df["appln_nr_epodoc"].loc[nas]

# we keep only the rows where appln_id is not missing
df2 = df[df["appln_id"].isna()==False]

tls201_id = cfq.filtering("tls201", df2, "appln_id", DICT["tls201_id"])

final = pd.merge(df2,
                 tls201_id,
                 on=["appln_id", "appln_nr_epodoc", "appln_auth"],
                 how="left")

final["key_appln_nr"] = final["appln_auth"] + final["appln_nr"] + final["appln_kind"] + final["receiving_office"]
final["key_appln_nr_person"] = final["key_appln_nr"] + "_" + final["person_id"].astype(str)
final.rename(columns={"appln_nr_epodoc": "id_patent", "person_name": "name_source"}, inplace=True)
final.to_csv("old_part_key.csv", sep="|", index=False, encoding="utf-8")

