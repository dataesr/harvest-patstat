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
DICT = {"sep": ",", "chunksize": 5000000, "dtype": {"appln_id": np.int64}}

# set working directory
os.chdir(DATA_PATH)

# load application ID for French persons post 2010 - output from p01
patent_scope = pd.read_csv("patent_scope.csv", sep="|", dtype=types.tls201_types)


# function to get titles or abstracts from application ID
def query_tit_res_from_set_appln_id(chunk: pd.DataFrame) -> pd.DataFrame:
    """
    :param chunk: a chunk from multi_csv_querying on which the function is applied
    :return: a subset of dataframe with filtered data
    """
    tit_res_chunk = chunk[chunk["appln_id"].isin(patent_scope["appln_id"])]
    return tit_res_chunk


def get_titles_abstracts_from_appln_id(directory: str, action: str) -> pd.DataFrame:
    """
    :param directory: directory where table files are
    :param action: collect titles or abstracts
    :return: a dataframe with all the data corresponding to the query
    """
    print(f"Start get {action} from application ID")
    _titles = cfq.multi_csv_files_querying(directory,
                                           lambda chunk: query_tit_res_from_set_appln_id(chunk),
                                           DICT)
    print(f"End get {action} from application ID")

    return _titles


def main():
    titles = get_titles_abstracts_from_appln_id("tls202", "titles")
    titles.to_csv("titles.csv", sep="|", index=False)
    abstracts = get_titles_abstracts_from_appln_id("tls203", "abstracts")
    abstracts.to_csv("abstracts.csv", sep="|", index=False)


if __name__ == "__main__":
    main()
