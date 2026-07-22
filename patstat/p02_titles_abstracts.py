#!/usr/bin/env python
# coding: utf-8

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types

import numpy as np
import os
import pandas as pd

from scientific_text_cleaner import clean_text

# directory where the files are
# DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
DATA_PATH = "/run/media/julia/DATA/fall2025/"

# dictionary with pd.read_csv parameters
DICT = {"sep": ",", "chunksize": 5000000, "dtype": {"appln_id": np.int64}}


def get_titles_abstracts_from_appln_id(directory: str, action: str, pat_sc: pd.DataFrame, colfilter: str,
                                       dict_param_load: dict) -> pd.DataFrame:
    """
    This function gets the abstracts and titles corresponding to French applications since 2010.

    :param directory: either tls with abstracts (203) or with titles (202)
    :param action: abstacts or titles
    :param pat_sc: df with application IDs to keep
    :param colfilter: column with the application IDs
    :param dict_param_load: dictionary with loading parameters
    :return: df with filtered data
    """

    print(f"Start get {action} from application ID")
    _titles = cfq.filtering(directory, pat_sc, colfilter, dict_param_load)
    print(f"End get {action} from application ID")

    return _titles


def tit_abst():
    # load application ID for French persons post 2010 - output from p01

    # set working directory
    os.chdir(DATA_PATH)
    patent_scope = pd.read_csv("patent_scope.csv", sep="|", dtype=types.tls201_types)
    titles = get_titles_abstracts_from_appln_id("tls202", "titles", patent_scope, "appln_id", DICT)
    ttag = titles.loc[(titles["appln_title"].notna()) & (titles["appln_title"].str.contains(r"\<.+\>"))]
    ttag.loc[(ttag["appln_title"].notna()), "t2"] = ttag.loc[
        (ttag["appln_title"].notna()), "appln_title"].apply(clean_text)
    ttag.to_csv("ttag.csv", sep="|", index=False)
    # titles.to_csv("titles.csv", sep="|", index=False)
    abstracts = get_titles_abstracts_from_appln_id("tls203", "abstracts", patent_scope, "appln_id", DICT)
    atag = abstracts.loc[(abstracts["appln_abstract"].notna()) & (abstracts["appln_abstract"].str.contains(r"\<.+\>"))]
    atex = abstracts.loc[(abstracts["appln_abstract"].notna()) & (abstracts["appln_abstract"].str.contains(r"\$.*?\$"))]
    atex = pd.concat([atex, atag], ignore_index=True)
    atex = atex.drop_duplicates().reset_index(drop=True)
    atex.loc[(atex["appln_abstract"].notna()), "ab2"] = atex.loc[
        (atex["appln_abstract"].notna()), "appln_abstract"].apply(clean_text)
    atex.to_csv("atex.csv", sep="|", index=False)
    # abstracts.to_csv("abstracts.csv", sep="|", index=False)
