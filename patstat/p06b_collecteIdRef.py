#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from retry import retry
import concurrent.futures
import logging
import threading
import sys
from patstat import pydref as pdref
from patstat import dtypes_patstat_declaration as types
from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# This program harvests inventors' IdRef based on their names via the ABES API.


def get_logger(name):
    """
    This function helps to follow the execution of the parallel computation.
    """
    loggers = {}
    if name in loggers:
        return loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    loggers[name] = logger
    return loggers[name]


def res_futures(dict_nb: dict) -> pd.DataFrame:
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=11, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                data = future.result()
                res.append(data)
                jointure = pd.concat(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)

    return jointure


@retry(tries=3, delay=5, backoff=5)
def query(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function queries the ABES API.
    It takes a df and, for each row, it matches the name to the names in the ABES API.
    It returns a df with the person ID, name and IdRef.
    """
    pydref = pdref.Pydref()
    logger = get_logger(threading.current_thread().name)
    logger.info("start")
    part_idref = {"person_id": [], "name_corrected": [], "idref": []}
    for _, r in df.iterrows():
        try:
            result = pydref.identify(r["name_corrected"])
            if result.get("status") == "found":
                part_idref["person_id"].append(r["person_id"])
                part_idref["name_corrected"].append(r["name_corrected"])
                part_idref["idref"].append(result.get("idref"))
        except:
            pass

    df = pd.DataFrame(data=part_idref)
    df["person_id"] = df["person_id"].astype(pd.Int64Dtype())
    logger.info("end")

    return df


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    prct10 = int(round(len(df) * 10 / 100, 0))
    dict_nb = {}
    deb = 0
    fin = prct10
    dict_nb["df1"] = df.iloc[deb:fin, :]
    deb = fin
    dixieme = 10 * prct10
    reste = (len(df) - dixieme)
    fin_reste = len(df) + 1
    for i in range(2, 11):
        fin = (i * prct10 + 1)
        dict_nb["df" + str(i)] = df.iloc[deb:fin, :]
        if reste > 0:
            dict_nb["reste"] = df.iloc[fin: fin_reste, :]
        deb = fin

    return dict_nb


def merge_idref(ori, dfref):
    ori2 = ori.rename(columns={"idref": "idref_original"})
    ori2 = pd.merge(ori2, dfref, on=["person_id", "name_corrected"], how="left")

    ori2.loc[(ori2["idref_original"].isna()) & (ori2["idref"].notna()), "idref2"] = ori2.loc[
        (ori2["idref_original"].isna()) & (ori2["idref"].notna()), "idref"]

    ori2.loc[ori2["idref_original"].notna(), "idref2"] = ori2.loc[
        ori2["idref_original"].notna(), "idref_original"]

    ori2 = ori2.drop(columns=["idref_original", "idref"]).rename(columns={"idref2": "idref"})

    return ori2


def collecte():
    os.chdir(DATA_PATH)
    part = pd.read_csv('part_individuals.csv', sep='|', encoding="utf-8", engine="python", dtype=types.part_entp_types)

    # if os.path.isfile("IdRef_identifies.csv"):
    #     idref = pd.read_csv("IdRef_identifies.csv", sep="|", encoding="utf-8", engine="python",
    #                         dtype=types.part_entp_types)
    #     part = merge_idref(part, idref)

    names = pd.DataFrame(
        part.loc[
            (part["idref"].isna()) & (part["name_corrected"].notna()), ["name_corrected",
                                                                        "person_id"]].drop_duplicates())
    names["length"] = names["name_corrected"].apply(lambda a: len(a.split()))
    names = names.loc[names["length"] > 1]
    names = names.drop(columns="length")
    names = names.reset_index().drop(columns="index")

    dict_subset_df = subset_df(names)

    df_futures = res_futures(dict_subset_df)

    part2 = merge_idref(part, df_futures)

    idref_identifies = pd.DataFrame(
        part.loc[
            (part["idref"].notna()) & (part["name_corrected"].notna()), ["name_corrected",
                                                                        "person_id", "idref"]].drop_duplicates())

    idref_identifies.to_csv("IdRef_identifies.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'IdRef_identifies.csv')

    part2.to_csv('part_individuals_p06b.csv', sep='|', index=False)