import requests
import os
import pandas as pd
from patstat import dtypes_patstat_declaration as types
import numpy as np
import logging
import threading
import sys
import concurrent.futures
from retry import retry
from utils import swift

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")
url_upw = os.getenv("URL_UPW")
PUBLIC_API_PASSWORD = os.getenv("ENTITY_API")


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


def res_futures(dict_nb: dict, query) -> pd.DataFrame:
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    global jointure
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=1001, thread_name_prefix="thread") as executor:
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


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    prct10 = int(round(len(df) * 0.001, 0))
    dict_nb = {}
    df = df.reset_index().drop(columns="index")
    indices = list(df.index)
    listes_indices = [indices[i:i + prct10] for i in range(0, len(indices), prct10)]
    i = 1
    for liste in listes_indices:
        min_ind = np.min(liste)
        max_ind = np.max(liste) + 1
        dict_nb["df" + str(i)] = df.iloc[min_ind: max_ind, :]
        i = i + 1

    return dict_nb


@retry(tries=3, delay=5, backoff=5)
def get_ef(mytext, lng):
    global res
    global df
    text = mytext
    json = {
        "text": text,
        # "shortText": text,
        "language": {"lang": lng},
        "mentions": ["wikipedia", "ner"]
    }

    r = requests.post(f"{url_upw}/forward", json={
        'PUBLIC_API_PASSWORD': PUBLIC_API_PASSWORD,
        "url": "http://entity-fishing:5001/service/disambiguate",
        "params": json
    })
    if r.status_code == 202:
        res = r.json()
        if "message" in res or res == {}:
            pass
        elif "entities" not in res:
            pass
        else:
            df = pd.json_normalize(res["entities"]).drop(columns=["offsetEnd", "offsetStart"])
            if "domains" in df.columns:
                df = df.explode("domains").drop_duplicates().reset_index(drop=True)
            df = df.loc[df["confidence_score"] >= 0.5]
    else:
        pass

    return df


def req_ef(pt: pd.DataFrame) -> pd.DataFrame:
    liste_res = []

    logger = get_logger(threading.current_thread().name)
    logger.info("start query entity fishing")

    for row in pt.itertuples():
        txt = row.appln_abstract
        lg = row.appln_abstract_lg
        docdb = row.docdb_family_id

        tp = get_ef(txt, lg)
        tp["docdb_family_id"] = docdb
        print(f"Fishing for {docdb}")
        liste_res.append(tp)

    logger.info("end query entity fishing")

    df3 = pd.concat(liste_res, ignore_index=True)
    return df3


def get_ef():
    os.chdir(DATA_PATH)

    pat = pd.read_csv("patent.csv", sep="|", encoding="utf-8", engine="python", dtype=types.patent_types)

    auth = {}
    for r in pat.itertuples():
        lg = r.appln_abstract_lg
        fam = r.docdb_family_id
        key = r.key_appln_nr

        if fam not in auth:
            auth[fam] = {"appln_abstract_lg": [], "key_appln_nr": []}

        if lg in ["fr", "en"] and key not in auth[fam]["key_appln_nr"]:
            auth[fam]["appln_abstract_lg"].append(lg)
            auth[fam]["key_appln_nr"].append(key)

    liste_fr = []
    for key in auth.keys():
        tmp = pd.DataFrame(data=auth[key])
        tmp["docdb_family_id"] = key
        liste_fr.append(tmp)

    df1 = pd.concat(liste_fr, ignore_index=True)

    fr = df1.loc[df1["appln_abstract_lg"] == "fr"]

    pat3 = pat.loc[pat["key_appln_nr"].isin(fr["key_appln_nr"])].sort_values("docdb_family_id").reset_index(drop=True)

    df2 = df1.loc[~df1["key_appln_nr"].isin(fr["key_appln_nr"])]

    pat_autres = pat.loc[
        (pat["key_appln_nr"].isin(df2["key_appln_nr"])) & (
            ~pat["docdb_family_id"].isin(pat3["docdb_family_id"]))].sort_values("docdb_family_id").reset_index(
        drop=True)

    pat4 = pd.concat([pat3, pat_autres], ignore_index=True)
    pat5 = pat4[["docdb_family_id", "appln_abstract_lg", "appln_abstract"]].drop_duplicates().sort_values(
        "docdb_family_id").reset_index(drop=True)

    subpat_fr = subset_df(pat5)

    df_pat = res_futures(subpat_fr, req_ef)
    df_pat2 = df_pat.explode("domains").drop(columns="type").drop_duplicates().reset_index(drop=True)
    df_pat2.to_csv("pat_ef.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'pat_ef.csv')
