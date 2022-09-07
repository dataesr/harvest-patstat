import os
import pandas as pd
from retry import retry
import concurrent.futures
import logging
import threading
import sys
from patstat import pydref as pdref
from patstat import dtypes_patstat_declaration as types

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def get_logger(name):
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


def res_futures(dict_nb):
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
def query(df):
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


def subset_df(df):
    prct10 = int(round(len(df) * 10 / 100, 0))
    dict_nb = {}
    deb = 0
    fin = prct10
    dict_nb["df1"] = df.iloc[deb:fin, :]
    deb = fin + 1
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


def collecte():
    os.chdir(DATA_PATH)

    part = pd.read_csv('part_individuals.csv', sep='|', encoding="utf-8", dtype=types.part_entp_types)

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

    part2 = part.rename(columns={"idref": "idref_original"})
    part2 = pd.merge(part2, df_futures, on=["person_id", "name_corrected"], how="left")

    part2.loc[(part2["idref_original"].isna()) & (part2["idref"].notna()), "idref2"] = part2.loc[
        (part2["idref_original"].isna()) & (part2["idref"].notna()), "idref"]

    part2.loc[part2["idref_original"].notna(), "idref2"] = part2.loc[
        part2["idref_original"].notna(), "idref_original"]

    part2 = part2.drop(columns=["idref_original", "idref"]).rename(columns={"idref2": "idref"})

    part2.to_csv('part_individuals_p06b.csv', sep='|', index=False)
