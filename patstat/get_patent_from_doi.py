import pandas as pd
import os
import dtypes_patstat_declaration as types
import logging
import sys
from pymongo import MongoClient
import json

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
URI = os.getenv("MONGO_CITPAT")

client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)

global db
db = client["citpat"]


def get_logger(name):
    """
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


def get_common_doi(lste_doi: list) -> list:
    plog = get_logger("Get common DOI")
    plog.info("Query DOI")
    liste_res = []

    for di in db["doi_pat_publn"].find({"doi": {"$in": lste_doi}}, {"_id": 0}):
        liste_res.append(di)

    plog.info("Write JSON DOI")
    with open(f"{DATA_PATH}common_doi/doi_patstat.json", "w") as f:
        json.dump(liste_res, f, indent=1)

    liste = []
    for res in liste_res:
        rs = res.get("cited_npl_publn_id")
        liste.append(rs)

    plog.info("End query common DOI")

    return liste


def get_pat_publn_id(lste_citid: list) -> list:
    plog = get_logger("Get PAT_PUBLN_ID")

    liste_cited = []

    for npl in db["tls214_npl_publn"].find({"npl_publn_id": {"$in": lste_citid}}, {"_id": 0}):
        liste_cited.append(npl)

    plog.info("Write JSON PAT_PUBLN_ID")
    with open(f"{DATA_PATH}common_doi/pat_publnid_patstat.json", "w") as f:
        json.dump(liste_cited, f, indent=1)

    liste = []
    for res in liste_cited:
        rs = res.get("pat_publn_id")
        liste.append(rs)

    plog.info("End query PAT_PUBLN_ID")

    return liste


def get_appln_id(lste_patpid: list) -> list:
    plog = get_logger("Get APPLN_ID")
    liste_patpnid = []

    for pnid in db["tls211_pat_publn"].find({"pat_publn_id": {"$in": lste_patpid}}):
        liste_patpnid.append(pnid)

    plog.info("Write JSON APPLN_ID")
    with open(f"{DATA_PATH}common_doi/pat_publnid_patstat.json", "w") as f:
        json.dump(liste_patpnid, f, indent=1)

    liste = []
    for res in liste_patpnid:
        rs = res.get("appln_id")
        liste.append(rs)

    plog.info("End query APPLN_ID")

    return liste


def get_patents_common_doi(liste_doi: list):
    plog = get_logger("get_patents_common_doi")
    plog.info("Create directory")
    new_directory = f'{DATA_PATH}common_doi'
    os.system(f'rm -rf {new_directory}')
    os.system(f'mkdir -p {new_directory}')

    liste_cdoi = get_common_doi(liste_doi)

    liste_cited = get_pat_publn_id(liste_cdoi)

    liste_applnid = get_pat_publn_id(liste_cited)

    plog.info("Get French patents")
    patents = pd.read_csv(f"{DATA_PATH}patents.csv", sep="|", encoding="utf-8", engine="python",
                          dtype=types.patent_types)

    compat = patents.loc[patents["appln_id"].isin(liste_applnid)]

    compat.to_csv(f"{DATA_PATH}french_patents_doi.csv", sep="|", encoding="utf-8", index=False)

    compat.to_json("fam_final_json.jsonl", orient="records", lines=True)
