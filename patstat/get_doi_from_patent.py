import pandas as pd
import os
from patstat import dtypes_patstat_declaration as types
from application.server.main.logger import get_logger
from utils import swift

from patstat import csv_files_querying as cfq

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

logger = get_logger(__name__)

DICT = {"tls211": {'sep': ',', 'chunksize': 10000000, 'dtype': types.tls211_types},
        "tls212": {'sep': ',', 'chunksize': 10000000, 'dtype': types.tls212_types},
        "tls214": {'sep': ',', 'chunksize': 10000000, 'dtype': types.tls214_types}}


def get_patents_common_doi():
    os.chdir(DATA_PATH)

    logger.info("Get French patents")
    patents = pd.read_csv(f"patent.csv", sep="|", encoding="utf-8", engine="python",
                          dtype=types.patent_types)

    pat = patents[["key_appln_nr", "appln_id", "docdb_family_id"]].drop_duplicates().reset_index(drop=True)

    tls211 = cfq.filtering("tls211", patents, "appln_id", DICT["tls211"])
    tls211 = pd.merge(tls211, pat, how="left", on="appln_id")
    tls211id = tls211[["key_appln_nr", "appln_id", "docdb_family_id", "pat_publn_id"]].drop_duplicates().reset_index(
        drop=True)

    doi = pd.read_json("link_publ_doi/link_publication_doi.json")

    tls212 = cfq.filtering("tls212", doi, "cited_npl_publn_id", DICT["tls212"])
    tls212 = tls212.loc[tls212["pat_publn_id"].isin(tls211["pat_publn_id"])]
    tls212["npl_publn_id"] = tls212["cited_npl_publn_id"].copy()
    tls212 = pd.merge(tls212, tls211id, on="pat_publn_id", how="left")
    tls212id = tls212[
        ["key_appln_nr", "appln_id", "docdb_family_id", "pat_publn_id", "npl_publn_id"]].drop_duplicates().reset_index(
        drop=True)

    tls214 = cfq.filtering("tls214", tls212, "npl_publn_id", DICT["tls214"])
    tls214 = pd.merge(tls214, tls212id, on="npl_publn_id", how="left")
    tls214 = tls214.drop_duplicates().reset_index(drop=True)

    tls214.to_csv("publi_brevets.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'publi_brevets.csv')
