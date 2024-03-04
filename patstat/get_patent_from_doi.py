import pandas as pd
import os
from patstat import dtypes_patstat_declaration as types
from pymongo import MongoClient
from application.server.main.logger import get_logger
import json
from utils import swift

logger = get_logger(__name__)


def get_patents_common_doi(liste_doi: list):
    DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=43200000)

    db = client["citpat"]

    print(db.list_collection_names(), flush=True)

    logger.info("get_patents_common_doi")

    liste_res = []

    for di in db["doi_pat_publn"].find({"doi": {"$in": liste_doi}}, {"_id": 0}):
        liste_res.append(di)

    df_doi = pd.DataFrame(data=liste_res)

    liste_cdoi = list(df_doi["cited_npl_publn_id"].unique())

    logger.info("get_pat_publn_id")
    liste_cited = []

    for npl in db["tls212_citation"].find({"cited_npl_publn_id": {"$in": liste_cdoi}}, {"_id": 0}):
        liste_cited.append(npl)

    df_cited = pd.DataFrame(data=liste_cited)

    liste_patpid = []
    for res in liste_cited:
        rs = res.get("pat_publn_id")
        liste_patpid.append(rs)

    logger.info("End query PAT_PUBLN_ID")

    logger.info("get_appln_id")

    liste_patpnid = []

    for pnid in db["tls211_pat_publn"].find({"pat_publn_id": {"$in": liste_patpid}}, {"_id": 0}):
        liste_patpnid.append(pnid)

    df_applnid = pd.DataFrame(data=liste_patpnid)

    liste_applnid = []
    for res in liste_patpnid:
        rs = res.get("appln_id")
        liste_applnid.append(rs)

    logger.info("End query APPLN_ID")

    logger.info("Get French patents")
    patents = pd.read_csv(f"{DATA_PATH}patent.csv", sep="|", encoding="utf-8", engine="python",
                          dtype=types.patent_types)

    compat = patents.loc[patents["appln_id"].isin(liste_applnid)]

    logger.info("Merge")

    doi_cited = pd.merge(df_doi, df_cited[["cited_npl_publn_id", "pat_publn_id"]], how="inner",
                         on="cited_npl_publn_id").drop_duplicates().reset_index(drop=True)
    doi_cited_appln = pd.merge(doi_cited, df_applnid[["pat_publn_id", "appln_id"]], how="inner",
                               on="pat_publn_id").drop_duplicates().reset_index(drop=True)

    logger.info("Final DF")
    pat_doi = pd.merge(doi_cited_appln[["doi", "appln_id"]],
                       compat[["appln_id", "docdb_family_id", "appln_publn_number", "appln_auth"]], how="inner",
                       on="appln_id").drop(columns="appln_id").drop_duplicates().reset_index(drop=True)

    logger.info("Final dict")
    dict_pat = pat_doi.to_dict(orient="records")
    with open("french_patents_doi.json", "w") as f:
        json.dump(dict_pat, f)

    swift.upload_object('patstat', 'french_patents_doi.json')

