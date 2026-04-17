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

    tls214["doi_clean"] = tls214["npl_doi"].str.replace(r"^doi\s?\:?", "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"^DOI\s?\:?", "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"\*\s?", "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"\*\s?", "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"HTTPS?\:\/\/DX\.DOI\.ORG\/", "", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"https?\:\/\/doi\.org\/", "", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"\:\s?", "", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"^\.org\/", "", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"^\/", "", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"(\.\?)|(\?\.)", ".", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"\/\?", "/", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"\?", "-", regex=True,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"https?\/\/onlinelibrary\.wiley\.com\/doi\/",
                                                          "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"https?\/\/\:rd\.springer\.com\/article\/",
                                                          "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace(r"\,availableonlineathttp\/\/www\.idealibrary\.comon",
                                                          "", regex=True, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.3390/molecules 171214205",
                                                          "10.3390/molecules171214205", regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1638/1042-7260(2000)031 0512ITCCOM!2.0.CO;2",
                                                          "10.1638/1042-7260(2000)031[0185:oioawd]2.0.co;2",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10. 1 387!ijdb.0929480w",
                                                          "10.1387/ijdb.092948ow", regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1101/ 2020.09.15.298257",
                                                          "10.1101/2020.09.15.298257", regex=False,
                                                          case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1016/ S2213-2600(20)30225-3",
                                                          "10.1016/s2213-2600(20)30225-3",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1016/j.surfcoat. 2008.05.04 7",
                                                          "10.1016/j.surfcoat.2008.05.047",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1295/POLYMJ.20.477 10.1295/POLYMJ.20.477",
                                                          "10.1295/polymj.20.477",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1016/j.petrol. 2016.07.00 2",
                                                          "10.1016/j.petrol.2016.07.002",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1016/j.ajpath. 2014.11.02 9",
                                                          "10.1016/j.ajpath.2014.11.029",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1081/SCC-200048974 10.1081/SCC-200048974",
                                                          "10.1081/SCC-200048974",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1517/13543776.2011.542412 CR",
                                                          "10.1517/13543776.2011.542412",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1007/s11771 010 0033 3",
                                                          "10.1007/s11771-010-0033-3",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.7326/M15-2994 pubmed26999484",
                                                          "10.7326/M15-2994",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.3109/14756369709027648 10.3109/14756369709027648",
                                                          "10.3109/14756369709027648",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1016/ j.tibtech.2013.04.004",
                                                          "10.1016/j.tibtech.2013.04.004",
                                                          regex=False, case=False)
    tls214["doi_clean"] = tls214["doi_clean"].str.replace("10.1016/j.ajpath. 2014.11.02 9",
                                                          "10.1016/j.ajpath.2014.11.029",
                                                          regex=False, case=False)

    tls214["doi"] = "https://doi.org/" + tls214["doi_clean"]

    tls214.to_csv("publi_brevets.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'publi_brevets.csv')
