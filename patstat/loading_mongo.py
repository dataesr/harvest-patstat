import os
from pymongo import MongoClient
import logging
import sys
from patstat import dtypes_patstat_declaration as types
import pandas as pd
import glob
from datetime import datetime as dt

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")

client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)

db = client['citation-patstat']

for item in ["tls211_pat_publn", "tls212_citation", "tls214_npl_publn", "tls215_citn_categ"]:
    db[item].drop()

client.close()


DICT = {
    "tls211": {'sep': ',', 'chunksize': 1000000, 'engine': "python", "dtype": types.tls211_types},
    "tls212": {'sep': ',', 'chunksize': 1000000, 'engine': "python", "dtype": types.tls212_types},
    "tls214": {'sep': ',', 'chunksize': 1000000, 'engine': "python", "dtype": types.tls214_types},
    "tls215": {'sep': ',', 'chunksize': 1000000, 'engine': "python", "dtype": types.tls215_types}
}


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


def csv_file_querying(folder: str, chunk_query, dict_param_load_csv: dict):
    files_list = glob.glob(folder + "/*.csv")
    print(f"query beginning {folder}: ", dt.now())
    for file in files_list:
        df_chunk = pd.read_csv(file, sep=dict_param_load_csv.get('sep'), chunksize=dict_param_load_csv.get('chunksize'),
                               usecols=dict_param_load_csv.get('usecols'), dtype=dict_param_load_csv.get('dtype'))
        print(f"query beginning {file}: ", dt.now())
        chunk_result_list = list(map(lambda chunk: chunk_query(chunk), df_chunk))
        print(f"end of query {file} at : ", dt.now())
    print(f"end beginning {folder}: ", dt.now())


def chunk_function_211(chunk):
    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
    db = client['citation-patstat']
    tls211 = db.tls211_pat_publn
    tls211.create_index([("pat_publn_id", "text")], unique=True)
    chunk = chunk.loc[chunk["pat_publn_id"] > 0]
    js = chunk.to_dict(orient='records')
    x = tls211.insert_many(js)
    client.close()


def chunk_function_212(chunk):
    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
    db = client['citation-patstat']
    tls212 = db.tls212_citation
    tls212.create_index([("pat_publn_id", "text"), ("citn_replenished", "text"), ("citn_id", "text")], unique=True)
    chunk = chunk.loc[chunk["pat_publn_id"] > 0]
    js = chunk.to_dict(orient='records')
    x = tls212.insert_many(js)
    client.close()


def chunk_function_214(chunk):
    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
    db = client['citation-patstat']
    tls214 = db.tls214_npl_publn
    tls214.create_index([("cited_npl_publn_id", "text")], unique=True)
    chunk = chunk.loc[chunk["npl_publn_id"] != "0"]
    chunk = chunk.rename(columns={'npl_publn_id': 'cited_npl_publn_id'})
    js = chunk.to_dict(orient='records')
    x = tls214.insert_many(js)
    client.close()


def chunk_function_215(chunk):
    client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
    db = client['citation-patstat']
    tls215 = db.tls215_citn_categ
    tls215.create_index(
        [("pat_publn_id", "text"), ("citn_replenished", "text"), ("citn_id", "text"), ("citn_categ", "text"),
         ("relevant_claim", "text")], unique=True)
    chunk = chunk.loc[chunk["pat_publn_id"] > 0]
    js = chunk.to_dict(orient='records')
    x = tls215.insert_many(js)
    client.close()


def load_data():
    os.chdir(DATA_PATH)

    t211 = get_logger("Loading table 211")
    t211.info("Start loading table 211")
    csv_file_querying("tls211", chunk_function_211, DICT["tls211"])
    t211.info("End loading table 211")

    t212 = get_logger("Loading table 212")
    t212.info("Start loading table 212")
    csv_file_querying("tls212", chunk_function_212, DICT["tls212"])
    t212.info("End loading table 212")

    t214 = get_logger("Loading table 214")
    t214.info("Start loading table 214")
    csv_file_querying("tls214", chunk_function_214, DICT["tls214"])
    t214.info("End loading table 214")

    t215 = get_logger("Loading table 215")
    t215.info("Start loading table 215")
    csv_file_querying("tls215", chunk_function_215, DICT["tls215"])
    t215.info("End loading table 215")

    client.close()
