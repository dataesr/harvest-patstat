import os
from pymongo import MongoClient
from patstat import csv_files_querying as cfq
import logging
import sys

DATA_PATH = os.getenv("MOUNTED_VOLUME_TEST")

client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)
print(client.server_info(), flush=True)

global db
db = client['citation-patstat']

global tls211
tls211 = db.tls211_pat_publn
tls211.create_index([("pat_publn_id", "text")], unique=True)

global tls212
tls212 = db.tls212_citation
tls212.create_index([("pat_publn_id", "text"), ("citn_replenished", "text"), ("citn_id", "text")], unique=True)

global tls214
tls214 = db.tls214_npl_publn
tls214.create_index([("npl_publn_id", "text")], unique=True)

global tls215
tls215 = db.citn_categ
tls215.create_index(
    [("pat_publn_id", "text"), ("citn_replenished", "text"), ("citn_id", "text"), ("citn_categ", "text"),
     ("relevant_claim", "text")], unique=True)

DICT = {
    "tls": {'sep': ',', 'chunksize': 1000000, 'engine': "python"},
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


def chunk_function_211(chunk):
    js = chunk.to_dict(orient='records')
    x = tls211.insert_many(js, upsert=True).inserted_id

    return x


def chunk_function_212(chunk):
    js = chunk.to_dict(orient='records')
    x = tls212.insert_many(js, upsert=True).inserted_id

    return x


def chunk_function_214(chunk):
    js = chunk.to_dict(orient='records')
    x = tls211.insert_many(js, upsert=True).inserted_id

    return x


def chunk_function_215(chunk):
    js = chunk.to_dict(orient='records')
    x = tls211.insert_many(js, upsert=True).inserted_id

    return x


def incremental_loading(folder: str, chunk_function):
    patent_appln_chunk = cfq.multi_csv_files_querying(folder, chunk_function, DICT.get("tls"))


def load_data():
    os.chdir(DATA_PATH)

    t211 = get_logger("Loading table 211")
    t211.info("Start loading table 211")
    incremental_loading("tls211", chunk_function_211)
    t211.info("End loading table 211")

    t212 = get_logger("Loading table 212")
    t212.info("Start loading table 212")
    incremental_loading("tls212", chunk_function_212)
    t212.info("End loading table 212")

    t214 = get_logger("Loading table 214")
    t214.info("Start loading table 214")
    incremental_loading("tls214", chunk_function_214)
    t214.info("End loading table 214")

    t215 = get_logger("Loading table 215")
    t215.info("Start loading table 215")
    incremental_loading("tls215", chunk_function_215)
    t215.info("End loading table 215")
