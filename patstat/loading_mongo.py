import os
import logging
import sys
from pymongo import MongoClient
from retry import retry
from timeit import default_timer as timer

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

URI = os.getenv("MONGO_CITPAT")

client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)

global db
db = client['citation-patstat']


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


@retry(delay=200, tries=3)
def mongo_import_collection(collection_name: str, folder: str):
    """Import collection data into mongo db.

    Args:
        collection_name (_type_): _description_
        chunk (_type_): _description_
    """

    impmongo = get_logger(f"Import mongo {collection_name}")

    os.chdir(DATA_PATH + folder)

    files = os.listdir()

    for file in files:
        # Import to mongo
        import_timer = timer()
        impmongo.info(f"File {file} loading started")
        mongoimport = f"mongoimport '{URI}' --db='citpat' --collection='{collection_name}' --file='{file}' \
         --type='csv' --headerline --authenticationDatabase=admin"
        os.system(mongoimport)
        impmongo.info(f"File {file} loaded in {(timer() - import_timer):.2f}")

    impmongo.info(f"Folder {folder} finished loading")


def load_data():

    t211 = get_logger("Loading table 211")
    t211.info("Start loading table 211")
    mongo_import_collection("tls211_pat_publn", "tls211")
    db["tls211_pat_publn"].delete_many({"pat_publn_id": 0})
    t211.info("End loading table 211")

    t212 = get_logger("Loading table 212")
    t212.info("Start loading table 212")
    mongo_import_collection("tls212_citation", "tls212")
    db["tls212_citation"].delete_many({"pat_publn_id": 0})
    t212.info("End loading table 212")

    t214 = get_logger("Loading table 214")
    t214.info("Start loading table 214")
    mongo_import_collection("tls214_npl_publn", "tls214")
    db["tls214_npl_publn"].delete_many({"npl_publn_id": "0"})
    t214.info("End loading table 214")

    t215 = get_logger("Loading table 215")
    t215.info("Start loading table 215")
    mongo_import_collection("tls215_citn_categ", "tls215")
    db["tls215_citn_categ"].delete_many({"pat_publn_id": 0})
    t215.info("End loading table 215")

    client.close()
