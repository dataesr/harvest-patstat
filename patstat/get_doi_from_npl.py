import os
import re
import logging
import sys
import pandas as pd
import json
from timeit import default_timer as timer

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

DOI_REGEX = "10.\\d{4,9}/[-._;()/:a-z0-9A-Z]+"
DOI_PATTERN = re.compile(DOI_REGEX)

URI = os.getenv("MONGO_CITPAT")


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


def extract_dois(txt):
    return DOI_PATTERN.findall(txt)


def parse_npl(file: str):
    new_directory = f'{DATA_PATH}/link_publ_doi'
    os.system(f'rm -rf {new_directory}')
    os.system(f'mkdir -p {new_directory}')
    link_publication_doi = []
    df = pd.read_csv(f'{DATA_PATH}/tls214/{file}', chunksize=10000)
    dic_key = {}
    for c in df:
        for row in c.itertuples():
            current_publication_id = row.npl_publn_id
            if isinstance(row.npl_doi, str) and '10.' in row.npl_doi:
                dic_key[str(current_publication_id) + "_" + str(row.npl_doi.strip().lower())] = {
                    'cited_npl_publn_id': current_publication_id, 'doi': row.npl_doi.strip().lower()}
                if isinstance(row.npl_biblio, str):
                    doi_found = extract_dois(row.npl_biblio)

                if "doi_found" in locals():
                    for doi in doi_found:
                        dic_key[str(current_publication_id) + "_" + str(doi.strip().lower())] = {
                            'cited_npl_publn_id': current_publication_id, 'doi': doi.strip().lower()}

    for key in dic_key.keys():
        link_publication_doi.append(dic_key[key])

    with open(f"{DATA_PATH}/link_publ_doi/link_publication_doi.json", "w") as f:
        json.dump(link_publication_doi, f, indent=1)


def load_pbln_doi_to_mongo():
    """Import collection data into mongo db.

    """
    os.chdir(f"{DATA_PATH}/link_publ_doi/")

    impmongo = get_logger(f"Import mongo doi_pat_publn")

    import_timer = timer()
    impmongo.info(f"File link_publication_doi.json loading started")
    mongoimport = f"mongoimport '{URI}' --db='citpat' --collection='doi_pat_publn' --file='link_publication_doi.json' \
         --authenticationDatabase=admin"
    os.system(mongoimport)
    impmongo.info(f"File link_publication_doi.json loaded in {(timer() - import_timer):.2f}")

