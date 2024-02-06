import os
import re
import pandas as pd
import json
from pymongo import MongoClient

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

DOI_REGEX = "10.\\d{4,9}/[-._;()/:a-z0-9A-Z]+"
DOI_PATTERN = re.compile(DOI_REGEX)

client = MongoClient(host=os.getenv("MONGO_URI"), connect=True, connectTimeoutMS=360000)

global db
db = client['citation-patstat']

global doim
doim = db.doi_pat_publn
doim.delete_many({})
doim.create_index([("cited_npl_publn_id", "text"), ("doi", "text")])


def extract_dois(txt):
    return DOI_PATTERN.findall(txt)


def parse_npl(file: str):
    new_directory = f'{DATA_PATH}/link_publ_doi'
    os.system(f'rm -rf {new_directory}')
    os.system(f'mkdir - p {new_directory}')
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
                for doi in doi_found:
                    dic_key[str(current_publication_id) + "_" + str(doi.strip().lower())] = {
                        'cited_npl_publn_id': current_publication_id, 'doi': doi.strip().lower()}

    for key in dic_key.keys():
        link_publication_doi.append(dic_key[key])

    with open("link_publication_doi.json", "w") as f:
        json.dump(link_publication_doi, f)


    return link_publication_doi


def load_pbln_doi_to_mongo():
    liste_doi = parse_npl("tls214_part01.csv")
    x = doim.insert_many(liste_doi).inserted_ids
