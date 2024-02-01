import os
import re
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
    
DOI_REGEX = "10.\\d{4,9}/[-._;()/:a-z0-9A-Z]+"
DOI_PATTERN = re.compile(DOI_REGEX)

def extract_dois(txt):
    return DOI_PATTERN.findall(txt)

def parse_npl():
    new_directory = f'{DATA_PATH}/link_publ_doi'
    os.system(f'rm -rf {new_directory}')
    os.system(f'mkdir - p {new_directory}')
    link_publication_doi = []
    for npl_file in f'{DATA_PATH}/tls214':
        df = pd.read_csv(f'{DATA_PATH}/tls214/{npl_file}', chunksize=10000)
        for c in df:
            for row in df.itertuples():
                current_publication_id = row.npl_publn_id
                if isinstance(row.npl_doi, str) and '10.' in row.npl_doi:
                    link_publication_doi.append({'publication_id': current_publication_id, 'doi': row.npl_doi.strip().lower()}
                if isinstance(row.npl_biblio, str):
                    doi_found = extract_dois(row.npl_biblio)
                    for doi in doi_found:
                        link_publication_doi.append({'publication_id': current_publication_id, 'doi': doi.strip().lower()}
            #TODO write link_publication_doi in a jsonl file in new_directory

def load_pbln_doi_to_mongo():
    #TODO

