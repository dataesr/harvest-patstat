# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de requêter l'API de Patstat et de télécharger les fichiers zippés.
# This script collects PATSTAT Global data from EPO's API, download and write zipped folders

import os
import re
import shutil
import concurrent.futures
import logging
import threading
import sys
import glob

import pandas as pd
from retry import retry

import requests

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

URL_PATSTAT = "https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api/"
URL_FILES = "external/subscribedProducts/17"
URL_LOADING = "products/17"
URL_BDDS = "https://login.epo.org/oauth2/aus3up3nz0N133c0V417/v1/token"


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


# fonction pour faire les requêtes GET sur l'API Patstat
# function that produces a GET request and check if it's successful
def get_url(url: str, tkn: str, strm: bool):
    response = requests.get(url, headers={"Authorization": tkn}, stream=strm)
    status = response.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed", flush=True)
    return response


# fonction pour s'authentifier sur l'API Patstat
# function to authenticate on PATSTAT API
def connexion_api():
    res = requests.post(URL_BDDS, headers={'Authorization': os.getenv("Basic"),
                                          'Content-Type': 'application/x-www-form-urlencoded'},
                        data={'grant_type': 'password', 'username': os.getenv("username"),
                              "password": os.getenv("password"),
                              "scope": "openid"})
    status = res.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed", flush=True)
    res_json = res.json()
    tkn = f"{res_json.get('token_type')} {res_json.get('access_token')}"
    return tkn


def ed_number(url_17: str, tkn: str, strm: bool) -> dict:
    ed = get_url(url_17, tkn, strm).json()
    edit = ed.get("deliveries")[0].get("deliveryId")
    list_keys = list(ed.get("deliveries")[0].keys())
    res = [re.match(r"^(?!delivery).+", l) for l in list_keys]
    tag_fi = [m.group(0) for m in res if m][0]
    fils = ed.get("deliveries")[0].get(tag_fi)
    dict_pal = {}
    for i in range(len(fils)):
        n = str(i + 1)
        key = "df" + n
        df = pd.DataFrame(
            data={"nb": [fils[i].get("itemId")], "Name": [fils[i].get("itemName")], "edition": [edit], "tkn": [token]})
        dict_pal[key] = df

    return dict_pal


@retry(tries=3, delay=5, backoff=5)
# fonction pour télécharger les fichiers zip et les enregistrer en local
# function to dowload, name and write the zipped folders
def download_write(df: pd.DataFrame):
    logger = get_logger(threading.current_thread().name)
    logger.info("start")
    for _, r in df.iterrows():
        ed = r.edition
        print(ed)
        nb = r.nb
        url = f"{URL_PATSTAT}{URL_LOADING}/delivery/{ed}/file/{nb}/download"
        print(url)
        name = r.Name
        print(name)
        tkn = r.tkn
        req = get_url(url, tkn, True)
        with open(name, "wb") as code:
            shutil.copyfileobj(req.raw, code)
    logger.info("end")


def res_futures(dict_nb: dict, query):
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=11, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)



def delete_folders(pth, reg, reg2):
    folds = glob.glob(pth + reg)
    res = [fld for fld in folds if re.match(reg2, fld)]
    if res:
        print(res, flush=True)
        for fld in res:
            shutil.rmtree(fld)


def delete_files(pth, reg):
    files = glob.glob(pth + reg)
    files.sort()
    print(files, flush=True)
    if files:
        for file in files:
            print(file, flush=True)
            os.remove(file)


def harvest_patstat():
    # authentification
    # authentication
    token = connexion_api()

    dict_nb_name = ed_number(URL_PATSTAT + URL_FILES, token, False)

    # set working directory
    os.chdir(DATA_PATH)


    delete_folders(DATA_PATH, r"tls*", r"\/data\/tls\d{3}$")
    delete_files(DATA_PATH, r"tls*")
    delete_files(DATA_PATH, r"index_documentation_scripts_PATSTAT_Global_*")
    delete_files(DATA_PATH, r"data_PATSTAT_Global_*")

    list_dir = os.listdir(DATA_PATH)
    print(list_dir)

    # # téléchargement et écriture des fichiers zip
    # download and write zipped files
    res_futures(dict_nb_name, download_write)