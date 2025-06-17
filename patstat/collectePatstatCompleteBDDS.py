# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de requêter l'API de Patstat et de télécharger les fichiers zippés.
# This script collects PATSTAT Global data from EPO's API, download and write zipped folders

import os
import re
import shutil
import glob

from retry import retry

import requests

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

URL_PATSTAT = "https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api/"
URL_FILES = "external/subscribedProducts/17"
URL_LOADING = "products/17"
URL_BDDS = "https://login.epo.org/oauth2/aus3up3nz0N133c0V417/v1/token"


def get_url(url: str, tkn: str, strm: bool):
    """
    fonction pour faire les requêtes GET sur l'API Patstat
    function that produces a GET request and check if it's successful
    url: string
    tkn: token, get it with authentication function
    strm: boolean, stream data from the API or not
    """
    response = requests.get(url, headers={"Authorization": tkn}, stream=strm)
    status = response.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed", flush=True)
    return response



def connexion_api():
    """
    fonction pour s'authentifier sur l'API Patstat
    function to authenticate on PATSTAT API
    Output: token which is needed to query the API - max duration = 1hr
    """
    res = requests.post(URL_BDDS, headers={'Authorization': os.getenv("AUTHORIZATION"),
                                          'Content-Type': 'application/x-www-form-urlencoded'},
                        data={'grant_type': 'password', 'username': os.getenv("USERNAME"),
                              "password": os.getenv("PASSWORD"),
                              "scope": "openid"})
    status = res.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to authenticate")
    else:
        print("Successfully authenticated", flush=True)
    res_json = res.json()
    tkn = f"{res_json.get('token_type')} {res_json.get('access_token')}"
    return tkn


def ed_number(url_17: str):
    """
    Authenticate, get edition number and infos on the files
    url_17: URL of the page with infos on the PATSTAT files
    Get edition number and dictionary with info on the files
    """
    tkn = connexion_api()
    ed = get_url(url_17, tkn, False).json()
    edit = ed.get("deliveries")[0].get("deliveryId")
    list_keys = list(ed.get("deliveries")[0].keys())
    res = [re.match(r"^(?!delivery).+", l) for l in list_keys]
    tag_fi = [m.group(0) for m in res if m][0]
    fils = ed.get("deliveries")[0].get(tag_fi)

    return edit, fils


@retry(tries=3, delay=5, backoff=5)
def download_write(ed: int, liste: list):
    """
    # fonction pour télécharger les fichiers zip et les enregistrer en local
    # function to dowload, name and write the zipped folders
    ed: edition number
    liste: list of dictionaries with info on the files
    Since token lasts for 1hr and files are heavy, re-authenticate and get new token for every file
    """
    for item in liste:
        print(ed)
        nb = item.get("itemId")
        url = f"{URL_PATSTAT}{URL_LOADING}/delivery/{ed}/file/{nb}/download"
        print(url)
        name = item.get("itemName")
        print(name)
        tkn = connexion_api()
        req = get_url(url, tkn, True)
        with open(name, "wb") as code:
            shutil.copyfileobj(req.raw, code)
    print("All the files have been successfully loaded.", flush=True)


def delete_folders(pth, reg, reg2):
    """
    Delete folders based on their name (regex)
    """
    folds = glob.glob(pth + reg)
    res = [fld for fld in folds if re.match(reg2, fld)]
    if res:
        print(res, flush=True)
        for fld in res:
            shutil.rmtree(fld)


def delete_files(pth, reg):
    """
    Delete files based on their name (regex)
    """
    files = glob.glob(pth + reg)
    files.sort()
    print(files, flush=True)
    if files:
        for file in files:
            print(file, flush=True)
            os.remove(file)


def harvest_patstat():
    # get edition number and the file id numbers and names
    edition, list_files = ed_number(URL_PATSTAT + URL_FILES)
    list_files = [_zip for _zip in list_files if
                  re.match(r"tls(204|211|201|206|207|209|225|224|203|202|212|214)_", _zip["itemName"])]

    # set working directory
    os.chdir(DATA_PATH)


    # remove folders and files previous run
    delete_folders(DATA_PATH, r"tls*", r"\/data\/tls\d{3}$")
    delete_files(DATA_PATH, r"tls*")
    delete_files(DATA_PATH, r"index_documentation_scripts_PATSTAT_Global_*")
    delete_files(DATA_PATH, r"data_PATSTAT_Global_*")

    list_dir = os.listdir(DATA_PATH)
    print(list_dir)

    # # téléchargement et écriture des fichiers zip
    # download and write zipped files
    download_write(edition, list_files)