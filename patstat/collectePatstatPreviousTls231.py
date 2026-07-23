# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de requêter l'API de Patstat et de télécharger les fichiers zippés.
# This script collects PATSTAT Global data from EPO's API, download and write zipped folders

import glob
import os
import re
import shutil
import zipfile

import pandas as pd
import requests
from retry import retry

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types
from application.server.main.logger import get_logger

from utils import swift

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

URL_PATSTAT = "https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api/"
URL_FILES = "external/subscribedProducts/17"
URL_LOADING = "products/17"
URL_BDDS = "https://login.epo.org/oauth2/aus3up3nz0N133c0V417/v1/token"

# dictionary with pd.read_csv parameters
DICT = {"sep": ",", "chunksize": 5000000, "dtype": types.tls231_types}

logger = get_logger(__name__)


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
        logger.debug(f"Error code {status}")
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
    edit = ed.get("deliveries")[1].get("deliveryId")
    list_keys = list(ed.get("deliveries")[1].keys())
    res = [re.match(r"^(?!delivery).+", l) for l in list_keys]
    tag_fi = [m.group(0) for m in res if m][0]
    fils = ed.get("deliveries")[1].get(tag_fi)

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


# function to unzip the folders
def unzip_folders(pth: str, folders: list):
    os.chdir(pth)
    for folder in folders:
        zipfile.ZipFile(folder).extractall()


# function to select the files to unzip based on their names
def select_files(pth: str, pattern: str) -> list:
    zip_folds = glob.glob(pth + r"*.zip")
    list_folds = [_zip for _zip in zip_folds if re.match(pth + pattern, _zip)]
    if len(list_folds) < 1:
        raise ValueError("There are no zipped files in the folder")
    else:
        pass
    return list_folds


def delete_files(pth, reg):
    files = glob.glob(pth + reg)
    files.sort()
    print(files, flush=True)
    if files:
        for file in files:
            print(file, flush=True)
            os.remove(file)


def get_events_from_appln_id(directory: str, action: str, pat_sc: pd.DataFrame, colfilter: str,
                             dict_param_load: dict) -> pd.DataFrame:
    """
    This function gets the abstracts and titles corresponding to French applications since 2010.

    :param directory: tls231
    :param action: events
    :param pat_sc: df with application IDs to keep
    :param colfilter: column with the application IDs
    :param dict_param_load: dictionary with loading parameters
    :return: df with filtered data
    """

    print(f"Start get {action} from application ID")
    _lic = cfq.filtering(directory, pat_sc, colfilter, dict_param_load)
    print(f"End get {action} from application ID")

    return _lic


def harvest_tls231():
    # get edition number and the file id numbers and names
    logger.debug("Beginning loading tls231")
    edition, list_files = ed_number(URL_PATSTAT + URL_FILES)
    list_files = [_zip for _zip in list_files if
                  re.match(r"tls231_", _zip["itemName"])]

    # set working directory
    os.chdir(DATA_PATH)

    # # téléchargement et écriture des fichiers zip
    # download and write zipped files
    download_write(edition, list_files)

    logger.debug("End loading tls231\nBeginning unzipping tls231")

    path = DATA_PATH
    # selects the zipped folders to unzip
    zipped_folders = select_files(path, r"tls231_")
    # unzips the folders
    unzip_folders(path, zipped_folders)

    # lists all the CSV files and rename them (model tls\d{3}_part\d{2}
    list_csv = glob.glob(path + r"*.csv")
    list_csv = [_zip for _zip in list_csv if re.match(path + r"tls\d{3}.+\.csv", _zip)]
    for csv in list_csv:
        new_name = csv.split("_")[0] + "_" + csv.split("_")[-1]
        os.rename(csv, new_name)

    # removes the end of the folders to get table names from PATSTAT Global database
    table_names = list(map(lambda a: re.sub(r"_.+_?.+?_part\d+.zip", "", a), zipped_folders))

    # creates a dataframe to connect each file to its folder and table
    file_names = pd.DataFrame({"subfolders": zipped_folders, "table_names": table_names})
    file_names["file_names"] = file_names["subfolders"].apply(lambda a: a.split("_")[0] + "_" + a.split("_")[-1])
    file_names["file_names"] = file_names["file_names"].str.replace("zip", "csv", regex=False)

    # moves each file into a folder corresponding to its table in PATSTAT Global database
    for _, r in file_names.iterrows():
        os.makedirs(r["table_names"], exist_ok=True)
        shutil.move(r["file_names"], r["table_names"])

    delete_files(DATA_PATH, r"*.zip")

    logger.debug("End unzipping tls231\nBeginning get licensing events")

    patents = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.patent_types,
                          engine="python")

    res = requests.get("https://link.epo.org/web/coverage/weekly/EN-Legal-event-codes.xlsx")

    with open("legal_event_codes.xlsx", "wb") as f:
        f.write(res.content)

    codes = pd.read_excel("legal_event_codes.xlsx", engine="openpyxl")
    colonnes = list(codes.columns)
    colonnes2 = {col: col.lower().replace("-", "_").replace(" ", "_") for col in colonnes}
    colonnes2["Authority"] = "event_auth"
    codes = codes.rename(columns=colonnes2)
    for col in ["event_auth", "event_code", "influence", "description_eng", "description_ori", "event_class",
                "event_class_description", "st27_status_event_code"]:
        codes[col] = codes[col].str.strip()
    codes["lic"] = codes['description_eng'].str.contains("lice", case=False)
    codes["lic2"] = codes['event_class_description'].str.contains("lice", case=False)
    codes2 = codes.loc[(codes["lic"]) | (codes["lic2"])]
    codes2["auth_type"] = codes2["event_auth"] + "_" + codes2["event_code"]
    codes2.to_excel("legal_event_codes2.xlsx", engine="openpyxl", index=False)

    lic = get_events_from_appln_id("tls231", "events", patents, "appln_id", DICT)
    for col in ["event_auth", "event_code"]:
        lic[col] = lic[col].str.strip()
    lic["auth_type"] = lic["event_auth"] + "_" + lic["event_code"]

    licensee = lic.loc[lic["party_type"] == "LIC"]
    lic2 = lic.loc[~lic["event_id"].isin(licensee["event_id"])]
    lic3 = lic2.loc[lic2["auth_type"].isin(codes2["auth_type"])]
    licenses = pd.concat([lic3, licensee], ignore_index=True)
    licenses = pd.merge(licenses, patents[["appln_id", "key_appln_nr", "docdb_family_id"]], on="appln_id", how="inner")
    licenses.to_excel("licenses.xlsx", engine="openpyxl", index=False)
    swift.upload_object('patstat', 'licenses.xlsx')

    select = pd.merge(licenses, codes2, on=["event_auth", "event_code", "auth_type"], how="left")
    select.to_excel("licenses_codes.xlsx", engine="openpyxl", index=False)
    swift.upload_object('patstat', 'licenses_codes.xlsx')

    logger.debug("End get licensing events")
