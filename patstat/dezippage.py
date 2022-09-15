#!/usr/bin/env python
# coding: utf-8

# This script takes the zipped folders from PATSTAT Global, unzip them until they are CSV and
# place the CSVs into folders
# which coressponds to PATSTAT Global database tables.
# Each folders "data_PATSTAT_Gobal..." contain subfolders "tls\d{3}_part\d{2}"
# that contain the CSVs (1 CSV by subfolder).
# The first part "tls\d{3}" of the CSV name correponds to a table of PATSTAT Global database.
# A table may be split into several CSVs.

import glob
import os
import pandas as pd
import re
import shutil
import zipfile

# directory where the folders are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
# VERSION = "2021_Autumn/"


# function to unzip the folders
def unzip_folders(pth: str, folders: list):
    os.chdir(pth)
    for folder in folders:
        zipfile.ZipFile(folder).extractall()


# function to select the files to unzip based on their names
def select_files(pth: str, pattern: str) -> list:
    zip_folds = glob.glob(pth+r"*.zip")
    list_folds = [_zip for _zip in zip_folds if re.match(pth+pattern, _zip)]
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


def unzip():
    path = DATA_PATH
    # selects the zipped folders to unzip (everything except the documentation)
    zipped_folders = select_files(path, r"data_PATSTAT_Global_\d+_.+\.zip")
    # unzips the folders
    unzip_folders(path, zipped_folders)

    # selects the subfolders
    subfolders = select_files(path, r"tls(204|211|201|206|207|209|225|224|203|202)_part\d{2}\.zip")
    # unzips the subfolders
    unzip_folders(path, subfolders)

    # removes the end of the subfolders to get table names from PATSTAT Global database
    table_names = list(map(lambda a: re.sub(r"_part\d+.zip", "", a), subfolders))

    # creates a dataframe to connect each file to its subfolder and table
    file_names = pd.DataFrame({"subfolders": subfolders, "table_names": table_names})
    file_names["file_names"] = file_names["subfolders"].apply(lambda a: re.sub(r".zip", ".csv", a), 1)

    # moves each file into a folder corresponding to its table in PATSTAT Global database
    for item in range(len(file_names["subfolders"])):
        os.makedirs(file_names["table_names"][item], exist_ok=True)
        shutil.move(file_names["file_names"][item], file_names["table_names"][item])

    delete_files(DATA_PATH, r"index_documentation_scripts_PATSTAT_Global_*")

    delete_files(DATA_PATH, r"data_PATSTAT_Global_*")

    fichiers = os.listdir(DATA_PATH)
    fichiers_zip = [fl for fl in fichiers if re.match(r".+tls\d{3}_part\d{2}\.zip", fl)]
    for fichier in fichiers_zip:
        print(fichier, flush=True)
        os.remove(fichier)


