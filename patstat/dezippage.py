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


def unzip():
    path = DATA_PATH
    # selects the zipped folders to unzip
    zipped_folders = select_files(path, r"tls(204|211|201|206|207|209|225|224|203|202|212|214)_")
    # unzips the folders
    unzip_folders(path, zipped_folders)

    # lists all the CSV files and rename them (model tls\d{3}_part\d{2}
    list_csv = glob.glob(path + r"*.csv")
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

    delete_files(DATA_PATH, r"*.txt")

    delete_files(DATA_PATH, r"*.zip")
