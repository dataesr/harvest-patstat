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
DATA_PATH = "/run/media/julia/DATA/test/"


# function to unzip the folders
def unzip_folders(folder):
    for fold in folder:
        zipfile.ZipFile(fold).extractall()


# function to select the files to unzip based on their names
def select_files(pattern):
    zip_folds = glob.glob(r"*.zip")
    list_folds = [_zip for _zip in zip_folds if re.match(pattern, _zip)]
    if len(list_folds) < 1:
        raise ValueError("There are no zipped files in the folder")
    else:
        pass
    return list_folds


def main():
    # set working directory
    os.chdir(DATA_PATH)

    # selects the zipped folders to unzip (everything except the documentation)
    zipped_folders = select_files(r"data_PATSTAT_Global_\d+_.+\.zip")
    # unzips the folders
    unzip_folders(zipped_folders)

    # selects the subfolders
    subfolders = select_files(r"tls\d{3}_part\d{2}\.zip")
    # unzips the subfolders
    unzip_folders(subfolders)

    # removes the end of the subfolders to get table names from PATSTAT Global database
    table_names = list(map(lambda a: re.sub(r"_part\d+.zip", "", a), subfolders))

    # creates a dataframe to connect each file to its subfolder and table
    file_names = pd.DataFrame({"subfolders": subfolders, "table_names": table_names})
    file_names["file_names"] = file_names["subfolders"].apply(lambda a: re.sub(r".zip", ".csv", a), 1)

    # moves each file into a folder corresponding to its table in PATSTAT Global database
    for item in range(len(file_names["subfolders"])):
        os.makedirs(file_names["table_names"][item], exist_ok=True)
        shutil.move(file_names["file_names"][item], file_names["table_names"][item])


if __name__ == '__main__':
    main()
