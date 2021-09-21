#!/usr/bin/env python
# coding: utf-8

import glob
import os
import pandas as pd
import re
from retry import retry
import shutil
import zipfile

DATA_PATH = "/run/media/julia/DATA/test/"


def dezippage_dossier(dossier):
    for dos in dossier:
        zipfile.ZipFile(dos).extractall()


def select_files(pattern):
    zip_folds = glob.glob(r"*.zip")
    list_folds = [_zip for _zip in zip_folds if re.match(pattern, _zip)]
    if len(list_folds) < 1:
        raise ValueError("There are no zipped files in the folder")
    else:
        pass
    return list_folds


def main():
    os.chdir(DATA_PATH)

    dossiers_zip = select_files(r"data_PATSTAT_Global_\d+_.+\.zip")
    dezippage_dossier(dossiers_zip)

    noms_tables = select_files(r"tls\d{3}_part\d{2}\.zip")
    dezippage_dossier(noms_tables)

    noms_dossiers = list(map(lambda a: re.sub(r"_part\d+.zip", "", a), noms_tables))

    dossier_fichiers = pd.DataFrame({"noms_tables": noms_tables, "noms_dossiers": noms_dossiers})
    dossier_fichiers["noms_fichiers"] = dossier_fichiers["noms_tables"].apply(lambda a: re.sub(r".zip", ".csv", a), 1)

    for item in range(len(dossier_fichiers["noms_tables"])):
        os.makedirs(dossier_fichiers["noms_dossiers"][item], exist_ok=True)
        shutil.move(dossier_fichiers["noms_fichiers"][item], dossier_fichiers["noms_dossiers"][item])


if __name__ == '__main__':
    main()
