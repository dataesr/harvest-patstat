#!/usr/bin/env python
# coding: utf-8

import os
import re
import pandas as pd
import shutil

from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def fun1():
    # swift.download_object('patstat', 'part_init_p05.csv', f'{DATA_PATH}part_init_p05_bckup.csv')
    # swift.download_object('patstat', 'part_p05.csv', f'{DATA_PATH}part_p05_bckup.csv')
    # swift.download_object('patstat', 'part_p08.csv', f'{DATA_PATH}part_p08_bckup.csv')
    # swift.download_object('patstat', 'part_init_p05_corrected.csv',
    #                       f'{DATA_PATH}part_init_p05.csv')
    # swift.download_object('patstat', 'part_init_p05_corrected.csv',
    #                       f'{DATA_PATH}part_init_p05_corrected.csv')
    # swift.download_object('patstat', 'part_p05_corrected.csv', f'{DATA_PATH}part_p05.csv')
    # swift.download_object('patstat', 'part_p08_corrected.csv', f'{DATA_PATH}part_p08.csv')
    # swift.download_object('patstat', 'model_xgb.json', f'{DATA_PATH}model_xgb.json')
    # swift.download_object('patstat', 'spring2025_fam_final_json.jsonl',
    #                       f'{DATA_PATH}spring2025_fam_final_json.jsonl')

    # for nr in ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14"]:
    #     swift.download_object('patstat', f'tls231_inpadoc_legal_event_part{nr}.zip',
    #                           f'{DATA_PATH}tls231_inpadoc_legal_event_part{nr}.zip')

    # lists all the CSV files and rename them (model tls\d{3}_part\d{2}

    list_csv = ["tls201_part01.csv", "tls201_part02.csv", "tls201_part03.csv", "tls202_part01.csv", "tls203_part01.csv",
           "tls203_part02.csv", "tls203_part03.csv", "tls203_part04.csv", "tls203_part05.csv", "tls203_part06.csv",
           "tls203_part07.csv", "tls203_part08.csv", "tls203_part09.csv", "tls203_part10.csv", "tls204_part01.csv",
           "tls206_part01.csv", "tls206_part02.csv", "tls207_part01.csv", "tls209_part01.csv", "tls209_part02.csv",
           "tls209_part03.csv", "tls211_part01.csv", "tls211_part02.csv", "tls212_part01.csv", "tls212_part02.csv",
           "tls212_part03.csv", "tls212_part04.csv", "tls214_part01.csv", "tls214_part02.csv", "tls224_part01.csv",
           "tls224_part02.csv", "tls225_part01.csv", "tls225_part02.csv", "tls902_part01.csv", "tls231_part14.csv",
           "tls231_part13.csv", "tls231_part12.csv", "tls231_part11.csv", "tls231_part10.csv", "tls231_part09.csv",
           "tls231_part08.csv", "tls231_part07.csv", "tls231_part06.csv", "tls231_part05.csv", "tls231_part04.csv",
           "tls231_part03.csv", "tls231_part02.csv", "tls231_part01.csv", "tls229_part01.csv"]

    files = os.listdir(DATA_PATH)

    list_csv2 = list(set(list_csv) - set(files))
    list_csv2.sort()

    for csv in list_csv2:
        swift.download_object('patstat', csv, f'{DATA_PATH}{csv}')

    # removes the end of the folders to get table names from PATSTAT Global database
    table_names = list(map(lambda a: re.sub(r"_part\d+.csv", "", a), list_csv))

    # creates a dataframe to connect each file to its folder and table
    file_names = pd.DataFrame({"subfolders": list_csv, "table_names": table_names})
    file_names["file_names"] = file_names["subfolders"].apply(lambda a: a.split("_")[0] + "_" + a.split("_")[-1])
    print(file_names.head(), flush=True)
    # moves each file into a folder corresponding to its table in PATSTAT Global database
    for _, r in file_names.iterrows():
        os.makedirs(r["table_names"], exist_ok=True)
        shutil.move(r["file_names"], r["table_names"])
