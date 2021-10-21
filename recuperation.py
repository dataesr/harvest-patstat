#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import os
import pandas as pd


# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"
DICT = {"tls201": {'sep': ',', 'chunksize': 2000000, 'dtype': types.tls201_types}
        }

# set working directory
os.chdir(DATA_PATH)
# On commence par récupérer les fichiers patent (infos sur les dépôts de brevets) et old_part
# (participants dédoublonnés et identifiés de la version antérieure)

old_part = pd.read_csv("/run/media/julia/DATA/DONNEES/PATENTS/SOURCES/DATAESR/partfin.csv",
                       sep='|',
                       dtype=types.partfin_types)

old_part.rename(columns={"id_patent": "appln_nr_epodoc"}, inplace=True)

tls201 = cfq.filtering("tls201", old_part, "appln_nr_epodoc", DICT["tls201"])
