#!/usr/bin/env python
# coding: utf-8

import dask.dataframe as dd
import os

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/tls201/"

# set working directory
os.chdir(DATA_PATH)

# read the CSVs with Dask
df = dd.read_csv("*.csv",
                 usecols=["appln_id", "appln_kind", "earliest_filing_year", "ipr_type", 'earliest_publn_year'],
                 dtype={'appln_id': str,
                        'appln_kind': 'category',
                        'earliest_filing_date': str,
                        'ipr_type': 'category',
                        'earliest_publn_year': 'uint16'})

