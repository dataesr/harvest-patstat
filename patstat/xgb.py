#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import numpy as np
import os
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb

# directory where the files are
DATA_PATH = "/run/media/julia/DATA/test/"
DICT = {"tls207": {'sep': ',', 'chunksize': 10000000, 'dtype': types.tls207_types},
        "tls206": {'sep': ',', 'chunksize': 3000000, 'dtype': types.tls206_types}
        }

PM = ["COMPANY",
      "GOV NON-PROFIT",
      "UNIVERSITY",
      "HOSPITAL",
      "GOV NON-PROFIT UNIVERSITY",
      "COMPANY GOV NON-PROFIT",
      "COMPANY UNIVERSITY",
      "COMPANY HOSPITAL"]

# set working directory
os.chdir(DATA_PATH)

patents = pd.read_csv("patent.csv", sep="|", dtype=types.patent_types)
old_part = pd.read_csv("part_init.csv",
                       sep='|',
                       dtype=types.part_init_types,
                       encoding="utf-8")

appln_id = pd.DataFrame(patents["appln_id"].append(old_part["appln_id"])).drop_duplicates()

tls207 = cfq.filtering("tls207", appln_id, "appln_id", DICT["tls207"])
tls206 = cfq.filtering("tls206", tls207, "person_id", DICT["tls206"])
new_participants = pd.merge(tls207, tls206, how="inner", on="person_id")

tls206_c = new_participants.dropna(subset=["person_name"])

tls206_c = tls206_c.copy()

tls206_c["label"] = tls206_c["psn_sector"].apply(
    lambda a: 0 if a in set(PM) else 1 if a == "INDIVIDUAL" else np.nan)

lrn = tls206_c[tls206_c["label"].notna()].drop(
    columns=["applt_seq_nr", "person_id", "appln_id", "doc_std_name_id", "nuts", "nuts_level", "doc_std_name_id",
             "psn_id", "psn_level", "psn_sector","han_id",
             "han_harmonized"]).drop_duplicates()

colonnes = ['person_name', 'person_name_orig_lg', 'person_address',
     'person_ctry_code', 'doc_std_name', 'psn_name',
     'han_name']

for col in colonnes:
    lrn[col] = lrn[col].astype("category")

train, test = train_test_split(lrn, random_state=123, shuffle=True)


dtrain = xgb.DMatrix(train.iloc[:,:-1], train["label"], enable_categorical=True)
dtest = xgb.DMatrix(test.iloc[:,:-1], test["label"], enable_categorical=True)

param = {'max_depth': 2, 'eta': 1, 'objective': 'binary:logistic'}
param['nthread'] = 2
param['eval_metric'] = 'auc'
evallist = [(dtest, 'eval'), (dtrain, 'train')]
num_round = 10
bst = xgb.train(param, dtrain, num_round, evallist)
bst.save_model('0001.model')

ypred = bst.predict(dtest)

test = test.reset_index().copy()

test.insert(9, "pred", pd.Series(ypred))

test["type"] = test["pred"].apply(lambda a: "pp" if a >= 0.5 else "pm")
test["perso"] = test["label"].apply(lambda a: "pp" if a == 1.0 else "pm")
test["invt"] = test["invt_seq_nr"].apply(lambda a: "pm" if a == 0 else "pp")
test = test.drop(columns="index")
test.to_csv("prediction_xgb.csv", sep=";", encoding="utf-8", index=False)



