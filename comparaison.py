#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import fasttext
from imblearn.over_sampling import RandomOverSampler
import numpy as np
import os
import pandas as pd
import re
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
    lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

part = tls206_c.copy()

indiv_with_pm_type = part[["doc_std_name", "doc_std_name_id", "person_name", "label"]].drop_duplicates() \
    .groupby(["doc_std_name", "doc_std_name_id", "person_name"]).count().reset_index().rename(
    columns={"label": "count_label"})

indiv_with_pm_type = indiv_with_pm_type[indiv_with_pm_type["count_label"] > 1]

double = pd.merge(indiv_with_pm_type, part, on=["doc_std_name", "doc_std_name_id", "person_name"], how="left")
sum_invt = pd.DataFrame(double.groupby(["doc_std_name", "person_name"])["invt_seq_nr"].sum()).rename(
    columns={"invt_seq_nr": "sum_invt"})
double = pd.merge(double, sum_invt, on=["doc_std_name", "person_name"], how="left")
double["type2"] = double["sum_invt"].apply(lambda a: "pp" if a > 0 else "pm")
double = double.copy()
double = double[["doc_std_name", "person_name", "type2"]].drop_duplicates()

part2 = pd.merge(part, double, on=["doc_std_name", "person_name"], how="left")
part2 = part2.copy()
part2["label2"] = np.where(part2["type2"].isna(), part2["label"], part2["type2"])
part2 = part2.drop(columns=["label", "type2"]).rename(columns={"label2": "label"})

lrn = part2[part2["label"].notna()].drop(
    columns=["applt_seq_nr", "person_id", "appln_id", "doc_std_name_id", "nuts", "nuts_level", "doc_std_name_id",
             "psn_id", "psn_level", "psn_sector", "han_id",
             "han_harmonized"]).drop_duplicates()

train, test = train_test_split(lrn, random_state=123, shuffle=True)
ros = RandomOverSampler(random_state=42)
train_sample = train.copy()
train_ros_X, train_ros_Y = ros.fit_resample(train_sample.iloc[:, :-1], train_sample["label"])
train_ros = train_ros_X.join(train_ros_Y)

# FASTEXT
# sans invt
train_file = open('train.txt', 'w')
for i, row in train_ros.iterrows():
    my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} \n"
    train_file.write(my_line)
train_file.close()

test_file = open('test.txt', 'w')
for i, row in test.iterrows():
    my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} \n"
    test_file.write(my_line)
test_file.close()

model = fasttext.train_supervised('train.txt',
                                  wordNgrams=2,
                                  minCount=20,
                                  loss='ova',
                                  epoch=50)

# avec invt
train_file2 = open('train2.txt', 'w')
for i, row in train_ros.iterrows():
    my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} str({row.invt_seq_nr}) \n"
    train_file2.write(my_line)
train_file2.close()

test_file2 = open('test2.txt', 'w')
for i, row in test.iterrows():
    my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} str({row.invt_seq_nr}) \n"
    test_file2.write(my_line)
test_file2.close()

model2 = fasttext.train_supervised('train2.txt',
                                   wordNgrams=2,
                                   minCount=20,
                                   loss='ova',
                                   epoch=50)

model.save_model('model_test')
model2.save_model("model_test2")
test_pred = model.test('test.txt', k=-1, threshold=0.5)
test_pred2 = model2.test('test2.txt', k=-1, threshold=0.5)
test_fsttxt = test.copy()
test_fsttxt["txt"] = test_fsttxt["doc_std_name"] + " " + test_fsttxt["person_name"]
test_fsttxt["prediction"] = test_fsttxt["txt"].apply(lambda a: model.predict(a))
test_fsttxt["type"] = test_fsttxt["prediction"].apply(lambda a: re.search("pp|pm", str(a)).group(0))
test_fsttxt = test_fsttxt.copy()
test_fsttxt["txt_invt"] = test_fsttxt["doc_std_name"] + " " + test_fsttxt["person_name"] + " " + test_fsttxt[
    "invt_seq_nr"].astype(str)
test_fsttxt["pred_invt"] = test_fsttxt["txt_invt"].apply(lambda a: model2.predict(a))
test_fsttxt["type_int"] = test_fsttxt["pred_invt"].apply(lambda a: re.search("pp|pm", str(a)).group(0))
test_fsttxt["invt"] = test_fsttxt["invt_seq_nr"].apply(lambda a: "pm" if a == 0 else "pp")
test_fsttxt.to_csv("predictions_fasttext.csv", sep=";", encoding="utf-8", index=False)

# XGB
test_xgb = test.copy()
train_xgb = train_ros.copy()

test_xgb["label2"] = test_xgb["label"].apply(
    lambda a: 0 if a == "pm" else 1)

test_xgb = test_xgb.drop(columns=["label"]).rename(columns={"label2": "label"}).copy()

train_xgb["label2"] = train_xgb["label"].apply(
    lambda a: 0 if a == "pm" else 1)

train_xgb = train_xgb.drop(columns="label").rename(columns={"label2": "label"}).copy()

colonnes = ['person_name', 'person_name_orig_lg', 'person_address',
            'person_ctry_code', 'doc_std_name', 'psn_name',
            'han_name']

for col in colonnes:
    train_xgb[col] = train_xgb[col].astype("category")
    test_xgb[col] = test_xgb[col].astype("category")

dtrain = xgb.DMatrix(train_xgb.iloc[:, :-1], train_xgb["label"], enable_categorical=True)
dtest = xgb.DMatrix(test_xgb.iloc[:, :-1], test_xgb["label"], enable_categorical=True)

param = {'max_depth': 2, 'objective': 'binary:logistic', 'seed': 123, 'nthread': 2, 'eval_metric': 'auc'}
evallist = [(dtest, 'eval'), (dtrain, 'train')]
num_round = 10
bst = xgb.train(param, dtrain, num_round, evallist)
bst.save_model('0001.model')

ypred = bst.predict(dtest)

test_xgb = test_xgb.reset_index().copy()

test_xgb.insert(9, "pred", pd.Series(ypred))

test_xgb["type"] = test_xgb["pred"].apply(lambda a: "pp" if a >= 0.5 else "pm")
test_xgb["perso"] = test_xgb["label"].apply(lambda a: "pp" if a == 1.0 else "pm")
test_xgb["invt"] = test_xgb["invt_seq_nr"].apply(lambda a: "pm" if a == 0 else "pp")
test_xgb = test_xgb.drop(columns="index")
test_xgb.to_csv("prediction_xgb.csv", sep=";", encoding="utf-8", index=False)

fsttxt = pd.read_csv("predictions_fasttext.csv", sep=";", encoding="utf-8")
fsttxt = fsttxt.drop(columns=["txt", "prediction", "txt_invt", "pred_invt"]).rename(
    columns={"type": "type_fsttxt1", "type_int": "type_fsttxt_invt"}).copy()
gb = pd.read_csv("prediction_xgb.csv", sep=";", encoding="utf-8")
gb = gb.drop(columns=["label", "pred"]).rename(columns={"perso": "label", "type": "type_xgb"}).copy()

comp = pd.merge(fsttxt, gb, on=["invt_seq_nr", "person_name", "doc_std_name", "label", "invt"], how="inner")
comp["test_invt"] = np.where(comp["label"] != comp["invt"], 1, 0)
comp["test_fsttxt1"] = np.where(comp["label"] != comp["type_fsttxt1"], 1, 0)
comp["test_fsttxt_invt"] = np.where(comp["label"] != comp["type_fsttxt_invt"], 1, 0)
comp["test_xgb"] = np.where(comp["label"] != comp["type_xgb"], 1, 0)

res = pd.DataFrame(data={"invt": [comp["test_invt"].sum()],
                         "fsttxt1": [comp["test_fsttxt1"].sum()],
                         "fsttxt_invt": [comp["test_fsttxt_invt"].sum()],
                         "xgb": [comp["test_xgb"].sum()]})
