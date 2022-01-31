#!/usr/bin/env python
# coding: utf-8

from patstat import csv_files_querying as cfq
from patstat import dtypes_patstat_declaration as types
import fasttext
from imblearn.over_sampling import RandomOverSampler
import numpy as np
import os
import pandas as pd
import re
from sklearn.model_selection import train_test_split
import xgboost as xgb

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME')
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

COLUMNS = ['person_name', 'person_name_orig_lg', 'person_address',
           'person_ctry_code', 'doc_std_name', 'psn_name',
           'han_name']

# set working directory
os.chdir(DATA_PATH)


def create_dataset(t206: pd.DataFrame) -> pd.DataFrame:
    """

    :param t206:
    :return:
    """
    tls206_c = t206.dropna(subset=["person_name"])
    tls206_c = tls206_c.copy()

    tls206_c["label"] = tls206_c["psn_sector"].apply(
        lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

    indiv_with_pm_type = tls206_c[["doc_std_name", "doc_std_name_id", "person_name", "label"]].drop_duplicates() \
        .groupby(["doc_std_name", "doc_std_name_id", "person_name"]).count().reset_index().rename(
        columns={"label": "count_label"})

    indiv_with_pm_type = indiv_with_pm_type[indiv_with_pm_type["count_label"] > 1]

    double = pd.merge(indiv_with_pm_type, tls206_c, on=["doc_std_name", "doc_std_name_id", "person_name"], how="left")
    sum_invt = pd.DataFrame(double.groupby(["doc_std_name", "person_name"])["invt_seq_nr"].sum()).rename(
        columns={"invt_seq_nr": "sum_invt"})
    double = pd.merge(double, sum_invt, on=["doc_std_name", "person_name"], how="left")
    double["type2"] = double["sum_invt"].apply(lambda a: "pp" if a > 0 else "pm")
    double = double.copy()
    double = double[["doc_std_name", "person_name", "type2"]].drop_duplicates()

    part2 = pd.merge(tls206_c, double, on=["doc_std_name", "person_name"], how="left")
    part2 = part2.copy()
    part2["label2"] = np.where(part2["type2"].isna(), part2["label"], part2["type2"])
    part2 = part2.drop(columns=["label", "type2"]).rename(columns={"label2": "label"})

    lrn = part2[part2["label"].notna()].drop(
        columns=["applt_seq_nr", "person_id", "appln_id", "doc_std_name_id", "nuts", "nuts_level", "doc_std_name_id",
                 "psn_id", "psn_level", "psn_sector", "han_id",
                 "han_harmonized"]).drop_duplicates()

    return lrn


def trn_tst(lrn: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    trn, tst = train_test_split(lrn, random_state=123, shuffle=True)
    ros = RandomOverSampler(random_state=42)
    trn_sample = trn.copy()
    trn_ros_x, trn_ros_y = ros.fit_resample(trn_sample.iloc[:, :-1], trn_sample["label"])
    trn_ros = trn_ros_x.join(trn_ros_y)

    return tst, trn_ros


def learning_type_fsttxt(trn: pd.DataFrame, tst: pd.DataFrame) -> fasttext.FastText:
    train_file = open('train.txt', 'w')
    for i, row in trn.iterrows():
        my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} \n"
        train_file.write(my_line)
    train_file.close()

    test_file = open('test.txt', 'w')
    for i, row in tst.iterrows():
        my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} \n"
        test_file.write(my_line)
    test_file.close()

    model = fasttext.train_supervised('train.txt',
                                      wordNgrams=2,
                                      minCount=20,
                                      loss='ova',
                                      epoch=50)

    model.save_model('model_test')
    test_pred = model.test('test.txt', k=-1, threshold=0.5)
    print(test_pred)

    return model


def learning_type_invt_fsttxt(trn: pd.DataFrame, tst: pd.DataFrame) -> fasttext.FastText:
    train_file2 = open('train2.txt', 'w')
    for i, row in trn.iterrows():
        my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} str({row.invt_seq_nr}) \n"
        train_file2.write(my_line)
    train_file2.close()

    test_file2 = open('test2.txt', 'w')
    for i, row in tst.iterrows():
        my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name} str({row.invt_seq_nr}) \n"
        test_file2.write(my_line)
    test_file2.close()

    model2 = fasttext.train_supervised('train2.txt',
                                       wordNgrams=2,
                                       minCount=20,
                                       loss='ova',
                                       epoch=50)

    model2.save_model('model_test2')

    test_pred2 = model2.test('test.txt', k=-1, threshold=0.5)
    print(test_pred2)

    return model2


def testing_fasttext(tst: pd.DataFrame, col_txt: str, col_pred: str, col_type: str, mod_fsttxt: fasttext.FastText) -> pd.DataFrame:
    tst[col_pred] = tst[col_txt].apply(lambda a: mod_fsttxt.predict(a))
    tst[col_type] = tst[col_pred].apply(lambda a: re.search("pp|pm", str(a)).group(0))

    return tst


def learning_xgb(trn_xgb: pd.DataFrame, tst: pd.DataFrame, colmns: list) -> (
        xgb.Booster, xgb.DMatrix, xgb.DMatrix, pd.DataFrame):
    trn_xgb = trn_xgb.copy()

    trn_xgb["label2"] = trn_xgb["label"].apply(
        lambda a: 0 if a == "pm" else 1)

    tst_xgb = tst.copy()

    tst_xgb["label2"] = tst_xgb["label"].apply(
        lambda a: 0 if a == "pm" else 1)

    trn_xgb = trn_xgb.drop(columns="label").rename(columns={"label2": "label"}).copy()

    tst_xgb = tst_xgb.drop(columns="label").rename(columns={"label2": "label"}).copy()

    for col in colmns:
        trn_xgb[col] = trn_xgb[col].astype("category")
        tst_xgb[col] = tst_xgb[col].astype("category")

    dtrain = xgb.DMatrix(trn_xgb.iloc[:, :-1], trn_xgb["label"], enable_categorical=True)
    dtest = xgb.DMatrix(tst_xgb.iloc[:, :-1], tst_xgb["label"], enable_categorical=True)

    param = {'max_depth': 2, 'objective': 'binary:logistic', 'seed': 123, 'nthread': 2, 'eval_metric': 'auc'}
    evallist = [(dtest, 'eval'), (dtrain, 'train')]
    num_round = 10
    bst = xgb.train(param, dtrain, num_round, evallist)
    bst.save_model('0001.model')

    ypred = bst.predict(dtest)

    tst = tst.reset_index().copy()

    tst.insert(9, "pred_xgb", pd.Series(ypred))

    tst["type_xgb"] = tst["pred_xgb"].apply(lambda a: "pp" if a >= 0.5 else "pm")
    tst = tst.drop(columns="index")

    return bst, trn_xgb, tst_xgb, tst

def comparison(tst: pd.DataFrame) -> pd.DataFrame:
    comp = tst[["invt_seq_nr", "person_name", "doc_std_name", "label", "type_xgb", "type", "type_int", "invt"]].copy()
    comp["test_invt"] = np.where(comp["label"] != comp["invt"], 1, 0)
    comp["test_fsttxt1"] = np.where(comp["label"] != comp["type"], 1, 0)
    comp["test_fsttxt_invt"] = np.where(comp["label"] != comp["type_int"], 1, 0)
    comp["test_xgb"] = np.where(comp["label"] != comp["type_xgb"], 1, 0)

    res = pd.DataFrame(data={"invt": [comp["test_invt"].sum()],
                             "fsttxt1": [comp["test_fsttxt1"].sum()],
                             "fsttxt_invt": [comp["test_fsttxt_invt"].sum()],
                             "xgb": [comp["test_xgb"].sum()]})

    return res


def main():
    patents = pd.read_csv("patent.csv", sep="|", dtype=types.patent_types)
    old_part = pd.read_csv("part_init.csv",
                           sep='|',
                           dtype=types.part_init_types,
                           encoding="utf-8")

    appln_id = pd.DataFrame(patents["appln_id"].append(old_part["appln_id"])).drop_duplicates()

    tls207 = cfq.filtering("tls207", appln_id, "appln_id", DICT["tls207"])
    tls206 = cfq.filtering("tls206", tls207, "person_id", DICT["tls206"])
    new_participants = pd.merge(tls207, tls206, how="inner", on="person_id")
    learning = create_dataset(new_participants)
    test, train_ros = trn_tst(learning)
    mod = learning_type_fsttxt(train_ros, test)
    mod2 = learning_type_invt_fsttxt(train_ros, test)
    boosting, train_xgb, test_xgb, test = learning_xgb(train_ros, test, COLUMNS)
    test = test.copy()
    test["txt"] = test["doc_std_name"] + " " + test["person_name"]
    test["txt_invt"] = test["doc_std_name"] + " " + test["person_name"] + " " + test["invt_seq_nr"].astype(str)
    test = test.copy()
    test = testing_fasttext(test, "txt", "prediction", "type", mod)
    test = testing_fasttext(test, "txt_invt", "pred_invt", "type_int", mod2)
    test = test.copy()
    test["invt"] = test["invt_seq_nr"].apply(lambda a: "pm" if a == 0 else "pp")
    results = comparison(test)
    print("Best model:", results.idxmin(axis=1)[0])


if __name__ == "__main__":
    main()
