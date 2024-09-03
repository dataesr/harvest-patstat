#!/usr/bin/env python
# coding: utf-8

from patstat import dtypes_patstat_declaration as types
import fasttext
import numpy as np
import os
import pandas as pd
import re
from sklearn.model_selection import train_test_split
import xgboost as xgb

from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
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

COLUMNS = ["person_name", "doc_std_name", "invt_seq_nr"]

# set working directory
os.chdir(DATA_PATH)


def create_dataset(t206: pd.DataFrame) -> pd.DataFrame:
    """

    :param t206:
    :return:
    """
    t206["label"] = t206["type"].copy()
    tls206_c = t206.copy()

    indiv_with_pm_type = tls206_c[["doc_std_name", "doc_std_name_id", "person_name", "label"]].drop_duplicates() \
        .groupby(["doc_std_name", "doc_std_name_id", "person_name"]).count().reset_index().rename(
        columns={"label": "count_label"})

    indiv_with_pm_type = indiv_with_pm_type[indiv_with_pm_type["count_label"] > 1]
    indiv_with_pm_type = pd.merge(tls206_c, indiv_with_pm_type, on=["doc_std_name", "doc_std_name_id", "person_name"],
                                  how="inner")

    part2 = tls206_c.loc[~tls206_c["key_appln_nr_person"].isin(indiv_with_pm_type["key_appln_nr_person"])]

    lrn = part2[["person_name", "doc_std_name", "invt_seq_nr", "label"]].drop_duplicates()

    lrn = lrn.loc[lrn["label"].notna()]

    return lrn


def trn_tst(lrn: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
    trn, tst = train_test_split(lrn, random_state=123, shuffle=True)

    return tst, trn


def learning_type_fsttxt(trn: pd.DataFrame, tst: pd.DataFrame) -> fasttext.FastText:
    train_file = open('train.txt', 'w')
    for i, row in trn.iterrows():
        my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name}\n"
        train_file.write(my_line)
    train_file.close()

    test_file = open('test.txt', 'w')
    for i, row in tst.iterrows():
        my_line = f"__label__{row.label} {row.doc_std_name} {row.person_name}\n"
        test_file.write(my_line)
    test_file.close()

    model = fasttext.train_supervised('train.txt',
                                      wordNgrams=2,
                                      minCount=20,
                                      loss='ova',
                                      epoch=50)

    model.save_model('model_test')
    swift.upload_object('patstat', 'model_test')
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
    swift.upload_object('patstat', 'model_test2')

    test_pred2 = model2.test('test.txt', k=-1, threshold=0.5)
    print(test_pred2)

    return model2


def testing_fasttext(tst: pd.DataFrame, col_txt: str, col_pred: str, col_type: str,
                     mod_fsttxt: fasttext.FastText) -> pd.DataFrame:
    tst[col_pred] = tst[col_txt].apply(lambda a: mod_fsttxt.predict(a))
    tst[col_type] = tst[col_pred].apply(
        lambda a: re.search("pp|pm", str(a)).group(0) if re.search("pp|pm", str(a)) else np.nan)

    return tst


def learning_xgb_invt(trn_xgb: pd.DataFrame, tst: pd.DataFrame, colmns: list) -> (
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
    # bst.save_model('0001.model')
    bst.save_model('model_xgb_invt.json')
    swift.upload_object('patstat', 'model_xgb_invt.json')

    ypred = bst.predict(dtest)

    tst = tst.reset_index(drop=True).copy()

    tst.insert(2, "pred_xgb_invt", pd.Series(ypred))

    tst["type_xgb_invt"] = tst["pred_xgb_invt"].apply(lambda a: "pp" if a >= 0.5 else "pm")

    return bst, trn_xgb, tst_xgb, tst


def learning_xgb(trn_xgb: pd.DataFrame, tst: pd.DataFrame) -> (
        xgb.Booster, xgb.DMatrix, xgb.DMatrix, pd.DataFrame):
    trn_xgb = trn_xgb.copy()
    trn_xgb = trn_xgb.drop(columns="invt_seq_nr")

    trn_xgb["label2"] = trn_xgb["label"].apply(
        lambda a: 0 if a == "pm" else 1)

    tst_xgb = tst.copy()
    tst_xgb = tst_xgb[['person_name', 'doc_std_name', 'label']]

    tst_xgb["label2"] = tst_xgb["label"].apply(
        lambda a: 0 if a == "pm" else 1)

    trn_xgb = trn_xgb.drop(columns="label").rename(columns={"label2": "label"}).copy()

    tst_xgb = tst_xgb.drop(columns="label").rename(columns={"label2": "label"}).copy()

    for col in ["doc_std_name", "person_name"]:
        trn_xgb[col] = trn_xgb[col].astype("category")
        tst_xgb[col] = tst_xgb[col].astype("category")

    dtrain = xgb.DMatrix(trn_xgb.iloc[:, :-1], trn_xgb["label"], enable_categorical=True)
    dtest = xgb.DMatrix(tst_xgb.iloc[:, :-1], tst_xgb["label"], enable_categorical=True)

    param = {'max_depth': 2, 'objective': 'binary:logistic', 'seed': 123, 'nthread': 2, 'eval_metric': 'auc'}
    evallist = [(dtest, 'eval'), (dtrain, 'train')]
    num_round = 10
    bst = xgb.train(param, dtrain, num_round, evallist)
    # bst.save_model('0001.model')
    bst.save_model('model_xgb.json')
    swift.upload_object('patstat', 'model_xgb.json')


    ypred = bst.predict(dtest)

    tst = tst.reset_index(drop=True).copy()

    tst.insert(1, "pred_xgb", pd.Series(ypred))

    tst["type_xgb"] = tst["pred_xgb"].apply(lambda a: "pp" if a >= 0.5 else "pm")

    return bst, trn_xgb, tst_xgb, tst


def comparison(tst: pd.DataFrame) -> pd.DataFrame:
    comp = tst[["person_name", "doc_std_name", "label", "type_xgb", "type", "type_int", "type_xgb_invt"]].copy()
    comp["test_fsttxt1"] = comp.apply(lambda a: 1 if a["label"] != a["type"] else 0, axis=1)
    comp["test_fsttxt_invt"] = comp.apply(lambda a: 1 if a["label"] != a["type_int"] else 0, axis=1)
    comp["test_xgb_invt"] = comp.apply(lambda a: 1 if a["label"] != a["type_xgb_invt"] else 0, axis=1)
    comp["test_xgb"] = comp.apply(lambda a: 1 if a["label"] != a["type_xgb"] else 0, axis=1)

    res = pd.DataFrame(data={"fsttxt1": [comp["test_fsttxt1"].sum()],
                             "fsttxt_invt": [comp["test_fsttxt_invt"].sum()],
                             "xgb_invt": [comp["test_xgb_invt"].sum()],
                             "xgb": [comp["test_xgb"].sum()]})

    # cols = ["type_xgb", "type", "type_int", "type_xgb_invt"]
    # comp["label_binaire"] = comp["label"].apply(lambda a: 0 if a == "pm" else 1)
    # for col in cols:
    #     nom = col + "_binaire"
    #     comp[nom] = comp[col].apply(lambda a: 0 if a == "pm" else 1)
    #     nom_vp = nom + "_vp"
    #     nom_fp = nom + "_fp"
    #     nom_vn = nom + "_vn"
    #     nom_fn = nom + "_fn"
    #
    #     comp[nom_vp] = comp.apply(lambda a: 1 if a["label_binaire"] == 1 and a[nom] == 1 else 0, axis=1)
    #     comp[nom_fp] = comp.apply(lambda a: 1 if a["label_binaire"] == 0 and a[nom] == 1 else 0, axis=1)
    #     comp[nom_vn] = comp.apply(lambda a: 1 if a["label_binaire"] == 0 and a[nom] == 0 else 0, axis=1)
    #     comp[nom_fn] = comp.apply(lambda a: 1 if a["label_binaire"] == 1 and a[nom] == 0 else 0, axis=1)
    #
    # res2 = pd.DataFrame(data={
    #     "prec_fsttxt1": [comp["type_binaire_vp"].sum() / (comp["type_binaire_vp"].sum() + comp["type_binaire_fp"].sum())],
    #     "prec_fsttxt_invt": [comp["type_int_binaire_vp"].sum() / (
    #                 comp["type_int_binaire_vp"].sum() + comp["type_int_binaire_fp"].sum())],
    #     "prec_xgb_invt": [comp["type_xgb_invt_binaire_vp"].sum() / (
    #                 comp["type_xgb_invt_binaire_vp"].sum() + comp["type_xgb_invt_binaire_fp"].sum())],
    #     "prec_xgb": [comp["type_xgb_binaire_vp"].sum() / (
    #                 comp["type_xgb_binaire_vp"].sum() + comp["type_xgb_binaire_fp"].sum())]})

    return res


def main():
    old_part = pd.read_csv("part_init_p05_corrected.csv",
                           sep='|',
                           dtype=types.part_init_types,
                           encoding="utf-8")

    new_participants = old_part.copy()

    new_participants["person_name"] = new_participants["name_source"].copy()

    learning = create_dataset(new_participants)
    test, train_ros = trn_tst(learning)
    mod = learning_type_fsttxt(train_ros, test)
    mod2 = learning_type_invt_fsttxt(train_ros, test)
    boosting_invt, train_xgb_invt, test_xgb_invt, test = learning_xgb_invt(train_ros, test, COLUMNS)
    boosting, train_xgb, test_xgb, test = learning_xgb(train_ros, test)
    test = test.copy()
    test["txt"] = test["doc_std_name"] + " " + test["person_name"]
    test["txt_invt"] = test["doc_std_name"] + " " + test["person_name"] + " " + test["invt_seq_nr"].astype(str)
    test = test.copy()
    test = testing_fasttext(test, "txt", "prediction", "type", mod)
    test = testing_fasttext(test, "txt_invt", "pred_invt", "type_int", mod2)
    test = test.copy()
    results = comparison(test)
    print("Best model:", results.idxmin(axis=1)[0], flush=True)
