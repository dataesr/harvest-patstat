#!/usr/bin/env python
# coding: utf-8

import csv_files_querying as cfq
import dtypes_patstat_declaration as types
import fasttext
import numpy as np
import os
import pandas as pd
import re
from sklearn.model_selection import train_test_split

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


def create_dataset(t206):
    """

    :param t206:
    :return:
    """
    tls206_c = t206.dropna(subset=["person_name"])

    doc_std_name = tls206_c[
        ["person_id", "person_name", "doc_std_name", "doc_std_name_id", "psn_sector", "invt_seq_nr"]].drop_duplicates()

    doc_std_name = doc_std_name.sort_values("doc_std_name_id")

    doc_std_name["label"] = doc_std_name["psn_sector"].apply(
        lambda a: 'pm' if a in set(PM) else 'pp' if a == "INDIVIDUAL" else np.nan)

    doc_std_name = doc_std_name.drop(columns="psn_sector")
    doc_std_name = doc_std_name.drop_duplicates()

    lrn = doc_std_name[doc_std_name["label"].notna()].drop(
        columns=["person_id", "doc_std_name_id"]).drop_duplicates()

    return lrn


def learning_type(trn, tst):
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

    return model


def learning_type_invt(trn, tst):
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

    return model2


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
    train, test = train_test_split(learning, random_state=123, shuffle=True)
    mod = learning_type(train, test)
    mod2 = learning_type_invt(train, test)
    mod.save_model('model_test')
    mod2.save_model("model_test2")
    test_pred = mod.test('test.txt', k=-1, threshold=0.5)
    test_pred2 = mod2.test('test2.txt', k=-1, threshold=0.5)
    test = test.copy()
    test["txt"] = test["doc_std_name"] + " " + test["person_name"]
    test["prediction"] = test["txt"].apply(lambda a: mod.predict(a))
    test["type"] = test["prediction"].apply(lambda a: re.search("pp|pm", str(a)).group(0))
    test = test.copy()
    test["txt_invt"] = test["doc_std_name"] + " " + test["person_name"] + " " + test["invt_seq_nr"].astype(str)
    test["pred_invt"] = test["txt_invt"].apply(lambda a: mod2.predict(a))
    test["type_int"] = test["pred_invt"].apply(lambda a: re.search("pp|pm", str(a)).group(0))
    test["invt"] = test["invt_seq_nr"].apply(lambda a: "pm" if a == 0 else "pp")
    test.to_csv("predictions_fasttext.csv", sep=";", encoding="utf-8", index=False)
    print(test_pred, test_pred2)


if __name__ == "__main__":
    main()
