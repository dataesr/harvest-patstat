import pandas as pd

import numpy as np
from patstat import dtypes_patstat_declaration as types
import os
import re
from collections import Counter
from utils import swift

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def familles_ri_file():
    os.chdir(DATA_PATH)
    part = pd.read_csv("part_p08.csv",
                       sep="|", encoding="utf-8", engine="python", dtype=types.partfin_types)
    part.to_csv("part_p08_backup.csv", sep="|", index=False)

    swift.download_object('patstat', 'corrections_familles_brevets.xlsx',
                          'corrections_familles_brevets.xlsx')
    corrections = pd.read_excel("corrections_familles_brevets.xlsx", engine="openpyxl",
                                sheet_name="corrections_familles_brevets_py",
                                dtype={"isin": str, "cols": str, "vals": str})
    corrections = corrections.drop_duplicates().reset_index(drop=True)
    corrections["lisin"] = corrections["isin"].str.replace("₀", "_0", regex=False)
    corrections["lisin"] = corrections["isin"].str.replace("\n", "", regex=True)
    corrections["lisin"] = corrections["isin"].str.split("|")
    corrections["lcols"] = corrections["cols"].str.split("|")
    corrections.loc[corrections["vals"].notna(), "lvals"] = corrections.loc[
        corrections["vals"].notna(), "vals"].str.split(
        "|")
    corrections.loc[corrections["lvals"].notna(), "lvals"] = corrections.loc[
        corrections["lvals"].notna(), "lvals"].apply(
        lambda a: [v if v != "" else np.nan for v in a])

    ldico = {"isin": [], "cols": [], "vals": []}
    for r in corrections.itertuples():
        lisin = list(set(r.lisin))
        dans = "|".join(lisin)
        ldico["isin"].append(dans)
        ldico["cols"].append(r.cols)
        ldico["vals"].append(r.vals)

    corr = pd.DataFrame(data=ldico)

    corr2 = corr.copy()
    corr2["lisin"] = corr2["isin"].str.split("|")
    corr2["lisin"] = corr2["lisin"].apply(lambda a: [x for x in a if x != ""])
    corr2["lisin"] = corr2["lisin"].apply(lambda a: [x for x in a if re.sub(r"^\s+", "", x)])
    corr2["lisin"] = corr2["lisin"].apply(
        lambda a: [re.search(r"\w{2}.+_.+_.+_.+", x).string for x in a if
                   not re.search(r"\w{2}.+_.+_.+_.+", x) is None])
    corr2["lcols"] = corr2["cols"].str.split("|")
    corr2.loc[corrections["vals"].notna(), "lvals"] = corr2.loc[corr2["vals"].notna(), "vals"].str.split(
        "|")
    corr2.loc[corr2["lvals"].notna(), "lvals"] = corrections.loc[corrections["lvals"].notna(), "lvals"].apply(
        lambda a: [v if v != "" else np.nan for v in a])

    maxi_liste = corr2.iloc[0]["lisin"]
    for idx in range(1, len(corr2)):
        maxi_liste = maxi_liste + corr2.iloc[idx]["lisin"]

    decompte = Counter(maxi_liste)
    decompte2 = {k: v for (k, v) in decompte.items() if v > 1}
    keys = list(decompte2.keys())
    doublons = {"key_appln_nr_person": [], "Index": [], "vals": []}
    for key in keys:
        for r in corr2.itertuples():
            if key in r.lisin:
                doublons["key_appln_nr_person"].append(key)
                doublons["Index"].append(r.Index)
                doublons["vals"].append(r.vals)

    ddoublons = pd.DataFrame(data=doublons)
    compte = ddoublons[["key_appln_nr_person", "vals"]].groupby("key_appln_nr_person").nunique(
        dropna=False).reset_index().rename(columns={"vals": "compte"})
    ddoublons = pd.merge(ddoublons, compte, on="key_appln_nr_person", how="left")
    pres1 = ddoublons.loc[ddoublons["compte"] == 1]
    max_pres1 = pres1[["key_appln_nr_person", "Index"]].groupby("key_appln_nr_person").max().reset_index().rename(
        columns={
            "Index": "max"})

    for r in max_pres1.itertuples():
        idx = r.max
        vals = r.key_appln_nr_person
        listr = corr2.iloc[idx]["lisin"]
        listr.remove(vals)
        corr2.iloc[idx]["lisin"] = listr

    ldico2 = {"isin": [], "cols": [], "vals": []}
    for r in corr2.itertuples():
        lisin = list(set(r.lisin))
        lcols = r.lcols
        lvals = r.lvals
        dans = "|".join(lisin)
        ldico2["isin"].append(dans)
        ldico2["cols"].append(r.cols)
        ldico2["vals"].append(r.vals)
        if len(lcols) > 1:
            part.loc[part["key_appln_nr_person"].isin(lisin), lcols] = lvals
        else:
            part.loc[part["key_appln_nr_person"].isin(lisin), r.cols] = r.vals

    corr3 = pd.DataFrame(data=ldico2)
    corr3.to_csv("corrections_familles_brevets_python2.csv", sep=";", encoding="utf-8", index=False)

    part.loc[part["type"] == "pp", ["category_libelle", "esri"]] = ["Personne physique", "AUTRE"]
    part.loc[(part["type"] == "pm") & (part["category_libelle"] == "Personne physique"), "category_libelle"] = np.nan
    part.loc[(part["type"] == "pp") & (part["siren"].notna()), "siren"] = np.nan
    part.loc[part["type"] == "pm", ["category_libelle", "esri", "sexe"]] = ["Personne morale", "AUTRE", np.nan]
    part.to_csv("part_p08.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'part_p08.csv')
