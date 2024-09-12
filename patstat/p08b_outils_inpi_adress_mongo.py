#!/usr/bin/env python
# coding: utf-8

import os
import re

import numpy as np
import pandas as pd
from pymongo import MongoClient
from patstat import dtypes_patstat_declaration as types
from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
URI = os.getenv("MONGO_URI")


def cleaning(df_fr: pd.DataFrame, pat_fr: pd.DataFrame) -> pd.DataFrame:
    fr = pd.merge(df_fr, pat_fr[["key_appln_nr", "appln_publn_number"]], left_on="publication-number",
                  right_on="appln_publn_number", how="inner")

    fr2 = fr.loc[fr["country"].isin(["XX", "FR", ""])]

    fr2["f"] = fr2["postcode"].str.replace(r"^F-?\d+", "present", regex=True)
    fr2.loc[fr2["f"] != "present", "f"] = "absent"
    fr2.loc[fr2["f"] == "present", "postcode"] = fr2.loc[fr2["f"] == "present", "postcode"].str.replace(
        r"^[F|f]-?",
        "",
        regex=True)
    fr2.loc[fr2["f"] == "present", "postcode"] = fr2.loc[fr2["f"] == "present", "postcode"].str.replace(
        r"F-", "", regex=True)

    fr2.loc[fr2["f"] == "present", "postcode"] = fr2.loc[fr2["f"] == "present", "postcode"].apply(
        lambda a: re.sub(r"^F-?", "", a))

    fr2["f"] = fr2["postcode"].apply(lambda a: "present" if bool(re.match('[a-zA-Z\s]+$', a)) else "absent")
    fr2.loc[fr2["f"] != "present", "f"] = "absent"
    fr2["pc"] = fr2["city"].apply(lambda a: "present" if bool(re.match('\d{5}', a)) else "absent")
    fr2.loc[fr2["pc"] == "present", "pc2"] = fr2.loc[fr2["pc"] == "present", "city"].str.replace(r"[^\d{5}]",
                                                                                                 "",
                                                                                                 regex=True)
    fr2.loc[fr2["pc"] == "present", "city2"] = fr2.loc[fr2["pc"] == "present", "city"].str.replace(
        r"\s{0,}\d{5}\s{0,}", "",
        regex=True)
    fr2.loc[fr2["pc"] == "present", "city2"] = fr2.loc[fr2["pc"] == "present", "city2"].str.replace(
        r"^[,|\-]", "",
        regex=True)

    fr2.loc[fr2["pc"] == "present", "city2"] = fr2.loc[fr2["pc"] == "present", "city2"].str.replace(
        r"^\s+", "",
        regex=True)
    fr2.loc[fr2["pc"] == "present", "city2"] = fr2.loc[fr2["pc"] == "present", "city2"].str.replace(
        r"\s+$", "",
        regex=True)

    fr2.loc[fr2["pc"] == "present", "city2"] = fr2.loc[fr2["pc"] == "present", "city2"].str.replace(
        r"\s{2,}", "",
        regex=True)

    fr2.loc[fr2["pc"] == "present", "city2"] = fr2.loc[fr2["pc"] == "present", "city2"].str.replace(
        r"-\s{1,}", "-",
        regex=True)

    fr2.loc[(fr2["f"] == "present") & (fr2["pc"] == "present"), "city2"] = fr2.loc[
        (fr2["f"] == "present") & (fr2["pc"] == "present"), "postcode"]

    fr2.loc[(fr2["f"] == "present") & (fr2["pc"] == "present"), "city"] = fr2.loc[
        (fr2["f"] == "present") & (fr2["pc"] == "present"), "city2"]

    fr2.loc[(fr2["f"] == "present") & (fr2["pc"] == "present"), "postcode"] = fr2.loc[
        (fr2["f"] == "present") & (fr2["pc"] == "present"), "pc2"]

    fr2 = fr2.loc[(fr2["city"] != "X") & (fr2["postcode"] != "X")]

    fr2["postcode2"] = fr2["postcode"].apply(lambda a: a if bool(re.match('\d+', a)) else "")
    fr2.loc[(fr2["pc2"].notna()) & (fr2["postcode2"] == ""), "postcode2"] = fr2.loc[
        (fr2["pc2"].notna()) & (fr2["postcode2"] == ""), "pc2"]
    fr2["city2"] = fr2.apply(lambda a: a.postcode if bool(re.match('[^\d+]', a.postcode)) else a.city2, axis=1)
    fr2["city2"] = fr2["city2"].fillna("")

    fr2["cedex"] = fr2["city2"].str.contains("cedex", case=False)
    fr2["cedex"] = fr2["cedex"].apply(lambda a: "cedex" if a is True else "non")

    fr2 = fr2.drop(columns=["f", "pc", "pc2"])

    fr22 = fr2.loc[
        (fr2["address-1"] != "") | (fr2["city"] != "") | (fr2["postcode"] != "") | (fr2["postcode2"] != "")]

    fr22.loc[(fr22["city2"] == "") & (fr22["city"] != ""), "city2"] = fr22.loc[
        (fr22["city2"] == "") & (fr22["city"] != ""), "city"]

    fr22 = fr22.reset_index()

    dico = {"id": [], "publication-number": [], "sequence": [], "address": [], "nb": [], "key_appln_nr": []}

    for _, r in fr22.iterrows():
        nb = 0
        nom = ""
        if r["address-1"] != "":
            nb = nb + 1
            nom = nom + r["address-1"]
            if r["postcode2"] != "":
                nb = nb + 1
                nom = nom + " " + r["postcode2"]
                if r["city2"] != "":
                    nb = nb + 1
                    nom = nom + " " + r["city2"]
        elif r["postcode2"] != "":
            nb = nb + 1
            nom = nom + r["postcode2"]
            if r["city2"] != "":
                nb = nb + 1
                nom = nom + " " + r["city2"]
        else:
            nb = nb + 1
            nom = nom + " " + r["city2"]
        nom.replace("\\n", " ")
        nom = nom.strip()
        dico["id"].append(r["id"])
        dico["address"].append(nom)
        dico["nb"].append(nb)
        dico["publication-number"].append(r["publication-number"])
        dico["sequence"].append(r["sequence"])
        dico["key_appln_nr"].append(r["key_appln_nr"])

    df_dico2 = pd.DataFrame(dico)

    df_dico2 = df_dico2.loc[~df_dico2["address"].isin(
        ["A COMPLETER", "IPSWICH", "-", "---", "CB4 0WS", "FR", "L-1661", "MA02451",
         "C/O AMADEUS NORTH AMERICA 1050 WINTER STREET", "LAURELS ARDLEIGH ROAD", "14 RUE MASSENA",
         "1, RUE DES TILLEULS",
         "11 CHEMIN DES MULETS", "13 RUE DU GRAND SUD", "14 ALLEE DE LA CHENAIE",
         "15 AVENUE DES BRUYERES", "16 RUE MAURICE BOUCHOR", "17 RUE DE L\'ARBALETRE", "18 AVENUE RENE COTY",
         "20 RUE DE LA TERRASSE", "3 RUE MARGUERITTE CHAPON", "30, RUE MICHAL", "31 RUE DU VAL", "33 RUE DES ALOUETTES",
         "33 RUE LOUIS GUILLOUX", "36 AV DES MAROTS", "43 RUE JEAN PREVOST", "44 RUE DE LA LOUBIERE",
         "45 BD DU MARECHAL JOFFRE", "48 CHEMIN DU BEARD", "5 ALLEE DU GROS CHENE", "5 AVENUE VICTOR HUGO",
         "5 VILLA SISLEY", "558 CHE DE BON RENCONTRE", "56 RUE DE VOUILLE", "6 CHEMIN DE LA RIVOIRE",
         "6, RUE DES AMANDIERS", "6, RUE F CHARVET", "9 CHEMIN DE PELLEGRI", "BARRAT", "CLAMART", "FRESNE",
         "LA VERPILLERE", "LA VERRIE", "LE BOSSON", "ST FELIX", "0004 CHEMIN DE LA COLLINE", "- 00000", "- 00000 -",
         "00000", "00000 -", ""])]
    df_dico2.loc[df_dico2["address"] == "F-75014 PARIS", "address"] = "75014 PARIS"
    df_dico2.loc[df_dico2["address"] == "F-75015 PARIS", "address"] = "75015 PARIS"
    df_dico2["address"] = df_dico2["address"].str.replace(r"\"", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"«", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"»", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"\'\'", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"\ufeff3", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"^-+\s?", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"^\.+\s?", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"^\/+\s?", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"\n", " ", regex=True)
    df_dico2["address"] = df_dico2["address"].str.replace(r"\s{2,}", "", regex=True)
    df_dico2["address"] = df_dico2["address"].str.strip()

    df_dico = df_dico2.drop(columns="id").drop_duplicates()

    compte_address = df_dico[["publication-number", "sequence", "address"]].groupby(
        ["publication-number", "sequence"]).nunique(dropna=False).reset_index()
    compte_address = compte_address.rename(columns={"address": "compte"})
    adm = compte_address.loc[compte_address["compte"] > 1]
    adu = compte_address.loc[compte_address["compte"] == 1]
    df_dicou = pd.merge(df_dico, adu, on=["publication-number", "sequence"], how="inner").drop_duplicates()
    df_dicom = pd.merge(df_dico, adm, on=["publication-number", "sequence"], how="inner").drop_duplicates()

    pt_max = pd.pivot_table(df_dicom, index=["publication-number", "sequence"], values="nb",
                            aggfunc="max").reset_index()
    pt_max.loc[:, "max"] = "max"

    df_dicom2 = pd.merge(df_dicom, pt_max, on=["publication-number", "sequence", "nb"], how="inner").drop_duplicates()

    df_address = pd.concat([df_dicou, df_dicom2], ignore_index=True)
    df_address = df_address.drop(columns=["compte", "max", "nb"]).drop_duplicates()

    df_address_cpt = df_address.groupby(["key_appln_nr", "publication-number", "sequence"]).nunique(
        dropna=False).reset_index().rename(columns={"address": "compte"})
    df_address_final = pd.merge(df_address, df_address_cpt, on=["key_appln_nr", "publication-number", "sequence"],
                                how="left").sort_values(["key_appln_nr", "publication-number", "sequence"]).reset_index(
        drop=True)


    return df_address_final


def create_df_part(prt: pd.DataFrame, pat: pd.DataFrame, key: set, liste: list) -> list:
    prt = prt.loc[prt["key_appln_nr_person"].isin(key)]
    prt = prt.loc[prt["country_corrected"].isin(["FR", np.nan])]

    dp_fr = set(prt.loc[prt["docdb_family_id"].isin(liste), "docdb_family_id"])
    pt_fr = pat.loc[(pat["docdb_family_id"].isin(dp_fr)) & (pat["appln_auth"] == "FR")]

    liste_pt_fr = list(pt_fr["appln_publn_number"].unique())

    return liste_pt_fr



def create_df_address():
    # set working directory
    os.chdir(DATA_PATH)

    client = MongoClient(URI)

    db = client["inpi"]

    patents = pd.read_csv("patent.csv", sep="|", encoding="utf-8", engine="python", dtype=types.patent_types)

    auth = {}
    for r in patents.itertuples():
        off = r.appln_auth
        fam = r.docdb_family_id

        if fam not in auth:
            auth[fam] = []

        if off not in auth[fam]:
            auth[fam].append(off)

    liste_fr = []
    for key in auth.keys():
        if "FR" in auth[key]:
            liste_fr.append(key)
        else:
            pass

    patents = patents.loc[patents["docdb_family_id"].isin(liste_fr)]

    part = pd.read_csv("part_p08.csv", sep="|", encoding="utf-8", engine="python",
                       dtype=types.partfin_types, parse_dates=["earliest_filing_date"])

    part2 = part.copy()

    part2["year"] = part2["earliest_filing_date"].apply(lambda a: a.year)
    part2 = part2.loc[part2["year"] >= 2010]
    min_year = part2[["docdb_family_id", "year"]].drop_duplicates()
    min_year["year"] = min_year["year"].astype(str)
    ct_min_year = pd.pivot_table(min_year, values="year", index="docdb_family_id", fill_value=0, aggfunc="min")
    ct_min_year = ct_min_year.rename(columns={"year": "year_min"})
    part2 = pd.merge(part2, ct_min_year, on="docdb_family_id", how="inner").drop_duplicates()
    part2["year"] = part2["year"].astype(str)
    part2["comparaison"] = part2.apply(lambda a: "egal" if a.year == a.year_min else "diff", axis=1)
    part3 = part2.loc[part2["comparaison"] == "egal"].drop(columns=["comparaison", "year_min"])
    part3 = part3.rename(columns={"year": "year_min"})

    role = pd.read_csv("role.csv", sep="|", encoding="utf-8", engine="python")
    dep = set(role.loc[role["role"] == "dep", "key_appln_nr_person"])
    invr = set(role.loc[role["role"] == "inv", "key_appln_nr_person"])

    pn_dep = create_df_part(part3, patents, dep, liste_fr)

    liste_res = []

    for pn in db["person"].find({"publication-number": {"$in": pn_dep}}, {"_id": 0}):
        liste_res.append(pn)

    part_dep = pd.DataFrame(liste_res)
    part_dep["id"] = part_dep.index

    part_dep = part_dep.loc[part_dep["type-party"] == "applicant"]

    df_address_final_dep = cleaning(part_dep, patents)

    df_address_final_dep.to_csv("df_address_final_dep.csv", sep="|", index=False)
    df_address_final_dep.to_hdf("df_address_final_dep.h5", key="df_address_final_inv", mode="w")
    swift.upload_object('patstat', 'df_address_final_dep.csv')

    pn_inv = create_df_part(part3, patents, invr, liste_fr)

    liste_res = []

    for pn in db["person"].find({"publication-number": {"$in": pn_inv}}, {"_id": 0}):
        liste_res.append(pn)

    part_inv = pd.DataFrame(liste_res)
    part_inv["id"] = part_inv.index

    part_inv = part_inv.loc[part_inv["type-party"]=="inventor"]

    df_address_final_inv = cleaning(part_inv, patents)

    df_address_final_inv.to_csv("df_address_final_inv.csv", sep="|", index=False)
    df_address_final_inv.to_hdf("df_address_final_inv.h5", key="df_address_final_inv", mode="w")
    swift.upload_object('patstat', 'df_address_final_inv.csv')


