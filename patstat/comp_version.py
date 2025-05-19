# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de créer le fichier au format JSON qui va alimenter ScanR


import os
import random
import string

import numpy as np
import pandas as pd
from utils import swift

from patstat import dtypes_patstat_declaration as types

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

random.seed(42)


def jointure(fm, df):
    fm2 = pd.merge(fm, df, left_on="docdb_family_id", right_on="family_id", how="left").drop(columns="family_id")
    return fm2


def to_date_str(x):
    res = str(x)[0:10]
    if len(res) == 10:
        res = res + "T00:00:00"
        return res
    return None


def get_year(x):
    if isinstance(x, str):
        return int(x[0:4])
    return None


def list_id(lng: int, nb: int) -> list:
    """
    Don't keep E/e and zero as first character.
    Generate codes of a certain size defined by the user.

    """
    letters = string.hexdigits
    letters = letters.replace("e", "")
    letters = letters.replace("E", "")
    liste = []
    for n in range(nb):
        res = "".join(random.choice(letters) for i in range(lng))
        liste.append(res)

    liste = list(set(liste))

    return liste


def get_json():
    # set working directory
    os.chdir(DATA_PATH)
    patent = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.patent_types,
                         usecols=["appln_auth", "appln_publn_number", "internat_appln_id", "publn_kind", "key_appln_nr",
                                  "docdb_family_id", "ispriority", "ipr_type", "appln_filing_date", "appln_publn_date",
                                  "grant_publn_date", "appln_nr"])
    patent["key_appln_nr"] = patent["key_appln_nr"].apply(lambda a: a.strip())
    for f_date in ["appln_filing_date", "appln_publn_date", "grant_publn_date"]:
        patent[f_date] = patent[f_date].apply(to_date_str)

    # patent[["appln_filing_date", "appln_publn_date", "grant_publn_date"]] = patent[
    #    ["appln_filing_date", "appln_publn_date", "grant_publn_date"]].apply(pd.to_datetime)

    fam = pd.read_csv("families.csv", sep="|", encoding="utf-8", dtype=types.patent_types)
    for f_date in ["earliest_publn_date", "earliest_application_date", "date_first_granted"]:
        fam[f_date] = fam[f_date].apply(to_date_str)

    # fam[["earliest_publn_date", "earliest_application_date", "date_first_granted"]] = fam[
    #    ["earliest_publn_date", "earliest_application_date", "date_first_granted"]].apply(pd.to_datetime)
    fam[["title_en", "title_fr", "title_default", "title_default_language", "abstract_en", "abstract_fr",
         "abstract_default_language", "abstract_default"]] = fam[
        ["title_en", "title_fr", "title_default", "title_default_language", "abstract_en", "abstract_fr",
         "abstract_default_language", "abstract_default"]].fillna("")

    for col in ["is_oeb", "is_international"]:
        fam[col] = fam[col].apply(lambda a: True if a == 1 else False)

    fam_techno = pd.read_csv("families_technologies.csv", sep="|", encoding="utf-8", dtype=types.patent_types)

    part = pd.read_csv("part_p08.csv", sep="|", encoding="utf-8", dtype=types.partfin_types, engine="python")
    part["key_appln_nr"] = part["key_appln_nr"].apply(lambda a: a.strip())
    part["key_appln_nr_person"] = part["key_appln_nr_person"].replace(r"\s+", "", regex=True)
    part["id_personne"] = part["id_personne"].replace(r"\s+", "", regex=True)
    part["siren"] = part["siren"].fillna("")

    role = pd.read_csv("role.csv", sep="|", encoding="utf-8")
    role["key_appln_nr_person"] = role["key_appln_nr_person"].replace(r"\s+", "", regex=True)

    dep = set(role.loc[role["role"] == "dep", "key_appln_nr_person"])
    inv = set(role.loc[role["role"] == "inv", "key_appln_nr_person"])

    liste_pseudo = list_id(6, 3000000)

    depextra = list(part.loc[(part["key_appln_nr_person"].isin(inv)) & (part["type"] == "pm") & (
        part["id_paysage"].notna()), "key_appln_nr_person"].unique())

    dep.update(depextra)

    [inv.remove(item) for item in depextra]

    inventeur = part.copy()
    inventeur = inventeur.loc[inventeur["key_appln_nr_person"].isin(inv)]

    df_pseudo = pd.DataFrame(data={"id_pseudo": liste_pseudo})

    deposant = part.copy()
    deposant = deposant.loc[deposant["key_appln_nr_person"].isin(dep)]
    deposant["virgules"] = deposant["name_corrected"].apply(lambda a: "virgules" if ",, " in a else "")
    dep_virg = deposant.loc[
        (deposant["virgules"] == "virgules") & (deposant["siren"] != "")]
    dep_virg["liste_name"] = dep_virg["name_corrected"].str.split(",, ")
    dep_virg["liste_siren"] = dep_virg["siren"].str.split(",, ")
    dep_virg["liste_paysage"] = dep_virg["id_paysage"].str.split(",, ")
    dep_virg2 = dep_virg.explode(["liste_name", "liste_siren", "liste_paysage"])
    dep_virg2 = dep_virg2.drop(columns=["name_corrected", "siren", "id_paysage"]).rename(
        columns={"liste_name": "name_corrected", "liste_siren": "siren",
                 "liste_paysage": "id_paysage"}).drop_duplicates().reset_index(drop=True)
    dep_virg2 = dep_virg2[deposant.columns].drop(columns="virgules")
    dep_virg2 = dep_virg2.reset_index()

    df_pseudo = df_pseudo.reset_index()

    dep_virg3 = pd.merge(dep_virg2, df_pseudo, on="index", how="left").drop(columns="index")
    df_pseudo2 = df_pseudo.loc[~df_pseudo["id_pseudo"].isin(dep_virg3["id_pseudo"])]
    df_pseudo2 = df_pseudo2.drop(columns="index").reset_index()

    dep_aut = deposant.loc[~deposant["key_appln_nr_person"].isin(dep_virg["key_appln_nr_person"])].reset_index(
        drop=True)
    dep_aut = dep_aut.reset_index()

    dep_aut2 = pd.merge(dep_aut, df_pseudo2, on="index", how="left").drop(columns="index")
    df_pseudo3 = df_pseudo2.loc[~df_pseudo2["id_pseudo"].isin(dep_aut2["id_pseudo"])]
    df_pseudo3 = df_pseudo3.drop(columns="index").reset_index()

    inventeur = inventeur.reset_index()

    inventeur2 = pd.merge(inventeur, df_pseudo3, on="index", how="left").drop(columns="index")

    deposant2 = pd.concat([dep_virg3, dep_aut2], ignore_index=True).drop_duplicates()

    pat_dict = {}
    for row in fam_techno.itertuples():
        family_id = row.docdb_family_id
        level = row.level
        item = {level: []}
        if family_id not in pat_dict:
            pat_dict[family_id] = {"groupe": [], "ss_classe": [], "classe": [], "section": []}

        elt = {"code": row.code, "label": row.libelle}
        if elt not in pat_dict[family_id][level]:
            pat_dict[family_id][level].append(elt)

    res = []
    for family_id in pat_dict:
        new_elt = {'family_id': family_id, 'cpc': pat_dict[family_id]}
        res.append(new_elt)

    df_fam = pd.DataFrame(res)

    patent["publicationNumber"] = patent["appln_publn_number"].astype(str) + patent["publn_kind"]
    patent["links"] = patent["appln_auth"] + patent["publicationNumber"]
    patent["internatApplicationNumber"] = patent["internat_appln_id"].apply(lambda a: "" if a == 0 else str(a))
    patent["isPriority"] = patent["ispriority"].apply(lambda a: True if a == 1 else False)
    patent.loc[patent["grant_publn_date"].notna(), "isGranted"] = True
    patent.loc[patent["grant_publn_date"].isna(), "isGranted"] = False
    patent["yearPublication"] = patent["appln_publn_date"].apply(get_year)
    pat_json = patent[
        ["docdb_family_id", "key_appln_nr", "isPriority", "ipr_type", "appln_auth", "appln_filing_date", "appln_nr",
         "internatApplicationNumber",
         "appln_publn_date", "yearPublication", "publicationNumber", "grant_publn_date", "isGranted", "links"]].rename(
        columns={"key_appln_nr": "id", "ipr_type": "ipType", "appln_auth": "office",
                 "appln_filing_date": "applicationDate",
                 "appln_nr": "applicationNumber", "appln_publn_date": "publicationDate",
                 "grant_publn_date": "grantedDate"})

    links_dict = {}
    for row in pat_json.itertuples():
        family_id = row.links
        elt = f"https://worldwide.espacenet.com/patent/search/?q=pn%3D%22{row.links}%22"
        if family_id not in links_dict:
            links_dict[family_id] = elt

    res = []
    for family_id in links_dict:
        new_elt = {'family_id': family_id, 'espacenetUrl': links_dict[family_id]}
        res.append(new_elt)

    df_links = pd.DataFrame(res)

    pat_json2 = pd.merge(pat_json, df_links, left_on="links", right_on="family_id", how="left").drop(
        columns=["family_id", "links"])

    pat_dict = {}
    for row in pat_json2.itertuples():
        family_id = row.docdb_family_id
        if family_id not in pat_dict:
            pat_dict[family_id] = []
        elt = {'id': row.id, "isPriority": row.isPriority, "ipType": row.ipType, "office": row.office,
               "applicationDate": row.applicationDate, "applicationNumber": row.applicationNumber,
               "internatApplicationNumber": row.internatApplicationNumber, "publicationDate": row.publicationDate,
               "yearPublication": row.yearPublication, "publicationNumber": row.publicationNumber,
               "grantedDate": row.grantedDate, "isGranted": row.isGranted, "espacenetUrl": row.espacenetUrl}
        if elt not in pat_dict[family_id]:
            pat_dict[family_id].append(elt)

    res = []
    for family_id in pat_dict:
        new_elt = {'family_id': family_id, 'patents': pat_dict[family_id]}
        res.append(new_elt)

    df_pat = pd.DataFrame(res)

    title_dict = {}
    for row in fam.itertuples():
        family_id = row.docdb_family_id
        if family_id not in title_dict:
            elt = {"fr": row.title_fr, "en": row.title_en, "default": row.title_default}
            title_dict[family_id] = elt
        if title_dict[family_id] != elt:
            title_dict[family_id] = elt

    res = []
    for family_id in title_dict:
        new_elt = {'family_id': family_id, 'title': title_dict[family_id]}
        res.append(new_elt)

    df_title = pd.DataFrame(res)

    abstract_dict = {}
    for row in fam.itertuples():
        family_id = row.docdb_family_id
        if family_id not in abstract_dict:
            elt = {"fr": row.abstract_fr, "en": row.abstract_en, "default": row.abstract_default}
            abstract_dict[family_id] = elt
        if abstract_dict[family_id] != elt:
            abstract_dict[family_id] = elt
    res = []
    for family_id in abstract_dict:
        new_elt = {'family_id': family_id, 'summary': abstract_dict[family_id]}
        res.append(new_elt)
    df_abstract = pd.DataFrame(res)

    depaffilition_dict = {}
    for row in deposant2.itertuples():
        family_id = row.id_pseudo
        grid = row.grid
        paysage = row.id_paysage
        siren = row.siren
        idref = row.idref
        rnsr = row.rnsr
        ids = []
        if grid is not np.nan:
            idd = {"id": grid, "type": "grid"}
            ids.append(idd)
        if paysage is not np.nan:
            idd = {"id": paysage, "type": "id_paysage"}
            ids.append(idd)
        if siren != "":
            idd = {"id": siren, "type": "siren"}
            ids.append(idd)
        if idref is not np.nan:
            idd = {"id": idref, "type": "idref"}
            ids.append(idd)
        if rnsr is not np.nan:
            idd = {"id": rnsr, "type": "rnsr"}
            ids.append(idd)

        name = row.name_corrected
        typ = row.type
        country = row.country_corrected
        dep_dict = {}

        if name:
            dep_dict["name"] = name
        if typ:
            if typ == "pm":
                dep_dict["type"] = "organisation"
            else:
                dep_dict["type"] = "person"
        if country:
            dep_dict["country"] = country
        if len(ids) > 0:
            dep_dict["ids"] = ids

        if family_id not in depaffilition_dict:
            depaffilition_dict[family_id] = dep_dict

    res = []

    for family_id in depaffilition_dict:
        new_elt = {'family_id': family_id, 'applicants': depaffilition_dict[family_id]}
        res.append(new_elt)

    df_depaffiliation = pd.DataFrame(res)

    part_dep = pd.merge(deposant2[["docdb_family_id", "id_pseudo"]], df_depaffiliation, left_on="id_pseudo",
                        right_on="family_id", how="left").drop(columns="family_id")

    dep_dict = {}
    for row in part_dep.itertuples():
        family_id = row.docdb_family_id
        if family_id not in dep_dict:
            dep_dict[family_id] = []
        elt = row.applicants
        if elt is not np.nan and elt not in dep_dict[family_id]:
            dep_dict[family_id].append(elt)

    res = []
    for family_id in dep_dict:
        new_elt = {'family_id': family_id, 'applicants': dep_dict[family_id]}
        res.append(new_elt)
    df_dep = pd.DataFrame(res)

    fam_dep = jointure(fam, df_dep)

    invaffilition_dict = {}
    for row in inventeur2.itertuples():
        family_id = row.id_pseudo
        paysage = row.id_paysage
        idref = row.idref
        ids = []
        if paysage is not np.nan:
            idd = {"id": paysage, "type": "id_paysage"}
            ids.append(idd)
        if idref is not np.nan:
            idd = {"id": idref, "type": "idref"}
            ids.append(idd)
        name = row.name_corrected
        country = row.country_corrected
        inv_dict = {}

        if name:
            inv_dict["name"] = name
        if country:
            inv_dict["country"] = country
        inv_dict["type"] = "person"
        if len(ids) > 0:
            inv_dict["ids"] = ids

        if family_id not in invaffilition_dict:
            invaffilition_dict[family_id] = inv_dict

    for family_id in invaffilition_dict:
        new_elt = {'family_id': family_id, 'inventors': invaffilition_dict[family_id]}
        res.append(new_elt)

    df_invaffiliation = pd.DataFrame(res)

    part_inv = pd.merge(inventeur2[["docdb_family_id", "id_pseudo"]], df_invaffiliation, left_on="id_pseudo",
                        right_on="family_id", how="left").drop(columns="family_id")

    inv_dict = {}
    for row in part_inv.itertuples():
        family_id = row.docdb_family_id
        if family_id not in inv_dict:
            inv_dict[family_id] = []
        elt = row.inventors
        if elt is not np.nan and elt not in dep_dict[family_id]:
            inv_dict[family_id].append(elt)

    res = []
    for family_id in inv_dict:
        new_elt = {'family_id': family_id, 'inventors': inv_dict[family_id]}
        res.append(new_elt)
    df_inv = pd.DataFrame(res)

    fam_inv = jointure(fam_dep, df_inv)
    fam_titles = jointure(fam_inv, df_title)
    fam_sum = jointure(fam_titles, df_abstract)
    fam_cpc = jointure(fam_sum, df_fam)
    fam_pat = jointure(fam_cpc, df_pat)
    fam_final = fam_pat.drop(
        columns=["design_patents_count", "patents_count", "utility_models_count", "is_granted", "title_en", "title_fr",
                 "title_default_language", "title_default", "abstract_en", "abstract_fr", "abstract_default_language",
                 "abstract_default"])
    fam_final["year"] = fam_final["earliest_application_date"].apply(get_year)
    fam_final = fam_final.loc[fam_final["year"] >= 2010]
    fam_final["productionType"] = "patent"
    fam_final["inventionKind"] = "brevet"
    fam_final['isOa'] = False
    fam_final = fam_final.rename(
        columns={"docdb_family_id": "id", "inpadoc_family_id": "inpadocFamily",
                 "earliest_publn_date": "publicationDate",
                 "earliest_application_date": "applicationDate", "is_oeb": "isOeb",
                 "is_international": "isInternational",
                 "date_first_granted": "grantedDate"})
    for str_field in ["id", "inpadocFamily"]:
        fam_final[str_field] = fam_final[str_field].apply(lambda x: str(x))
    fam_final.to_json("fam_final_json.jsonl", orient="records", lines=True)
    swift.upload_object('patstat', 'fam_final_json.jsonl')
