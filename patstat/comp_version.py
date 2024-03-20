# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de créer le fichier au format JSON qui va alimenter ScanR


import os

import numpy as np
import pandas as pd
from utils import swift

from patstat import dtypes_patstat_declaration as types

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


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

    depextra = list(part.loc[(part["key_appln_nr_person"].isin(inv)) & (part["type"] == "pm") & (
        part["id_paysage"].notna()), "key_appln_nr_person"].unique())

    dep.update(depextra)

    [inv.remove(item) for item in depextra]

    deposant = part.copy()
    deposant = deposant.loc[deposant["key_appln_nr_person"].isin(dep)]
    deposant["virgules"] = deposant["name_corrected"].apply(lambda a: "virgules" if ",, " in a else "")
    dep_virg = deposant.loc[
        (deposant["virgules"] == "virgules") & (deposant["siren"] != "")]
    dep_virg["liste_name"] = dep_virg["name_corrected"].str.split(",, ")
    dep_virg["liste_siren"] = dep_virg["siren"].str.split(",, ")
    dep_virg2 = dep_virg.explode(["liste_name", "liste_siren"])
    dep_virg2 = dep_virg2.drop(columns=["name_corrected", "siren"]).rename(
        columns={"liste_name": "name_corrected", "liste_siren": "siren"}).drop_duplicates().reset_index(drop=True)
    dep_virg2 = dep_virg2[deposant.columns].drop(columns="virgules")
    dep_aut = deposant.loc[~deposant["key_appln_nr_person"].isin(dep_virg["key_appln_nr_person"])]
    deposant2 = pd.concat([dep_virg2, dep_aut], ignore_index=True)

    inventeur = part.copy()
    inventeur = inventeur.loc[inventeur["key_appln_nr_person"].isin(inv)]

    pat_dict = {}
    for row in fam_techno.itertuples():
        family_id = row.docdb_family_id
        if family_id not in pat_dict:
            pat_dict[family_id] = []
        elt = {'level': row.level, "code": row.code, "type": "cpc", "label": {"default": row.libelle}}
        if elt not in pat_dict[family_id]:
            pat_dict[family_id].append(elt)

    res = []
    for family_id in pat_dict:
        new_elt = {'family_id': family_id, 'domains': pat_dict[family_id]}
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
        if family_id not in links_dict:
            links_dict[family_id] = []
        elt = {"type": "", "url": f"https://worldwide.espacenet.com/patent/search/?q=pn%3D%22{row.links}%22",
               "label": "espacenet"}
        if elt not in links_dict[family_id]:
            links_dict[family_id].append(elt)

    res = []
    for family_id in links_dict:
        new_elt = {'family_id': family_id, 'links': links_dict[family_id]}
        res.append(new_elt)

    df_links = pd.DataFrame(res)

    pat_json2 = pd.merge(pat_json, df_links, left_on="links", right_on="family_id", how="left").drop(
        columns=["links_x", "family_id"]).rename(columns={"links_y": "links"})

    pat_dict = {}
    for row in pat_json2.itertuples():
        family_id = row.docdb_family_id
        if family_id not in pat_dict:
            pat_dict[family_id] = []
        elt = {'id': row.id, "isPriority": row.isPriority, "ipType": row.ipType, "office": row.office,
               "applicationDate": row.applicationDate, "applicationNumber": row.applicationNumber,
               "internatApplicationNumber": row.internatApplicationNumber, "publicationDate": row.publicationDate,
               "yearPublication": row.yearPublication, "publicationNumber": row.publicationNumber,
               "grantedDate": row.grantedDate, "isGranted": row.isGranted, "links": row.links}
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
        family_id = row.key_appln_nr_person
        if family_id not in depaffilition_dict:
            depaffilition_dict[family_id] = []
        elt = row.siren
        idref = row.idref
        name = row.name_corrected
        typ = row.type
        country = row.country_corrected
        dep_dict = {}

        if name:
            dep_dict["fullName"] = name
        if elt != "":
            dep_dict["siren"] = elt
        if idref is not np.nan:
            dep_dict["idref"] = idref
        if typ:
            dep_dict["typeParticipant"] = typ
        if country:
            dep_dict["country"] = country

        depaffilition_dict[family_id].append(dep_dict)

    invaffilition_dict = {}
    for row in inventeur.itertuples():
        family_id = row.key_appln_nr_person
        if family_id not in invaffilition_dict:
            invaffilition_dict[family_id] = []
        idref = row.idref
        name = row.name_corrected
        country = row.country_corrected
        inv_dict = {}

        if name:
            inv_dict["fullName"] = name
        if idref is not np.nan:
            inv_dict["idref"] = idref
        if country:
            dep_dict["country"] = country
        inv_dict["typeParticipant"] = "pp"

        invaffilition_dict[family_id].append(inv_dict)

    res = []
    for family_id in depaffilition_dict:
        new_elt = {'family_id': family_id, 'affiliations': depaffilition_dict[family_id]}
        res.append(new_elt)
    df_depaffiliation = pd.DataFrame(res)

    part_dep = pd.merge(part[["docdb_family_id", "key_appln_nr_person"]], df_depaffiliation,
                        left_on="key_appln_nr_person", right_on="family_id", how="inner").drop(
        columns="family_id")

    res = []
    for family_id in invaffilition_dict:
        new_elt = {'family_id': family_id, 'affiliations': invaffilition_dict[family_id]}
        res.append(new_elt)
    df_invaffiliation = pd.DataFrame(res)

    part_inv = pd.merge(part[["docdb_family_id", "key_appln_nr_person"]], df_invaffiliation,
                        left_on="key_appln_nr_person", right_on="family_id", how="inner").drop(
        columns="family_id")

    deposant_dict = {}
    for row in part_dep.itertuples():
        family_id = row.docdb_family_id
        if family_id not in deposant_dict:
            deposant_dict[family_id] = []
            elt = row.affiliations
        if elt and elt not in deposant_dict[family_id]:
            deposant_dict[family_id].append(elt)

    inventeur_dict = {}
    for row in part_inv.itertuples():
        family_id = row.docdb_family_id
        if family_id not in inventeur_dict:
            inventeur_dict[family_id] = []
            elt = row.affiliations
        if elt and elt not in inventeur_dict[family_id]:
            inventeur_dict[family_id].append(elt)

    res = []
    for family_id in deposant_dict:
        new_elt = {'family_id': family_id, 'applicants': deposant_dict[family_id]}
        res.append(new_elt)
    df_deposants = pd.DataFrame(res)

    res = []
    for family_id in inventeur_dict:
        new_elt = {'family_id': family_id, 'inventors': inventeur_dict[family_id]}
        res.append(new_elt)
    df_inventeurs = pd.DataFrame(res)

    df_part = pd.merge(df_deposants, df_inventeurs, on="family_id", how="outer")

    part_dict = {}
    for row in df_part.itertuples():
        family_id = row.family_id
        applicants = row.applicants
        inventors = row.inventors
        pdict = {}
        if applicants:
            pdict["applicants"] = applicants
        if inventors:
            pdict["inventors"] = inventors
        if family_id not in part_dict:
            part_dict[family_id] = pdict

    res = []
    for family_id in part_dict:
        new_elt = {'family_id': family_id, 'authors': part_dict[family_id]}
        res.append(new_elt)
    df_authors = pd.DataFrame(res)

    fam_authors = jointure(fam, df_authors)
    fam_titles = jointure(fam_authors, df_title)
    fam_sum = jointure(fam_titles, df_abstract)
    fam_domains = jointure(fam_sum, df_fam)
    fam_pat = jointure(fam_domains, df_pat)
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
                 "earliest_application_date": "submissionDate", "is_oeb": "isOeb",
                 "is_international": "isInternational",
                 "date_first_granted": "grantedDate"})
    for str_field in ["id", "inpadocFamily"]:
        fam_final[str_field] = fam_final[str_field].apply(lambda x: str(x))
    fam_final.to_json("fam_final_json.jsonl", orient="records", lines=True)
    swift.upload_object('patstat', 'fam_final_json.jsonl')
