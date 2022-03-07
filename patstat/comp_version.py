# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de requêter l'API de Patstat et de télécharger les fichiers zippés.
# This script collects PATSTAT Global data from EPO's API, download and write zipped folders

import datetime
import dtypes_patstat_declaration as types
import os
import pandas as pd

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# set working directory
os.chdir(DATA_PATH)

patent = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.patent_types,
                     usecols=["appln_auth", "appln_publn_number", "internat_appln_id", "publn_kind", "key_appln_nr",
                              "docdb_family_id", "ispriority", "ipr_type", "appln_filing_date", "appln_publn_date",
                              "grant_publn_date", "appln_nr"])
patent["key_appln_nr"] = patent["key_appln_nr"].apply(lambda a: a.strip())
patent[["appln_filing_date", "appln_publn_date", "grant_publn_date"]] = patent[
    ["appln_filing_date", "appln_publn_date", "grant_publn_date"]].apply(lambda a: pd.to_datetime(a), axis=1)
fam = pd.read_csv("families.csv", sep="|", encoding="utf-8", dtype=types.patent_types)
fam_techno = pd.read_csv("families_technologies.csv", sep="|", encoding="utf-8", dtype=types.patent_types)
part = pd.read_csv("part.csv", sep="|", encoding="utf-8", dtype=types.partfin_types)
part["key_appln_nr"] = part["key_appln_nr"].apply(lambda a: a.strip())
part["key_appln_nr_person"] = part["key_appln_nr_person"].replace(r"\s+", "", regex=True)
part["id_personne"] = part["id_personne"].replace(r"\s+", "", regex=True)

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
    new_elt = {'family_id': family_id}
    new_elt['domains'] = pat_dict[family_id]
    res.append(new_elt)

df_fam = pd.DataFrame(res)

patent["publicationNumber"] = patent["appln_publn_number"].astype(str) + patent["publn_kind"]
patent["links"] = patent["appln_auth"] + patent["publicationNumber"]
patent["internatApplicationNumber"] = patent["internat_appln_id"].apply(lambda a: "" if a == 0 else str(a))
patent["isPriority"] = patent["ispriority"].apply(lambda a: True if a == 1 else False)
pat_json = patent[
    ["docdb_family_id", "key_appln_nr", "isPriority", "ipr_type", "appln_auth", "appln_filing_date", "appln_nr",
     "internatApplicationNumber",
     "appln_publn_date", "publicationNumber", "grant_publn_date", "links"]].rename(
    columns={"key_appln_nr": "id", "ipr_type": "ipType", "appln_auth": "office", "appln_filing_date": "applicationDate",
             "appln_nr": "applicationNumber", "appln_publn_date": "publicationDate", "grant_publn_date": "grantedDate"})

links_dict = {}
for row in pat_json.itertuples():
    family_id = row.links
    if family_id not in links_dict:
        links_dict[family_id] = []
    elt = {0: {"type": "", "url": f"https://worldwide.espacenet.com/patent/search/?q=pn%3D%22{row.links}%22"},
           "label": "espacenet"}
    if elt not in links_dict[family_id]:
        links_dict[family_id].append(elt)

res = []
for family_id in links_dict:
    new_elt = {'family_id': family_id}
    new_elt['links'] = links_dict[family_id]
    res.append(new_elt)

df_links = pd.DataFrame(res)

pat_json2 = pd.merge(pat_json, df_links, left_on="links", right_on="family_id", how="left").drop(
    columns=["links", "family_id"])

pat_dict = {}
for row in pat_json2.itertuples():
    family_id = row.docdb_family_id
    if family_id not in pat_dict:
        pat_dict[family_id] = []
    elt = {'id': row.id, "isPriority": row.isPriority, "ipType": row.ipType, "office": row.office,
           "applicationDate": row.applicationDate, "applicationNumber": row.applicationNumber,
           "internatApplicationNumber": row.internatApplicationNumber, "pulicationDate": row.publicationDate,
           "publicationNumber": row.publicationNumber, "grantedDate": row.grantedDate, "links": row.links}
    if elt not in pat_dict[family_id]:
        pat_dict[family_id].append(elt)

res = []
for family_id in pat_dict:
    new_elt = {'family_id': family_id}
    new_elt['patents'] = pat_dict[family_id]
    res.append(new_elt)

df_pat = pd.DataFrame(res)
