# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de créer le fichier au format JSON qui va alimenter ScanR


import os
import pandas as pd

from patstat import dtypes_patstat_declaration as types

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

# set working directory
os.chdir(DATA_PATH)


def jointure(fm, df):
    fm2 = pd.merge(fm, df, left_on="docdb_family_id", right_on="family_id", how="left").drop(columns="family_id")
    return fm2


def get_json():
    patent = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.patent_types,
                         usecols=["appln_auth", "appln_publn_number", "internat_appln_id", "publn_kind", "key_appln_nr",
                                  "docdb_family_id", "ispriority", "ipr_type", "appln_filing_date", "appln_publn_date",
                                  "grant_publn_date", "appln_nr"])
    patent["key_appln_nr"] = patent["key_appln_nr"].apply(lambda a: a.strip())
    patent[["appln_filing_date", "appln_publn_date", "grant_publn_date"]] = patent[
        ["appln_filing_date", "appln_publn_date", "grant_publn_date"]].apply(pd.to_datetime)

    fam = pd.read_csv("families.csv", sep="|", encoding="utf-8", dtype=types.patent_types)
    fam[["earliest_publn_date", "earliest_application_date", "date_first_granted"]] = fam[
        ["earliest_publn_date", "earliest_application_date", "date_first_granted"]].apply(pd.to_datetime)
    fam[["title_en", "title_fr", "title_default", "title_default_language", "abstract_en", "abstract_fr",
         "abstract_default_language", "abstract_default"]] = fam[
        ["title_en", "title_fr", "title_default", "title_default_language", "abstract_en", "abstract_fr",
         "abstract_default_language", "abstract_default"]].fillna("")

    for col in ["is_oeb", "is_international"]:
        fam[col] = fam[col].apply(lambda a: True if a == 1 else False)

    fam_techno = pd.read_csv("families_technologies.csv", sep="|", encoding="utf-8", dtype=types.patent_types)

    part = pd.read_csv("part.csv", sep="|", encoding="utf-8", dtype=types.partfin_types)
    part["key_appln_nr"] = part["key_appln_nr"].apply(lambda a: a.strip())
    part["key_appln_nr_person"] = part["key_appln_nr_person"].replace(r"\s+", "", regex=True)
    part["id_personne"] = part["id_personne"].replace(r"\s+", "", regex=True)
    part["siren"] = part["siren"].fillna("")

    role = pd.read_csv("role.csv", sep="|", encoding="utf-8")
    role["key_appln_nr_person"] = role["key_appln_nr_person"].replace(r"\s+", "", regex=True)

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
    pat_json = patent[
        ["docdb_family_id", "key_appln_nr", "isPriority", "ipr_type", "appln_auth", "appln_filing_date", "appln_nr",
         "internatApplicationNumber",
         "appln_publn_date", "publicationNumber", "grant_publn_date", "links"]].rename(
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
               "internatApplicationNumber": row.internatApplicationNumber, "pulicationDate": row.publicationDate,
               "publicationNumber": row.publicationNumber, "grantedDate": row.grantedDate, "links": row.links}
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
            title_dict[family_id] = []
        elt = {"fr": row.title_fr, "en": row.title_en, "default": row.title_default}
        if elt not in title_dict[family_id]:
            title_dict[family_id].append(elt)

    res = []
    for family_id in title_dict:
        new_elt = {'family_id': family_id, 'title': title_dict[family_id]}
        res.append(new_elt)

    df_title = pd.DataFrame(res)

    abstract_dict = {}
    for row in fam.itertuples():
        family_id = row.docdb_family_id
        if family_id not in abstract_dict:
            abstract_dict[family_id] = []
        elt = {"fr": row.abstract_fr, "en": row.abstract_en, "default": row.abstract_default}
        if elt not in abstract_dict[family_id]:
            abstract_dict[family_id].append(elt)

    res = []
    for family_id in abstract_dict:
        new_elt = {'family_id': family_id, 'abstract': abstract_dict[family_id]}
        res.append(new_elt)

    df_abstract = pd.DataFrame(res)

    role_dict = {}
    for row in role.itertuples():
        family_id = row.key_appln_nr_person
        if family_id not in role_dict:
            role_dict[family_id] = []
        elt = {"role": row.role, "description": row.role}
        if elt not in role_dict[family_id]:
            role_dict[family_id].append(elt)

    res = []
    for family_id in role_dict:
        new_elt = {'family_id': family_id, 'rolePatent': role_dict[family_id]}
        res.append(new_elt)

    df_role = pd.DataFrame(res)

    part2 = pd.merge(part, df_role, left_on="key_appln_nr_person", right_on="family_id", how="left").drop(
        columns="family_id")

    affiliation_dict = {}
    for row in part2.itertuples():
        family_id = row.key_appln_nr_person
        if family_id not in affiliation_dict:
            affiliation_dict[family_id] = []
        elt = row.siren
        if elt not in affiliation_dict[family_id]:
            affiliation_dict[family_id].append(elt)

    res = []
    for family_id in affiliation_dict:
        new_elt = {'family_id': family_id, 'affiliations': affiliation_dict[family_id]}
        res.append(new_elt)

    df_affiliation = pd.DataFrame(res)

    part3 = pd.merge(part2, df_affiliation, left_on="key_appln_nr_person", right_on="family_id", how="left").drop(
        columns="family_id")

    authors_dict = {}
    for row in part3.itertuples():
        family_id = row.docdb_family_id
        if family_id not in authors_dict:
            authors_dict[family_id] = []
        elt = {"typeParticipant": row.type, "fullName": row.name_corrected, "country": row.country_corrected,
               "rolePatent": row.rolePatent, "affiliations": row.affiliations}
        if elt not in authors_dict[family_id]:
            authors_dict[family_id].append(elt)

    res = []
    for family_id in authors_dict:
        new_elt = {'family_id': family_id, 'authors': authors_dict[family_id]}
        res.append(new_elt)

    df_authors = pd.DataFrame(res)

    fam_authors = jointure(fam, df_authors)
    fam_titles = jointure(fam_authors, df_title)
    fam_sum = jointure(fam_titles, df_abstract)
    fam_domains = jointure(fam_sum, df_fam)
    fam_pat = jointure(fam_domains, df_pat)

    affiliation_dict2 = {}
    for row in part2.itertuples():
        family_id = row.docdb_family_id
        if family_id not in affiliation_dict2:
            affiliation_dict2[family_id] = []
        elt = row.siren
        if elt not in affiliation_dict2[family_id]:
            affiliation_dict2[family_id].append(elt)

    res = []
    for family_id in affiliation_dict2:
        new_elt = {'family_id': family_id, 'affiliations': affiliation_dict2[family_id]}
        res.append(new_elt)

    df_affiliation2 = pd.DataFrame(res)

    fam_affiliations = jointure(fam_pat, df_affiliation2)

    fam_final = fam_affiliations.drop(
        columns=["design_patents_count", "patents_count", "utility_models_count", "is_granted", "title_en", "title_fr",
                 "title_default_language", "title_default", "abstract_en", "abstract_fr", "abstract_default_language",
                 "abstract_default"])

    fam_final["year"] = fam_final["earliest_application_date"].apply(lambda a: a.year)
    fam_final["productionType"] = "patent"
    fam_final["inventionKind"] = "brevet"
    fam_final = fam_final.rename(
        columns={"docdb_family_id": "id", "inpadoc_family_id": "inpadocFamily",
                 "earliest_publn_date": "publicationDate",
                 "earliest_application_date": "submissionDate", "is_oeb": "isOeb",
                 "is_international": "isInternational",
                 "date_first_granted": "grantedDate"})

    fam_final_json = fam_final.to_json(orient="records", lines=True)
    fam_final.to_json("fam_final_json.json", orient="records", lines=True)
    return fam_final_json
