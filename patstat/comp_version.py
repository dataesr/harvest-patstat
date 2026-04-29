# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de créer le fichier au format JSON qui va alimenter ScanR


import os
import random
import string
import copy

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

    publi = pd.read_csv("publi_oa.csv", sep="|", encoding="utf-8", engine="python", dtype=types.oa_types)
    publi[
        ["id", "display_name", "orcid", "display_name_title", "title", "id_pub", "language", "type", "publication_year",
         "publication_date", "volume", "issue", "first_page", "last_page", "pdf_url", "raw_type", "raw_source_name",
         "id_source", "issn_l", "host_organization_name", "id_ins", "display_name_ins", "ror_ins", "country_code_ins",
         "type_ins", "id_paysage", "idref_ins", "grid", "siren", "siret"]] = publi[
        ["id", "display_name", "orcid", "display_name_title", "title", "id_pub", "language", "type", "publication_year",
         "publication_date", "volume", "issue", "first_page", "last_page", "pdf_url", "raw_type", "raw_source_name",
         "id_source", "issn_l", "host_organization_name", "id_ins", "display_name_ins", "ror_ins", "country_code_ins",
         "type_ins", "id_paysage", "idref_ins", "grid", "siren", "siret"]].fillna("")

    publi_dict = {}
    for r in publi.itertuples():
        family_id = r.docdb_family_id
        authid = r.id
        doi = r.doi_clean
        author = r.display_name
        orcid = r.orcid
        title = r.display_name_title
        language = r.language
        type = r.type
        year = r.publication_year
        isoa = r.is_oa
        pdf = r.pdf_url
        journal = r.raw_source_name
        oajournal = r.id_source
        issn = r.issn_l
        host = r.host_organization_name
        idins = r.id_ins
        ins = r.display_name_ins
        rorins = r.ror_ins
        country = r.country_code_ins
        typeins = r.type_ins
        idpaysage = r.id_paysage
        idref = r.idref_ins
        grid = r.grid
        siren = r.siren
        siret = r.siret

        affil_dict = {}
        if idins != "":
            if idins not in affil_dict:
                ids = []
                idd = {"id": idins, "type": "oa"}
                ids.append(idd)
                if rorins != "":
                    idd = {"id": rorins, "type": "ror"}
                    ids.append(idd)
                if idpaysage != "":
                    idd = {"id": idpaysage, "type": "id_paysage"}
                    ids.append(idd)
                if idref != "":
                    idd = {"id": idref, "type": "idref"}
                    ids.append(idd)
                if grid != "":
                    idd = {"id": grid, "type": "grid"}
                    ids.append(idd)
                if siren != "":
                    idd = {"id": siren, "type": "siren"}
                    ids.append(idd)
                if siret != "":
                    idd = {"id": siret, "type": "siret"}
                    ids.append(idd)
                affil_dict[idins] = {"name": ins, "country": country, "typeIns": typeins, "ids": ids}

        auth_dict = {}
        if author != "":
            if author not in auth_dict:
                auth_dict[author] = {"name": author}
                if affil_dict != dict():
                    auth_dict[author]["affiliations"] = affil_dict
                ids = []
                if authid != "":
                    idd = {"id": authid, "type": "oa"}
                    ids.append(idd)
                if orcid != "":
                    idd = {"id": orcid, "type": "orcid"}
                    ids.append(idd)
                if ids != []:
                    auth_dict[author]["ids"] = ids

        journal_dict = {}
        if journal != "":
            if journal not in journal_dict:
                journal_dict[journal] = {"name": journal}
                ids = []
                if oajournal != "":
                    idd = {"id": oajournal, "type": "oa"}
                    ids.append(idd)
                if issn != "":
                    idd = {"id": issn, "type": "issn"}
                    ids.append(idd)
                if ids != []:
                    journal_dict[journal]["ids"] = ids

        pub_dict = {}
        if doi != "":
            if doi not in pub_dict:
                pub_dict[doi] = {"title": title, "language": language, "doi": doi, "type": type,
                                 "publicationYear": year, "isOa": isoa, "pdfUrl": pdf, "hostOrganizationName": host}
                if journal_dict != dict():
                    pub_dict[doi]["journals"] = []
                    if journal_dict not in pub_dict[doi]["journals"]:
                        pub_dict[doi]["journals"].append(journal_dict)
                if auth_dict != dict():
                    pub_dict[doi]["authors"] = []
                    if auth_dict not in pub_dict[doi]["authors"]:
                        pub_dict[doi]["authors"].append(auth_dict)

        if family_id not in publi_dict:
            publi_dict[family_id] = {}

        if doi not in publi_dict[family_id]:
            publi_dict[family_id][doi] = {"title": title, "language": language, "doi": doi, "type": type,
                                          "publicationYear": year, "isOa": isoa, "pdfUrl": pdf,
                                          "hostOrganizationName": host}

        if journal_dict != dict():
            publi_dict[family_id][doi]["journals"] = journal_dict

        if auth_dict != dict() and "authors" not in list(publi_dict[family_id][doi].keys()):
            publi_dict[family_id][doi]["authors"] = [auth_dict]
        elif auth_dict != dict() and auth_dict not in publi_dict[family_id][doi]["authors"]:
            publi_dict[family_id][doi]["authors"].append(auth_dict)

    publi_dict2 = {}
    for family_id in publi_dict:
        if family_id not in publi_dict2:
            publi_dict2[family_id] = []
        for doi in publi_dict[family_id]:
            publi_dict2[family_id].append(copy.deepcopy(publi_dict[family_id][doi]))

        for item in publi_dict2[family_id]:
            if "journals" in item:
                journal_list = []
                for it in item["journals"]:
                    journal_list.append(item["journals"][it])
                item["journals"] = journal_list
            if "authors" in item:
                author_list = []
                for it in item["authors"]:
                    for author in it:
                        if "affiliations" in list(it[author].keys()):
                            affiliation_list = []
                            for aff in it[author]["affiliations"]:
                                affiliation_list.append(it[author]["affiliations"][aff])
                            it[author]["affiliations"] = affiliation_list
                        author_list.append(it[author])
                    item["authors"] = author_list

    res = []
    for family_id in publi_dict2:
        new_elt = {'family_id': family_id, 'publications': publi_dict2[family_id]}
        res.append(new_elt)
    df_publi = pd.DataFrame(res)

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

    liste_pseudo = list_id(6, 3500000)

    depextra = list(part.loc[(part["key_appln_nr_person"].isin(inv)) & (part["type"] == "pm") & (
        part["id_paysage"].notna()), "key_appln_nr_person"].unique())

    dep.update(depextra)

    [inv.remove(item) for item in depextra]

    inventeur = part.copy()
    inventeur = inventeur.loc[inventeur["key_appln_nr_person"].isin(inv)]

    df_pseudo = pd.DataFrame(data={"id_pseudo": liste_pseudo})

    deposant = part.copy()
    deposant = deposant.loc[deposant["key_appln_nr_person"].isin(dep)]
    deposant["virgules"] = deposant["name_corrected"].apply(lambda a: "virgules" if ",," in a else "")
    dep_virg = deposant.loc[
        (deposant["virgules"] == "virgules") & (deposant["siren"] != "")]
    dep_virg["liste_name"] = dep_virg["name_corrected"].str.split(r",,\s{0,}", regex=True)
    dep_virg["liste_siren"] = dep_virg["siren"].str.split(r",,\s{0,}", regex=True)
    dep_virg["liste_paysage"] = dep_virg["id_paysage"].str.split(r",,\s{0,}", regex=True)

    dep_virg["liste_grid"] = dep_virg["grid"].str.split(r",,\s{0,}", regex=True)
    dep_virg["liste_idref"] = dep_virg["idref"].str.split(r",,\s{0,}", regex=True)
    dep_virg["liste_ror"] = dep_virg["ror"].str.split(r",,\s{0,}", regex=True)

    dep_virg.loc[dep_virg["liste_paysage"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_paysage"].isna(), "liste_name"]
    dep_virg.loc[dep_virg["liste_paysage"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_paysage"].isna(), "missing"].apply(lambda a: ["" for x in a])
    dep_virg.loc[dep_virg["liste_paysage"].isna(), "liste_paysage"] = dep_virg.loc[
        dep_virg["liste_paysage"].isna(), "missing"]

    dep_virg.loc[dep_virg["liste_grid"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_grid"].isna(), "liste_name"]
    dep_virg.loc[dep_virg["liste_grid"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_grid"].isna(), "missing"].apply(lambda a: ["" for x in a])
    dep_virg.loc[dep_virg["liste_grid"].isna(), "liste_grid"] = dep_virg.loc[
        dep_virg["liste_grid"].isna(), "missing"]

    dep_virg.loc[dep_virg["liste_idref"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_idref"].isna(), "liste_name"]
    dep_virg.loc[dep_virg["liste_idref"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_idref"].isna(), "missing"].apply(lambda a: ["" for x in a])
    dep_virg.loc[dep_virg["liste_idref"].isna(), "liste_idref"] = dep_virg.loc[
        dep_virg["liste_idref"].isna(), "missing"]

    dep_virg.loc[dep_virg["liste_ror"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_ror"].isna(), "liste_name"]
    dep_virg.loc[dep_virg["liste_ror"].isna(), "missing"] = dep_virg.loc[
        dep_virg["liste_ror"].isna(), "missing"].apply(lambda a: ["" for x in a])
    dep_virg.loc[dep_virg["liste_ror"].isna(), "liste_ror"] = dep_virg.loc[
        dep_virg["liste_ror"].isna(), "missing"]

    dep_virg = dep_virg.drop(columns="missing")
    dep_virg2 = dep_virg.explode(
        ["liste_name", "liste_siren", "liste_paysage", "liste_grid", "liste_idref", "liste_ror"])
    dep_virg2 = dep_virg2.drop(columns=["name_corrected", "siren", "id_paysage", "grid", "idref", "ror"]).rename(
        columns={"liste_name": "name_corrected", "liste_siren": "siren",
                 "liste_paysage": "id_paysage", "liste_grid": "grid", "liste_idref": "idref",
                 "liste_ror": "ror"}).drop_duplicates().reset_index(drop=True)
    dep_virg2 = dep_virg2[deposant.columns].drop(columns="virgules")
    dep_virg2 = dep_virg2.reset_index()

    df_pseudo = df_pseudo.reset_index().drop(columns="index").reset_index()

    dep_virg3 = pd.merge(dep_virg2, df_pseudo, on="index", how="left").drop(columns="index")
    df_pseudo2 = df_pseudo.loc[~df_pseudo["id_pseudo"].isin(dep_virg3["id_pseudo"])]
    df_pseudo2 = df_pseudo2.drop(columns="index").reset_index().drop(columns="index").reset_index()

    dep_aut = deposant.loc[~deposant["key_appln_nr_person"].isin(dep_virg["key_appln_nr_person"])].reset_index(
        drop=True)
    dep_aut = dep_aut.reset_index()

    dep_aut2 = pd.merge(dep_aut, df_pseudo2, on="index", how="left").drop(columns="index")
    df_pseudo3 = df_pseudo2.loc[~df_pseudo2["id_pseudo"].isin(dep_aut2["id_pseudo"])]
    df_pseudo3 = df_pseudo3.drop(columns="index").reset_index().drop(columns="index").reset_index()

    inventeur = inventeur.reset_index().drop(columns="index").reset_index()

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
        family_id = row.docdb_family_id
        grid = row.grid
        paysage = row.id_paysage
        siren = row.siren
        idref = row.idref
        ror = row.ror
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
        if ror is not np.nan:
            idd = {"id": ror, "type": "ror"}
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
            depaffilition_dict[family_id] = []
            depaffilition_dict[family_id].append(dep_dict)
        else:
            depaffilition_dict[family_id].append(dep_dict)

    res = []

    for family_id in depaffilition_dict:
        new_elt = {'family_id': family_id, 'applicants': depaffilition_dict[family_id]}
        res.append(new_elt)

    df_depaffiliation = pd.DataFrame(res)

    dep_dict = {}
    for row in df_depaffiliation.itertuples():
        family_id = row.family_id
        if family_id not in dep_dict:
            dep_dict[family_id] = []

        for item in row.applicants:
            if item is not np.nan and item not in dep_dict[family_id]:
                dep_dict[family_id].append(item)

    res = []
    for family_id in dep_dict:
        new_elt = {'family_id': family_id, 'applicants': dep_dict[family_id]}
        res.append(new_elt)
    df_dep = pd.DataFrame(res)

    fam_dep = jointure(fam, df_dep)

    invaffilition_dict = {}
    for row in inventeur2.itertuples():
        family_id = row.docdb_family_id
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
            invaffilition_dict[family_id] = []
            invaffilition_dict[family_id].append(inv_dict)
        else:
            invaffilition_dict[family_id].append(inv_dict)

    res = []
    for family_id in invaffilition_dict:
        new_elt = {'family_id': family_id, 'inventors': invaffilition_dict[family_id]}
        res.append(new_elt)

    df_invaffiliation = pd.DataFrame(res)

    inv_dict = {}
    for row in df_invaffiliation.itertuples():
        family_id = row.family_id
        if family_id not in inv_dict:
            inv_dict[family_id] = []

        elt = row.inventors
        for item in elt:
            if item is not np.nan and item not in inv_dict[family_id]:
                inv_dict[family_id].append(item)

    res = []
    for family_id in inv_dict:
        new_elt = {'family_id': family_id, 'inventors': inv_dict[family_id]}
        res.append(new_elt)
    df_inv = pd.DataFrame(res)

    fam_inv = jointure(fam_dep, df_inv)
    fam_publi = jointure(fam_inv, df_publi)
    fam_titles = jointure(fam_publi, df_title)
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
