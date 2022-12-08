# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de créer les fichiers au format CSV qui vont alimenter dataESR.
# This program creates the CSV files to publish on dataESR.


import os
import re

import pandas as pd
import requests
from retry import retry

from patstat import dtypes_patstat_declaration as types
from utils import swift

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def to_date_str(x: str):
    """
    This function turns date into strings
    """
    res = str(x)[0:10]
    if len(res) == 10:
        res = res + "T00:00:00"
        return res
    return None


def requete(ul: str, form: str, ky: str) -> object:
    """
    This function queries a website.
    """
    adr = ul + form + ky
    res = requests.get(adr)
    status = res.status_code
    if status != 200:
        raise ConnectionError(f"Failed while trying to access the URL with status code {status}")
    else:
        print("URL successfully accessed", flush=True)
    return res


@retry(tries=3, delay=5, backoff=5)
def df_req(ul: str, frm: str, ky: str) -> pd.DataFrame:
    """
    This functions takes the text result (CSV with ; as separator) from the query, writes it and reads it as a df.
    It returns a df
    """
    res = requete(ul, frm, ky)
    text = res.text
    with open("structures.csv", "w") as f:
        f.write(text)
    df = pd.read_csv("structures.csv", sep=";", encoding="utf-8", engine="python",
                     dtype={"siret": pd.Int64Dtype(), "siren": str,
                            "identifiant_pic": pd.Int64Dtype(), "identifiant_cti": pd.Int64Dtype(),
                            "identifiant_rcr": pd.Int64Dtype(), "identifiant_orgref": pd.Int64Dtype(),
                            "element_fundref": pd.Int64Dtype(), "numero_telephone_uai": pd.Int64Dtype(),
                            "rce": pd.Int64Dtype(), "dev_immo": pd.Int64Dtype()})
    swift.upload_object('patstat', 'structures.csv')
    return df


def cpt_struct(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function checks how many Paysage ID there are for each unique SIREN.
    It returns a df.
    """
    cmpt_struc = df[["uo_lib", "siren", "identifiant_interne"]].drop_duplicates()
    cmpt_struc = cmpt_struc.loc[cmpt_struc["siren"].notna()]
    cmpt_struc = cmpt_struc.sort_values("siren")
    cmpt_struc = cmpt_struc[["siren", "identifiant_interne"]].groupby("siren").nunique(
        dropna=False).reset_index()

    return cmpt_struc


def get_dataesr():
    # set working directory
    """
    This function selects a subset of columns and, for the families technologies, a subset of categories to avoid
    too large files as an output.
    It returns CSV files.
    """
    os.chdir(DATA_PATH)

    ### PATENT ###
    patent = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.patent_types,
                         engine="python")

    patent = patent[['appln_filing_date',
                     'appln_auth',
                     'ipr_type',
                     'appln_publn_number',
                     'internat_appln_id', 'publn_kind',
                     'docdb_family_id',
                     'ispriority',
                     'appln_publn_date',
                     'grant_publn_date',
                     'appln_title_lg',
                     'appln_title',
                     "key_appln_nr"]]

    for f_date in ["appln_filing_date", "appln_publn_date", "grant_publn_date"]:
        patent[f_date] = patent[f_date].apply(to_date_str)

    patent["ispriority"] = patent["ispriority"].apply(lambda a: "oui" if a == 1 else "non")
    patent["internat_appln_id"] = patent["internat_appln_id"].apply(lambda a: "" if a == 0 else str(a))

    patent = patent.rename(columns={'appln_filing_date': "date_demande",
                                    'appln_auth': "autorite_demande",
                                    'ipr_type': "domaine_pi",
                                    'appln_publn_number': "nr_publication_demande",
                                    'internat_appln_id': "nr_demande_pct",
                                    'publn_kind': "type_publication",
                                    'docdb_family_id': "nr_famille_docdb",
                                    'ispriority': "demande_priorite",
                                    'appln_publn_date': "date_publication_demande",
                                    'grant_publn_date': "date_octroi",
                                    'appln_title_lg': 'langue_titre_demande',
                                    'appln_title': 'titre_demande', })

    ### FAMILIES ###
    fam = pd.read_csv("families.csv", sep="|", encoding="utf-8", dtype=types.patent_types)

    fam = fam[['docdb_family_id',
               'inpadoc_family_id',
               'earliest_publn_date',
               'earliest_application_date',
               'is_oeb',
               'is_international',
               'is_granted',
               'date_first_granted',
               'title_en',
               'title_fr',
               'title_default_language',
               'title_default',
               'abstract_en',
               'abstract_fr',
               'abstract_default_language',
               'abstract_default']]

    for f_date in ["earliest_publn_date", "earliest_application_date", "date_first_granted"]:
        fam[f_date] = fam[f_date].apply(to_date_str)

    fam["is_oeb"] = fam["is_oeb"].apply(lambda a: "vrai" if a == 1 else "faux")
    fam["is_international"] = fam["is_international"].apply(
        lambda a: "vrai" if a == 1 else "faux")
    fam["is_granted"] = fam["is_granted"].apply(
        lambda a: "vrai" if a == 1 else "faux")

    fam = fam.rename(columns={'docdb_family_id': "nr_famille_docdb",
                              'inpadoc_family_id': "nr_famille_inpadoc",
                              'earliest_publn_date': "date_premiere_publication",
                              'earliest_application_date': "date_premiere_demande",
                              'is_oeb': "demande_oeb",
                              'is_international': "demande_internationale",
                              'is_granted': "octroye",
                              'date_first_granted': "date_premier_octroi",
                              'title_en': "titre_anglais",
                              'title_fr': "titre_francais",
                              'title_default_language': "langue_originale_titre",
                              'title_default': "titre_langue_originale",
                              'abstract_en': "resume_anglais",
                              'abstract_fr': "resume_francais",
                              'abstract_default_language': "langue_originale_resume",
                              'abstract_default': "resume_langue_originale"})

    ### TECHNOLOGIES OF THE FAMILIES ###
    fam_techno = pd.read_csv("families_technologies.csv", sep="|", encoding="utf-8",
                             dtype=types.patent_types)

    fam_techno = fam_techno.loc[fam_techno["level"].isin(["section", "classe", "ss_classe"])]
    fam_techno["level"] = fam_techno["level"].apply(lambda a: "sous-classe" if a == "ss_classe" else a)
    fam_techno = fam_techno.rename(columns={'docdb_family_id': "nr_famille_docdb",
                                            "level": "niveau"})

    ### APPLICANTS ###

    ## ROLE -- to keep only apllicants, not inventors ##

    role = pd.read_csv("role.csv", sep="|", encoding="utf-8")
    role["key_appln_nr_person"] = role["key_appln_nr_person"].replace(r"\s+", "", regex=True)

    # from participant df, select only applicants and clean SIRET and SIRET in some cases

    deposant = pd.read_csv("part_p08.csv", sep="|", encoding="utf-8", engine="python", dtype=types.partfin_types)
    deposant["key_appln_nr_person"] = deposant["key_appln_nr_person"].replace(r"\s+", "", regex=True)

    deposant = deposant.loc[deposant["type"] == "pm"]
    role = role.loc[role["key_appln_nr_person"].isin(deposant["key_appln_nr_person"])]
    key_dep = set(role.loc[role["role"] == "dep", "key_appln_nr_person"])
    deposant = deposant.loc[deposant["key_appln_nr_person"].isin(key_dep)]

    # keep columns final df

    deposant2 = deposant[
        ["id_patent", "docdb_family_id", "inpadoc_family_id", "doc_std_name", "country_corrected",
         "id_paysage", "siren", "key_appln_nr_person"]]

    ### OUTPUTS - write all the df and send them to ObjectStorage ###

    patent.to_csv("patent_dataesr.csv", sep="|", index=False, encoding="utf-8")
    fam.to_csv("families_dataesr.csv", sep="|", index=False, encoding="utf-8")
    fam_techno.to_csv("families_technologies_dataesr.csv", sep="|", index=False, encoding="utf-8")
    deposant2.to_csv("deposant_dataesr.csv", sep="|", index=False, encoding="utf-8")

    swift.upload_object('patstat', 'patent_dataesr.csv')
    swift.upload_object('patstat', 'families_dataesr.csv')
    swift.upload_object('patstat', 'families_technologies_dataesr.csv')
    swift.upload_object('patstat', 'deposant_dataesr.csv')
