# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de créer les fichiers au format CSV qui vont alimenter dataESR.
# This program creates the CSV files to publish on dataESR.


import os
import pandas as pd
from utils import swift

from patstat import dtypes_patstat_declaration as types

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def get_dataesr():
    # set working directory
    """
    This function selects a subset of columns and, for the families technologies, a subset of categories to avoid
    too large files as an output.
    It returns CSV files.
    """
    os.chdir(DATA_PATH)
    patent = pd.read_csv("patent.csv", sep="|", encoding="utf-8", dtype=types.patent_types,
                         engine="python")

    patent = patent[['appln_nr_epodoc',
                     'appln_filing_date',
                     'appln_auth',
                     'ipr_type',
                     'appln_publn_number',
                     'internat_appln_id', 'publn_kind',
                     'docdb_family_id',
                     'ispriority',
                     'appln_publn_date',
                     'grant_publn_date',
                     'appln_title_lg',
                     'appln_title']]

    patent[["appln_filing_date", "appln_publn_date", "grant_publn_date"]] = patent[
        ["appln_filing_date", "appln_publn_date", "grant_publn_date"]].apply(pd.to_datetime)

    patent["ispriority"] = patent["ispriority"].apply(lambda a: "oui" if a == 1 else "non")
    patent["internat_appln_id"] = patent["internat_appln_id"].apply(lambda a: "" if a == 0 else str(a))

    patent = patent.rename(columns={'appln_nr_epodoc': "nr_demande_epodoc",
                                    'appln_filing_date': "date_demande",
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

    fam[["earliest_publn_date", "earliest_application_date", "date_first_granted"]] = fam[
        ["earliest_publn_date", "earliest_application_date", "date_first_granted"]].apply(pd.to_datetime)

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

    fam_techno = pd.read_csv("families_technologies.csv", sep="|", encoding="utf-8",
                             dtype=types.patent_types)

    fam_techno = fam_techno.loc[fam_techno["level"].isin(["section", "classe", "ss_classe"])]
    fam_techno["level"] = fam_techno["level"].apply(lambda a: "sous-classe" if a == "ss_classe" else a)
    fam_techno = fam_techno.rename(columns={'docdb_family_id': "nr_famille_docdb",
                                            "level": "niveau"})

    patent.to_csv("patent_dataesr.csv", sep="|", index=False, encoding="utf-8")
    fam.to_csv("families_dataesr.csv", sep="|", index=False, encoding="utf-8")
    fam_techno.to_csv("families_technologies_dataesr.csv", sep="|", index=False, encoding="utf-8")

    swift.upload_object('patstat', 'patent_dataesr.csv')
    swift.upload_object('patstat', 'families_dataesr.csv')
    swift.upload_object('patstat', 'families_technologies_dataesr.csv')
