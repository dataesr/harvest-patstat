import os
import pandas as pd
from application.server.main.logger import get_logger
from utils import swift
from patstat import dtypes_patstat_declaration as types
from patstat import csv_files_querying

logger = get_logger(__name__)

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

DICT = {
    "tls206_french_person_id": {'sep': ',', 'chunksize': 5000000, 'usecols': "person_id",
                                'dtype': types.tls206_types},
    "tls904_nuts": {'sep': ',', 'chunksize': 5000000, 'usecols': "nuts",
                                'dtype': {"nuts": object, "nuts_level": pd.Int64Dtype(), "nuts_label": object}}
}


def get_regions():
    os.chdir(DATA_PATH)
    part = pd.read_csv("part_p08.csv", sep="|", encoding="utf-8", engine="python",
                       dtype=types.partfin_types)

    min_year = part[["docdb_family_id", "year"]].drop_duplicates()
    min_year["year"] = min_year["year"].astype(str)
    ct_min_year = pd.pivot_table(min_year, values="year", index="docdb_family_id", fill_value=0, aggfunc="min")
    ct_min_year = ct_min_year.rename(columns={"year": "year_min"})
    part2 = pd.merge(part, ct_min_year, on="docdb_family_id", how="inner").drop_duplicates()
    part2["year"] = part2["year"].astype(str)
    part2["comparaison"] = part2.apply(lambda a: "egal" if a.year == a.year_min else "diff", axis=1)
    part3 = part2.loc[part2["comparaison"] == "egal"].drop(columns=["comparaison", "year_min"])
    part3 = part3.rename(columns={"year": "year_min"})

    role = pd.read_csv("role.csv", sep="|", encoding="utf-8", engine="python")
    role = role.loc[role["role"] == "dep"]

    part3 = part3.loc[part3["key_appln_nr_person"].isin(role["key_appln_nr_person"])]

    t206 = csv_files_querying.filtering("tls206", part3, "person_id",
                                        DICT.get("tls206_french_person_id"))

    t206 = t206[["person_id", "nuts", "nuts_level"]].drop_duplicates().reset_index(drop=True)

    nuts = csv_files_querying.filtering("tls904", t206, "nuts",
                                        DICT.get("tls904_nuts"))

    t206n = pd.merge(t206, nuts, on=["nuts", "nuts_level"], how="left").drop_duplicates().reset_index(drop=True)
    partn = pd.merge(part3, t206n, on="person_id", how="left")
    partn.to_csv("part_region.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'part_region.csv')
    pat = pd.read_csv("patents.csv", sep="|", encoding="utf-8", engine="python", dtype=types.patent_types,
                      parse_dates=["grant_publn_date"])

    pat["grant_year"] = pat["grant_publn_date"].dt.year

    partn2 = partn.drop(columns='earliest_filing_date').drop_duplicates().reset_index().drop(columns="index")
    partn2018 = partn2.loc[partn2["year"] >= 2018]
    pat2 = pd.merge(pat, partn2018, on=['docdb_family_id',
                                     'inpadoc_family_id',
                                     'key_appln_nr',
                                     'appln_auth'], how="inner").drop_duplicates()
    pat2.to_csv("patpart_appln_region.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'patpart_appln_region.csv')

    patg = pat.loc[pat["grant_year"] >= 2018]
    patg2 = pd.merge(patg, partn, on=['docdb_family_id',
                                     'inpadoc_family_id',
                                     'key_appln_nr',
                                     'appln_auth'], how="inner").drop_duplicates()
    patg2.to_csv("patpart_grant_region.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'patpart_grant_region.csv')

