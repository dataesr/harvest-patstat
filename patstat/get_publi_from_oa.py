import pandas as pd
import os
from patstat import dtypes_patstat_declaration as types
from application.server.main.logger import get_logger
from utils import swift
import requests
from retry import retry

DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')

logger = get_logger(__name__)

@retry(tries=3, delay=5, backoff=5)
def request_oa(doi: str) -> dict:
    url_oa = f" https://api.openalex.org/works?filter=doi:{doi}"
    res_oa = requests.get(url_oa)
    if res_oa.status_code == 200:
        res = res_oa.json()
        for i in range(len(res['results'])):
            for l in range(len(res["results"][i]["authorships"])):
                dic = res["results"][i]["authorships"][l]["author"]
                print("doi")
                dic["doi"] = res["results"][i]["doi"]
                print("display_name_title")
                dic["display_name_title"] = res["results"][i]["display_name"]
                print("title")
                dic["title"] = res["results"][i]["title"]
                dic["id_pub"] = res["results"][i]["id"]
                dic["language"] = res["results"][i]["language"]
                dic["type"] = res["results"][i]["type"]
                dic["is_retracted"] = res["results"][i]["is_retracted"]
                dic["publication_year"] = res["results"][i]["publication_year"]
                dic["publication_date"] = res["results"][i]["publication_date"]
                dic["is_oa"] = res["results"][i]["open_access"]["is_oa"]
                dic["volume"] = res["results"][i]["biblio"]["volume"]
                dic["issue"] = res["results"][i]["biblio"]["issue"]
                dic["first_page"] = res["results"][i]["biblio"]["first_page"]
                dic["last_page"] = res["results"][i]["biblio"]["last_page"]
                dic["pdf_url"] = res["results"][i]["primary_location"]["pdf_url"]
                dic["raw_type"] = res["results"][i]["primary_location"]["raw_type"]
                dic["is_accepted"] = res["results"][i]["primary_location"]["is_accepted"]
                dic["is_published"] = res["results"][i]["primary_location"]["is_published"]
                print("raw_source_name")
                dic["raw_source_name"] = res["results"][i]["primary_location"]["raw_source_name"]
                if "source" in res["results"][i]["primary_location"]:
                    if isinstance(res["results"][i]["primary_location"]["source"], dict):
                        print("id_source")
                        dic["id_source"] = res["results"][i]["primary_location"]["source"]["id"]
                        print("issn_l")
                        dic["issn_l"] = res["results"][i]["primary_location"]["source"]["issn_l"]
                        print("host_organization_name")
                        dic["host_organization_name"] = res["results"][i]["primary_location"]["source"][
                            "host_organization_name"]
                else:
                    print("id_source None")
                    dic["id_source"] = None
                    print("issn_l None")
                    dic["issn_l"] = None
                    print("id_source None")
                    dic["host_organization_name"] = None
                keys_ins = []
                if len(res["results"][i]["authorships"][l]["institutions"]) > 0:
                    for li in range(len(res["results"][i]["authorships"][l]["institutions"])):
                        for key in res["results"][i]["authorships"][l]["institutions"][li].keys():
                            if key != "lineage":
                                key2 = key + "_ins"
                                keys_ins.append(key2)
                                dic[key2] = res["results"][i]["authorships"][l]["institutions"][li][key]
                for item in ["author", "institutions", "raw_affiliation_strings"]:
                    if item in dic.keys():
                        del dic[item]

    return dic


def get_info_publi():
    # set working directory
    os.chdir(DATA_PATH)

    publi = pd.read_csv("publi_brevets.csv", sep="|", encoding="utf-8", engine="python",
                        dtype=types.tls214_types)

    dois_uniques = list(publi["doi"].unique())

    authors = []
    for doi in dois_uniques:
        dic_authors = request_oa(doi)
        authors.append(dic_authors)

    df_authors = pd.DataFrame(authors)
    df_publi = df_authors[["doi", "display_name_title", "title", "id_pub", "language", "type", "is_retracted",
                           "publication_year", "publication_date", "is_oa", "volume", "issue", "first_page",
                           "last_page", "pdf_url", "raw_type", "is_accepted", "is_published", "raw_source_name",
                           "id_source", "issn_l", "host_organization_name"]].drop_duplicates().reset_index(drop=True)
    df_publi.to_csv("df_publi.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'df_publi.csv')

    df_authors2 = pd.merge(publi, df_authors, on="doi", how="left")
    df_authors2 = df_authors2.drop_duplicates().reset_index(drop=True)
    df_authors2.to_csv("df_authors.csv", sep="|", encoding="utf-8", index=False)
    swift.upload_object('patstat', 'df_authors.csv')



