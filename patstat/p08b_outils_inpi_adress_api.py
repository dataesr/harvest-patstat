#!/usr/bin/env python
# coding: utf-8

import os
import re
import concurrent.futures
import json
import faulthandler
faulthandler.enable()

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
from retry import retry
from patstat import dtypes_patstat_declaration as types
from timeit import default_timer as timer
import logging
import threading
import sys
import xmltodict
from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


def get_logger(name):
    """
    This function helps to follow the execution of the parallel computation.
    """
    loggers = {}
    if name in loggers:
        return loggers[name]
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    fmt = '%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    loggers[name] = logger
    return loggers[name]


def doc_nb(lmf, ptdoc):
    global pb_n
    if "doc-number" in ptdoc.attrs.keys():
        pb_n = ptdoc["doc-number"]
    else:
        for item in lmf:
            if ".xml" in item:
                pb_n = int(item.replace(".xml", ""))

    return pb_n


def person_ref(bs, pb_n, ptdoc):
    people = ["applicant", "inventor"]
    liste = []
    for person in people:
        apps = bs.find_all(person)
        if apps:
            rg = 1
            for item in apps:
                dic_app = {"publication-number": pb_n,
                           "address-1": "",
                           "address-2": "",
                           "address-3": "",
                           "mailcode": "",
                           "pobox": "",
                           "room": "",
                           "address-floor": "",
                           "building": "",
                           "street": "",
                           "city": "",
                           "county": "",
                           "state": "",
                           "postcode": "",
                           "country": "",
                           "sequence": "",
                           "type-party": person,
                           "data-format-person": "",
                           "prefix": "",
                           "first-name": "",
                           "middle-name": "",
                           "last-name": "",
                           "orgname": "",
                           "name-complete": "",
                           "suffix": "",
                           "siren": "",
                           "role": "",
                           "department": "",
                           "synonym": "",
                           "designation": "",
                           "application-number-fr": ptdoc["id"]}

                clefs_dic = list(dic_app.keys())
                if "sequence" in item.attrs.keys():
                    dic_app["sequence"] = int(item["sequence"])
                else:
                    dic_app["sequence"] = rg
                    rg = rg + 1
                if "data-format" in item.attrs.keys():
                    dic_app["data-format-person"] = item.attrs["data-format"]
                if "designation" in item.attrs.keys():
                    dic_app["designation"] = item.attrs["designation"]
                tags_item = [tag.name for tag in item.find_all()]
                inter = list(set(clefs_dic).intersection(set(tags_item)))
                for clef in inter:
                    dic_app[clef] = item.find(clef).text
                if "iid" in tags_item:
                    dic_app["siren"] = item.find("iid").text

                ch_name = ""
                for name in ["first-name", "middle-name", "last-name", "orgname"]:
                    if name != "":
                        if ch_name == "":
                            ch_name = dic_app[name].lstrip().rstrip()
                        else:
                            ch_name = ch_name + " " + dic_app[name].lstrip().rstrip()
                    else:
                        pass

                ch_name = re.sub(r"\s+", " ", ch_name)

                dic_app["name-complete"] = ch_name

                liste.append(dic_app)

    if len(liste) == 0:
        dic_app = {"publication-number": pb_n,
                   "address-1": "",
                   "address-2": "",
                   "address-3": "",
                   "mailcode": "",
                   "pobox": "",
                   "room": "",
                   "address-floor": "",
                   "building": "",
                   "street": "",
                   "city": "",
                   "county": "",
                   "state": "",
                   "postcode": "",
                   "country": "",
                   "sequence": "",
                   "type-party": "",
                   "data-format-person": "",
                   "prefix": "",
                   "first-name": "",
                   "middle-name": "",
                   "last-name": "",
                   "orgname": "",
                   "name-complete": "",
                   "suffix": "",
                   "siren": "",
                   "role": "",
                   "department": "",
                   "synonym": "",
                   "designation": "",
                   "application-number-fr": ptdoc["id"]}
        liste.append(dic_app)

    df = pd.DataFrame(data=liste).drop_duplicates()

    for col in list(df.columns):
        col2 = col.replace("-", "_")
        df = df.rename(columns={col: col2})

    return df


def cleaning_address(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df.loc[df[col] == "F-75014 PARIS", col] = "75014 PARIS"
    df.loc[df[col] == "F-75015 PARIS", col] = "75015 PARIS"
    df[col] = df[col].str.replace(r"f\-{0,}(\d+)", "\\1", case=False,
                                  regex=True)
    df[col] = df[col].str.replace(r"\"", " ", regex=True)
    df[col] = df[col].str.replace(r"\'", " ", regex=True)
    df[col] = df[col].str.replace(r"«", "", regex=True)
    df[col] = df[col].str.replace(r"»", "", regex=True)
    df[col] = df[col].str.replace(r"\'\'", "", regex=True)
    df[col] = df[col].str.replace(r"\ufeff3", "", regex=True)
    df[col] = df[col].str.replace(r"^-+\s?", "", regex=True)
    df[col] = df[col].str.replace(r"^\.+\s?", "", regex=True)
    df[col] = df[col].str.replace(r"^\/+\s?", "", regex=True)
    df[col] = df[col].str.replace(r"&", " ", regex=True)
    df[col] = df[col].str.replace(r",\s{0,}", " ", regex=True)
    df[col] = df[col].str.replace(r"\_{1,}", " ", regex=True)
    df[col] = df[col].str.replace(r"[c|C]\/[o|O]", "", regex=True)
    df[col] = df[col].str.replace(r"\n", " ", regex=True)
    df[col] = df[col].str.replace(r"\s+", " ", regex=True)
    df[col] = df[col].str.replace(r"cedex\s{0,}\d{1,2}", "",
                                  case=False, regex=True)
    df[col] = df[col].apply(
        lambda a: "".join(c if c.isalnum() or c == " " else " " for c in a))
    df[col] = df[col].str.replace(r"\s+", " ", regex=True)
    df[col] = df[col].str.lower()
    df[col] = df[col].str.strip()

    return df


@retry(tries=20, delay=5, backoff=5)
def call_api_adresse(df: pd.DataFrame) -> list:
    lng = len(df)
    lng2 = len(df)
    res_adm = []
    start = timer()
    for _, r in df.iterrows():
        prct = lng / lng2 * 100
        if prct % 10 == 0:
            print(f"Il reste {prct} % de requêtes à effectuer", flush=True)
        ad = r["address_complete_fr"]
        res = requests.get(f"https://api-adresse.data.gouv.fr/search/?q={ad}&format=json")
        if res.status_code != 200:
            print(f"L'adresse {ad} a renvoyé l'erreur {res.status_code}", flush=True)
            tadm = pd.DataFrame(data={"address_complete_fr": [ad]})
            res_adm.append(tadm)
        else:
            res = res.json()
            if "features" not in res.keys():
                tadm = pd.DataFrame(data={"address_complete_fr": [ad]})
                res_adm.append(tadm)
            else:
                res2 = res["features"]
                for r2 in res2:
                    r2["address_complete_fr"] = ad
                    tadm = pd.json_normalize(r2)
                    res_adm.append(tadm)
        lng = lng - 1

    end = timer()
    m, s = divmod(end - start, 60)
    h, m = divmod(m, 60)
    time_str = "%02d:%02d:%02d" % (h, m, s)
    print(f"Durée de traitement : {time_str}", flush=True)

    return res_adm


def plmf(d_adm5: pd.DataFrame) -> pd.DataFrame:
    d_adm5.loc[d_adm5["properties.district"].notna(), "ville"] = d_adm5.loc[
        d_adm5["properties.district"].notna(), "properties.district"]
    d_adm5.loc[d_adm5["properties.district"].isna(), "ville"] = d_adm5.loc[
        d_adm5["properties.district"].isna(), "properties.city"]

    d_adm5["properties.postcode"] = d_adm5["properties.postcode"].astype(object)

    paris = pd.DataFrame(data={"code": ['75101',
                                        '75102',
                                        '75103',
                                        '75104',
                                        '75105',
                                        '75106',
                                        '75107',
                                        '75108',
                                        '75109',
                                        '75110',
                                        '75111',
                                        '75112',
                                        '75113',
                                        '75114',
                                        '75115',
                                        '75116',
                                        '75117',
                                        '75118',
                                        '75119',
                                        '75120'],
                               "postcode": ['75001',
                                            '75002',
                                            '75003',
                                            '75004',
                                            '75005',
                                            '75006',
                                            '75007',
                                            '75008',
                                            '75009',
                                            '75010',
                                            '75011',
                                            '75012',
                                            '75013',
                                            '75014',
                                            '75015',
                                            '75116',
                                            '75017',
                                            '75018',
                                            '75019',
                                            '75020'],
                               "ville2": [f"Paris {i + 1}e arrondissement" for i in range(20)]})

    paris.loc[paris["code"] == "75101", "ville2"] = "Paris 1er arrondissement"

    paris16 = pd.DataFrame(data={"code": ['75116'],
                                 "postcode": ['75016'],
                                 "ville2": "Paris 16e arrondissement"})

    paris = pd.concat([paris, paris16], ignore_index=True)
    paris = paris.sort_values(by=['code', 'postcode'])

    lyon = pd.DataFrame(data={"code": [f'6938{i + 1}' for i in range(9)],
                              "postcode": [f'6900{i + 1}' for i in range(9)],
                              "ville2": [f"Lyon {i + 1}e arrondissement" for i in range(9)]})

    lyon.loc[lyon["code"] == "69381", "ville2"] = "Lyon 1er arrondissement"

    marseille = pd.DataFrame(data={"code": ['13201',
                                            '13202',
                                            '13203',
                                            '13204',
                                            '13205',
                                            '13206',
                                            '13207',
                                            '13208',
                                            '13209',
                                            '13210', '13211', '13212', '13213', '13214', '13215', '13216'],
                                   "postcode": ['13001',
                                                '13002',
                                                '13003',
                                                '13004',
                                                '13005',
                                                '13006',
                                                '13007',
                                                '13008',
                                                '13009',
                                                '13010', '13011', '13012', '13013', '13014', '13015',
                                                '13016'],
                                   "ville2": [f"Marseille {i + 1}e arrondissement" for i in range(16)]})

    marseille.loc[marseille["code"] == "13201", "ville2"] = "Marseille 1er arrondissement"

    plm = pd.concat([paris, lyon, marseille], ignore_index=True)
    plm = plm.rename(columns={"postcode": "properties.postcode"})

    d_adm6 = pd.merge(d_adm5, plm, on="properties.postcode", how="left")

    d_adm6.loc[
        (d_adm6["code"].notna()) & (
                d_adm6["code"] != d_adm6["properties.citycode"]), "properties.citycode"] = \
        d_adm6.loc[(d_adm6["code"].notna()) & (d_adm6["code"] != d_adm6["properties.citycode"]), "code"]

    d_adm6.loc[(d_adm6["code"].notna()) & (d_adm6["ville"] != d_adm6["ville2"]), "ville"] = \
        d_adm6.loc[(d_adm6["code"].notna()) & (d_adm6["ville"] != d_adm6["ville2"]), "ville2"]

    d_adm6["ad"] = d_adm6["properties.postcode"] + " " + d_adm6["properties.citycode"] + " " + d_adm6[
        "ville"] + " " + d_adm6["properties.context"]

    d_adm6 = d_adm6.drop(columns=["code", "ville2"])

    return d_adm6


@retry(tries=20, delay=5, backoff=5)
def get_address(df_per: pd.DataFrame) -> pd.DataFrame:
    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "f"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "postcode"].str.replace(r"^F-?\d+", "present", regex=True)
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] != "present"), "f"] = "absent"
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present"), "postcode"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present"), "postcode"].str.replace(
        r"^[F|f]-?",
        "",
        regex=True)
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present"), "postcode"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present"), "postcode"].str.replace(
        r"F-", "", regex=True)

    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present"), "postcode"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present"), "postcode"].apply(
        lambda a: re.sub(r"^F-?", "", a))

    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "f"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "postcode"].apply(
        lambda a: "present" if bool(re.match('[a-zA-Z\s]+$', a)) else "absent")
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] != "present"), "f"] = "absent"
    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "pc"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "city"].apply(
        lambda a: "present" if bool(re.match('\d{5}', a)) else "absent")
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "pc2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city"].str.replace(r"[^\d{5}]",
                                                                                                      "",
                                                                                                      regex=True)
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city"].str.replace(
        r"\s{0,}\d{5}\s{0,}", "",
        regex=True)
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"].str.replace(
        r"^[,|\-]", "",
        regex=True)

    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"].str.replace(
        r"^\s+", "",
        regex=True)
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"].str.replace(
        r"\s+$", "",
        regex=True)

    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"].str.replace(
        r"\s{2,}", "",
        regex=True)

    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"] = df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc"] == "present"), "city2"].str.replace(
        r"-\s{1,}", "-",
        regex=True)

    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present") & (
            df_per["pc"] == "present"), "city2"] = df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) &
                                                              (df_per["f"] == "present") & (
                                                                      df_per["pc"] == "present"), "postcode"]

    df_per.loc[
        (df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present") & (df_per["pc"] == "present"), "city"] = \
        df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) &
                   (df_per["f"] == "present") & (df_per["pc"] == "present"), "city2"]

    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["f"] == "present") & (
            df_per["pc"] == "present"), "postcode"] = df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) &
                                                                 (df_per["f"] == "present") & (
                                                                         df_per["pc"] == "present"), "pc2"]

    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "postcode2"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "postcode"].apply(lambda a: a if bool(re.match('\d+', a)) else "")
    df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) & (df_per["pc2"].notna()) & (
            df_per["postcode2"] == ""), "postcode2"] = df_per.loc[(df_per["country"].isin(["XX", "FR", ""])) &
                                                                  (df_per["pc2"].notna()) & (
                                                                          df_per["postcode2"] == ""), "pc2"]
    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "city2"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""])].apply(
        lambda a: a.postcode if bool(re.match('[^\d+]', a.postcode)) else a.city2, axis=1)
    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "city2"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "city2"].fillna("")

    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "cedex"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "city2"].str.contains("cedex", case=False)
    df_per.loc[df_per["country"].isin(["XX", "FR", ""]), "cedex"] = df_per.loc[
        df_per["country"].isin(["XX", "FR", ""]), "cedex"].apply(lambda a: "cedex" if a is True else "non")

    df_per = df_per.drop(columns=["f", "pc", "pc2"])

    df_per = df_per.fillna("")

    dico = {'publication_number': [], 'address_1': [], 'address_2': [], 'address_3': [], 'mailcode': [],
            'pobox': [], 'room': [], 'address_floor': [], 'building': [], 'street': [], 'city': [],
            'county': [], 'state': [], 'postcode': [], 'country': [], 'sequence': [], 'type_party': [],
            'data_format_person': [], 'prefix': [], 'first_name': [], 'middle_name': [],
            'last_name': [], 'orgname': [], 'name_complete': [], 'suffix': [], 'siren': [], 'role': [],
            'department': [], 'synonym': [], 'designation': [], 'application_number_fr': [],
            'city2': [], 'postcode2': [], 'cedex': [], 'address_complete_fr': []}

    for _, r in df_per.iterrows():
        if r["country"] in ["XX", "FR", ""]:
            nom = ""
            if r["address_1"] != "":
                nom = nom + r["address_1"]
                if r["postcode2"] != "":
                    nom = nom + " " + r["postcode2"]
                    if r["city2"] != "":
                        nom = nom + " " + r["city2"]
                    else:
                        nom = nom + " " + r["city"]
            elif r["postcode2"] != "":
                nom = nom + r["postcode2"]
                if r["city2"] != "":
                    nom = nom + " " + r["city2"]
                else:
                    nom = nom + " " + r["city"]
            else:
                nom = nom + " " + r["city2"]
            nom.replace("\\n", " ")
            nom = nom.strip()
        else:
            nom = ""

        dico['publication_number'].append(r["publication_number"])
        dico['address_1'].append(r["address_1"])
        dico['address_2'].append(r["address_2"])
        dico['address_3'].append(r["address_3"])
        dico['mailcode'].append(r["mailcode"])
        dico['pobox'].append(r["pobox"])
        dico['room'].append(r["room"])
        dico['address_floor'].append(r["address_floor"])
        dico['building'].append(r["building"])
        dico['street'].append(r["street"])
        dico['city'].append(r["city"])
        dico['county'].append(r["county"])
        dico['state'].append(r["state"])
        dico['postcode'].append(r["postcode"])
        dico['country'].append(r["country"])
        dico['sequence'].append(r["sequence"])
        dico['type_party'].append(r["type_party"])
        dico['data_format_person'].append(r["data_format_person"])
        dico['prefix'].append(r["prefix"])
        dico['first_name'].append(r["first_name"])
        dico['middle_name'].append(r["middle_name"])
        dico['last_name'].append(r["last_name"])
        dico['orgname'].append(r["orgname"])
        dico['name_complete'].append(r["name_complete"])
        dico['suffix'].append(r["suffix"])
        dico['siren'].append(r["siren"])
        dico['role'].append(r["role"])
        dico['department'].append(r["department"])
        dico['synonym'].append(r["synonym"])
        dico['designation'].append(r["designation"])
        dico['application_number_fr'].append(r["application_number_fr"])
        dico['city2'].append(r["city2"])
        dico['postcode2'].append(r["postcode2"])
        dico['cedex'].append(r["cedex"])
        dico['address_complete_fr'].append(nom)

    df_dico2 = pd.DataFrame(dico)

    df_dico2 = cleaning_address(df_dico2, "address_complete_fr")

    df_dico2 = df_dico2[['publication_number', 'address_1', 'address_2', 'address_3', 'mailcode',
                         'pobox', 'room', 'address_floor', 'building', 'street', 'city',
                         'county', 'state', 'postcode', 'country', 'sequence', 'type_party',
                         'data_format_person', 'prefix', 'first_name', 'middle_name',
                         'last_name', 'orgname', 'name_complete', 'suffix', 'siren', 'role',
                         'department', 'synonym', 'designation', 'application_number_fr', 'address_complete_fr']]

    evite = ["A COMPLETER", "IPSWICH", "-", "---", "CB4 0WS", "FR", "L-1661", "MA02451",
             "C/O AMADEUS NORTH AMERICA 1050 WINTER STREET", "LAURELS ARDLEIGH ROAD", "14 RUE MASSENA",
             "1, RUE DES TILLEULS",
             "11 CHEMIN DES MULETS", "13 RUE DU GRAND SUD", "14 ALLEE DE LA CHENAIE",
             "15 AVENUE DES BRUYERES", "16 RUE MAURICE BOUCHOR", "17 RUE DE L\'ARBALETRE", "18 AVENUE RENE COTY",
             "20 RUE DE LA TERRASSE", "3 RUE MARGUERITTE CHAPON", "30, RUE MICHAL", "31 RUE DU VAL",
             "33 RUE DES ALOUETTES",
             "33 RUE LOUIS GUILLOUX", "36 AV DES MAROTS", "43 RUE JEAN PREVOST", "44 RUE DE LA LOUBIERE",
             "45 BD DU MARECHAL JOFFRE", "48 CHEMIN DU BEARD", "5 ALLEE DU GROS CHENE", "5 AVENUE VICTOR HUGO",
             "5 VILLA SISLEY", "558 CHE DE BON RENCONTRE", "56 RUE DE VOUILLE", "6 CHEMIN DE LA RIVOIRE",
             "6, RUE DES AMANDIERS", "6, RUE F CHARVET", "9 CHEMIN DE PELLEGRI", "BARRAT", "CLAMART", "FRESNE",
             "LA VERPILLERE", "LA VERRIE", "LE BOSSON", "ST FELIX", "0004 CHEMIN DE LA COLLINE", "- 00000", "- 00000 -",
             "00000", "00000 -", ""]

    df_evite = df_dico2.loc[df_dico2["address_complete_fr"].isin(evite)]
    df_nevite = df_dico2.loc[~df_dico2["address_complete_fr"].isin(evite)]

    res_adm = call_api_adresse(df_nevite)

    if len(res_adm) > 0:
        df_adm = pd.concat(res_adm, ignore_index=True)

        colonnes = ["feature", "geometry.type", "geometry.coordinates", "properties.id", "properties.banId",
                    "properties.name", "properties.x", "properties.y", "properties.type", "properties.importance",
                    "properties.housenumber", "properties.street"]
        for col in colonnes:
            if col in df_adm.columns:
                df_adm = df_adm.drop(columns=col)

        if "properties.label" in df_adm.columns:
            df_adm2 = df_adm.loc[~df_adm["properties.label"].isin(["1 Cote Guy de Maupassant 76380 Canteleu"])]
            df_adm2 = df_adm2.loc[df_adm2["properties.score"] >= 0.5]
            ad_max = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                "address_complete_fr").max().reset_index()
            df_adm2 = pd.merge(df_adm2, ad_max, on=["address_complete_fr", "properties.score"], how="inner")

            if len(df_adm2) == 0:
                df_adm_final = df_nevite.copy()
                df_adm_final["dep_id"] = ""
                df_adm_final["dep_nom"] = ""
                df_adm_final["reg_nom"] = ""
                df_adm_final["com_code"] = ""
                df_adm_final["ville"] = ""
                df_adm_final = df_adm_final.fillna("")
            else:
                max_score = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                    "address_complete_fr").max().reset_index().drop_duplicates().reset_index(drop=True)

                df_adm3 = pd.merge(df_adm2, max_score, on=["address_complete_fr", "properties.score"],
                                   how="inner").sort_values(
                    "address_complete_fr").reset_index(drop=True)

                col_missing = ["properties.postcode", "properties.citycode", "properties.city",
                               "properties.context", "properties.district"]
                for col in col_missing:
                    if col not in df_adm3.columns:
                        df_adm3[col] = np.nan

                df_adm4 = df_adm3[
                    ["address_complete_fr", "properties.postcode", "properties.citycode", "properties.city",
                     "properties.context", "properties.district"]].drop_duplicates().reset_index(drop=True)

                df_adm5 = pd.merge(df_nevite, df_adm4, on="address_complete_fr", how="left")

                df_adm6 = plmf(df_adm5)

                df_adm62 = df_adm6[
                    ["publication_number", "sequence", "type_party", "ad"]].drop_duplicates().reset_index(
                    drop=True)

                df_adm6_compe = df_adm62.groupby(["publication_number", "sequence", "type_party"]).nunique(
                    dropna=False).reset_index().rename(columns={"ad": "compte"})

                df_adm6_compe_u = df_adm6_compe.loc[df_adm6_compe["compte"] == 1].sort_values(
                    ["publication_number", "sequence"]).reset_index(drop=True)

                df_adm6_u = pd.merge(df_adm6, df_adm6_compe_u, on=["publication_number", "sequence", "type_party"],
                                     how="inner").drop(
                    columns=["compte", "ad",
                             "properties.postcode", "properties.city",
                             "properties.district"]).drop_duplicates().reset_index(drop=True)

                df_adm6_compe_m = df_adm6_compe.loc[df_adm6_compe["compte"] > 1].sort_values(
                    ["publication_number", "sequence"]).reset_index(drop=True)

                if len(df_adm6_compe_m) > 0:
                    df_adm6m = pd.merge(df_adm6, df_adm6_compe_m, on=["publication_number", "sequence", "type_party"],
                                        how="inner")

                    dico = {}

                    for _, r in df_adm6m.iterrows():
                        ids = r.id
                        pubn = r["publication_number"]
                        seq = r.sequence
                        pc = r["properties.postcode"]
                        city = r["properties.city"]
                        idn = str(pubn) + " " + str(seq)

                        if idn not in dico:
                            dico[idn] = {"ids": [], "city": []}

                        dico[idn]["ids"].append(ids)

                        if city not in dico[idn]["city"]:
                            dico[idn]["city"].append(city)

                    res = []
                    for ids in dico:
                        lng = len(dico[ids]["city"])
                        if lng == 1:
                            df_ids = pd.DataFrame(data={"ids": ids, "city": dico[ids]["city"]})
                            res.append(df_ids)

                    df_city = pd.concat(res).reset_index(drop=True)

                    df_adm63 = df_adm6[["publication_number", "sequence", "type_party", "properties.citycode",
                                        "properties.context", "ville",
                                        "address_complete_fr"]].drop_duplicates().reset_index(
                        drop=True)
                    df_adm64 = df_adm63.copy()
                    df_adm64["ids"] = df_adm64["publication_number"] + " " + df_adm64["sequence"].astype(str) + " " + \
                                      df_adm64[
                                          "type_party"]
                    df_adm64 = pd.merge(df_adm64, df_city, on="ids", how="inner").drop(
                        columns=["ids", "city"]).drop_duplicates().reset_index(
                        drop=True)

                    df_adm7 = pd.concat([df_adm6_u, df_adm64], ignore_index=True)

                if "df_adm7" in locals():
                    df_adm_final = pd.concat([df_adm7, df_evite], ignore_index=True)
                else:
                    df_adm_final = pd.concat([df_adm6_u, df_evite], ignore_index=True)

                df_adm_final = df_adm_final.fillna("")

                df_adm_final = df_adm_final.rename(columns={"properties.citycode": "com_code"})
                df_adm_final["liste_context"] = df_adm_final["properties.context"].str.split(", ")
                df_adm_final["dep_id"] = df_adm_final["liste_context"].apply(
                    lambda a: a[0] if isinstance(a, list) and len(a) == 3 else "")
                df_adm_final["dep_nom"] = df_adm_final["liste_context"].apply(
                    lambda a: a[1] if isinstance(a, list) and len(a) == 3 else "")
                df_adm_final["reg_nom"] = df_adm_final["liste_context"].apply(
                    lambda a: a[2] if isinstance(a, list) and len(a) == 3 else "")
                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                    lambda a: "0" + a if len(a) == 1 and a != "" else a)
                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                    lambda a: "0" + a if len(a) == 2 and a != "" else a)
                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                    lambda a: "D" + a if len(a) == 3 and a != "" else a)
                df_adm_final = df_adm_final.drop(columns=["properties.context", "liste_context"])
        else:
            df_adm_final = df_nevite.copy()
            df_adm_final["dep_id"] = ""
            df_adm_final["dep_nom"] = ""
            df_adm_final["reg_nom"] = ""
            df_adm_final["com_code"] = ""
            df_adm_final["ville"] = ""
            df_adm_final = df_adm_final.fillna("")
    else:
        df_adm_final = df_evite.copy()
        df_adm_final["dep_id"] = ""
        df_adm_final["dep_nom"] = ""
        df_adm_final["reg_nom"] = ""
        df_adm_final["com_code"] = ""
        df_adm_final["ville"] = ""
        df_adm_final = df_adm_final.fillna("")

    df_adm_final = df_adm_final[
        ["publication_number", "type_party", "sequence", "address_complete_fr", "com_code", "ville", "dep_id",
         "dep_nom", "reg_nom"]].drop_duplicates().reset_index(drop=True)

    return df_adm_final


@retry(tries=20, delay=5, backoff=5)
def ad_missing(miss_fr2: pd.DataFrame) -> pd.DataFrame:
    logger = get_logger(threading.current_thread().name)
    logger.info("start query missing address")
    miss2 = miss_fr2.loc[(miss_fr2["address_complete_fr"] == "") & (miss_fr2["address_source"] != "") & (
        miss_fr2["country_source"].isin(['FR', '  ']))]

    miss2["address_complete_fr2"] = miss2["address_source"].copy()

    miss2 = cleaning_address(miss2, "address_complete_fr2")

    miss2["len_ad"] = miss2["address_complete_fr2"].apply(lambda x: len(x))
    miss2 = miss2.loc[miss2["len_ad"] > 2]

    miss2 = miss2.drop(
        columns=["dep_id", "dep_nom", "reg_nom", "com_code", "ville", "address_complete_fr", "len_ad"])
    miss2 = miss2.rename(columns={"address_complete_fr2": "address_complete_fr"})

    miss2 = miss2.loc[~miss2["address_complete_fr"].isin(["boulogne",
                                                          "marly",
                                                          "marly",
                                                          "gentilly",
                                                          "gentilly",
                                                          "massy",
                                                          "levens",
                                                          "massy", "belfort",
                                                          "16 les hauts de floriane quartier beauregard fbras 83149",
                                                          "bonneville", "pierrefonds", "lyons", "valbonne",
                                                          "le neubourg",
                                                          "3 rue jeanjacques rousseau aussillon 81200 mazamet"])]

    lng = len(miss2)
    lng2 = len(miss2)

    res_adm = call_api_adresse(miss2)

    if len(res_adm) > 0:
        df_adm = pd.concat(res_adm, ignore_index=True)

        colonnes = ["feature", "geometry.type", "geometry.coordinates", "properties.id", "properties.banId",
                    "properties.name", "properties.x", "properties.y", "properties.type", "properties.importance",
                    "properties.housenumber", "properties.street"]
        for col in colonnes:
            if col in df_adm.columns:
                df_adm = df_adm.drop(columns=col)

        if "properties.label" in df_adm.columns:
            df_adm2 = df_adm.loc[~df_adm["properties.label"].isin(["1 Cote Guy de Maupassant 76380 Canteleu"])]
            df_adm2 = df_adm2.loc[df_adm2["properties.score"] >= 0.5]
            ad_max = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                "address_complete_fr").max().reset_index()
            df_adm2 = pd.merge(df_adm2, ad_max, on=["address_complete_fr", "properties.score"], how="inner")

            if len(df_adm2) == 0:
                df_adm_final = df_adm.copy()
                df_adm_final["dep_id"] = ""
                df_adm_final["dep_nom"] = ""
                df_adm_final["reg_nom"] = ""
                df_adm_final["com_code"] = ""
                df_adm_final["ville"] = ""
                df_adm_final = df_adm_final.fillna("")
            else:
                max_score = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                    "address_complete_fr").max().reset_index().drop_duplicates().reset_index(drop=True)

                df_adm3 = pd.merge(df_adm2, max_score, on=["address_complete_fr", "properties.score"],
                                   how="inner").sort_values(
                    "address_complete_fr").reset_index(drop=True)

                col_miss = ["properties.postcode", "properties.citycode", "properties.city",
                            "properties.context", "properties.district"]
                for col in col_miss:
                    if col not in df_adm3.columns:
                        df_adm3[col] = np.nan

                df_adm4 = df_adm3[
                    ["address_complete_fr", "properties.postcode", "properties.citycode", "properties.city",
                     "properties.context", "properties.district"]].drop_duplicates().reset_index(drop=True)

                df_adm5 = pd.merge(miss2, df_adm4, on="address_complete_fr", how="left")

                df_adm6 = plmf(df_adm5)

                df_adm62 = df_adm6[
                    ["address_complete_fr", "ad"]].drop_duplicates().reset_index(
                    drop=True)

                df_adm6_compe = df_adm62.groupby(["address_complete_fr"]).nunique(
                    dropna=False).reset_index().rename(columns={"ad": "compte"})

                df_adm6_compe_u = df_adm6_compe.loc[df_adm6_compe["compte"] == 1].reset_index(drop=True)

                df_adm6_u = pd.merge(df_adm6, df_adm6_compe_u, on="address_complete_fr",
                                     how="inner").drop(
                    columns=["compte", "ad",
                             "properties.postcode", "properties.city",
                             "properties.district"]).drop_duplicates().reset_index(drop=True)

                df_adm6_compe_m = df_adm6_compe.loc[df_adm6_compe["compte"] > 1].reset_index(drop=True)

                if len(df_adm6_compe_m) > 0:
                    df_adm6m = pd.merge(df_adm6, df_adm6_compe_m, on="address_complete_fr",
                                        how="inner")

                    dico = {}

                    for _, r in df_adm6m.iterrows():
                        ids = r["address_complete_fr"]
                        pc = r["properties.postcode"]
                        city = r["properties.city"]

                        if ids not in dico:
                            dico[ids] = {"ids": [], "city": []}

                        dico[ids]["ids"].append(ids)

                        if city not in dico[ids]["city"]:
                            dico[ids]["city"].append(city)

                    res = []
                    for ids in dico:
                        lng = len(dico[ids]["city"])
                        if lng == 1:
                            df_ids = pd.DataFrame(data={"ids": ids, "city": dico[ids]["city"]})
                            res.append(df_ids)

                    df_city = pd.concat(res).reset_index(drop=True)

                    df_adm63 = df_adm6[["address_complete_fr", "properties.citycode",
                                        "properties.context", "ville",
                                        "address_complete_fr"]].drop_duplicates().reset_index(
                        drop=True)
                    df_adm64 = df_adm63.copy()
                    df_adm64 = pd.merge(df_adm64, df_city, on="address_complete_fr", how="inner").drop(
                        columns="city").drop_duplicates().reset_index(
                        drop=True)

                    df_adm7 = pd.concat([df_adm6_u, df_adm64], ignore_index=True)

                if "df_adm7" in locals():
                    df_adm_final = df_adm7.copy()
                else:
                    df_adm_final = df_adm6_u

                df_adm_final = df_adm_final.fillna("")

                df_adm_final = df_adm_final.rename(columns={"properties.citycode": "com_code"})
                df_adm_final["liste_context"] = df_adm_final["properties.context"].str.split(", ")
                df_adm_final["dep_id"] = df_adm_final["liste_context"].apply(
                    lambda a: a[0] if isinstance(a, list) and len(a) == 3 else "")
                df_adm_final["dep_nom"] = df_adm_final["liste_context"].apply(
                    lambda a: a[1] if isinstance(a, list) and len(a) == 3 else "")
                df_adm_final["reg_nom"] = df_adm_final["liste_context"].apply(
                    lambda a: a[2] if isinstance(a, list) and len(a) == 3 else "")
                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                    lambda a: "0" + a if len(a) == 1 and a != "" else a)
                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                    lambda a: "0" + a if len(a) == 2 and a != "" else a)
                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                    lambda a: "D" + a if len(a) == 3 and a != "" else a)
                df_adm_final = df_adm_final.drop(columns=["properties.context", "liste_context"])
        else:
            df_adm_final = df_adm.copy()
            df_adm_final["dep_id"] = ""
            df_adm_final["dep_nom"] = ""
            df_adm_final["reg_nom"] = ""
            df_adm_final["com_code"] = ""
            df_adm_final["ville"] = ""
            df_adm_final = df_adm_final.fillna("")
    else:
        df_adm_final = miss2.copy()
        df_adm_final["dep_id"] = ""
        df_adm_final["dep_nom"] = ""
        df_adm_final["reg_nom"] = ""
        df_adm_final["com_code"] = ""
        df_adm_final["ville"] = ""
        df_adm_final = df_adm_final.fillna("")

    df_adm_final = df_adm_final[
        ["address_complete_fr", "com_code", "ville", "dep_id",
         "dep_nom", "reg_nom"]].drop_duplicates().reset_index(drop=True)

    miss4 = pd.merge(miss2, df_adm_final, on="address_complete_fr", how="inner")
    logger.info("end query missing address")

    return miss4


def subset_df(df: pd.DataFrame) -> dict:
    """
    This function divides the initial df into subsets which represent ca. 10 % of the original df.
    The subsets are put into a dictionary with 10-11 pairs key-value.
    Each key is the df subset name and each value is the df subset.
    """
    prct10 = int(round(len(df) / 10, 0))
    dict_nb = {}
    df = df.reset_index().drop(columns="index")
    indices = list(df.index)
    listes_indices = [indices[i:i + prct10] for i in range(0, len(indices), prct10)]
    i = 1
    for liste in listes_indices:
        min_ind = np.min(liste)
        max_ind = np.max(liste) + 1
        dict_nb["df" + str(i)] = df.iloc[min_ind: max_ind, :]
        i = i + 1

    return dict_nb


def res_futures(dict_nb: dict, query) -> pd.DataFrame:
    """
    This function applies the query function on each subset of the original df in a parallel way
    It takes a dictionary with 10-11 pairs key-value. Each key is the df subset name and each value is the df subset
    It returns a df with the IdRef.
    """
    global jointure
    res = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=101, thread_name_prefix="thread") as executor:
        # Start the load operations and mark each future with its URL
        future_to_req = {executor.submit(query, df): df for df in dict_nb.values()}
        for future in concurrent.futures.as_completed(future_to_req):
            req = future_to_req[future]
            try:
                data = future.result()
                res.append(data)
                jointure = pd.concat(res)
            except Exception as exc:
                print('%r generated an exception: %s' % (req, exc), flush=True)

    return jointure


def read_xml(df_fles):
    logger = get_logger(threading.current_thread().name)
    logger.info("start query INPI address")
    liste2 = []

    for fle in df_fles.itertuples():
        file = fle.fullpath
        print(file)
        with open(file, "r") as f:
            data = f.read()

        elem_file = file.split("/")

        if len(data) > 0:

            bs_data = BeautifulSoup(data, "xml")

            pn = bs_data.find("fr-patent-document")

            pub_n = doc_nb(elem_file, pn)

            appl = person_ref(bs_data, pub_n, pn)
            appl = appl.drop_duplicates().reset_index(drop=True)
            adresses = get_address(appl)
            liste2.append(adresses)


        else:
            pass

    ddresses = pd.concat(liste2)
    ddresses = ddresses.drop_duplicates().reset_index(drop=True)

    logger.info("end query INPI address")

    return ddresses


@retry(tries=10, delay=5, backoff=5)
def req_paysage(df_pay: pd.DataFrame) -> pd.DataFrame:
    logger = get_logger(threading.current_thread().name)
    logger.info("start query paysage")
    liste = []

    for r in df_pay.itertuples():
        pay = r.ids_paysage
        try:
            print(pay)
            res = requests.get(
                f"https://api.paysage.dataesr.ovh/structures/{pay}/localisations",
                headers={"Content-Type": "application/json", "X-API-KEY": os.getenv("PAYSAGE_API_KEY")}).json()
            res_data = res.get("data")
            if len(res_data) > 0:
                for item in res_data:
                    tmp = pd.json_normalize(item)
                    tmp["id_api"] = pay
                    liste.append(tmp)
            else:
                pass
        except:
            print(f"trouve pas {pay}")

    df_pay2 = pd.concat(liste)

    logger.info("end query paysage")

    return df_pay2


def get_communes() -> pd.DataFrame:
    url = f'https://docs.google.com/spreadsheet/ccc?key={os.getenv("GGSHT")}&output=xls'
    VARS = ['LES_COMMUNES']
    df_c = pd.read_excel(url, sheet_name=VARS, dtype=str, na_filter=False)
    communes = df_c.get('LES_COMMUNES')

    return communes


def create_paysage(pays: pd.DataFrame) -> pd.DataFrame:
    pays2 = pays.copy()
    pays2["virgule"] = pays2["id_paysage"].apply(lambda a: "virgule" if ",, " in a else "non")
    pays2["ids_paysage"] = pays["id_paysage"].str.split(",, ")
    pays2 = pays2.explode("ids_paysage")
    pays2.loc[pays2["ids_paysage"] == "N2X5f", "ids_paysage"] = "n2X5f"
    pays2.loc[pays2["ids_paysage"] == "XjdyB", "ids_paysage"] = "xJdyB"
    ids_paysage = list(pays2["ids_paysage"].unique())
    ids_paysage = [x for x in ids_paysage if x != ""]
    df_ids_paysage = pd.DataFrame(data={"ids_paysage": ids_paysage})

    sub_ids = subset_df(df_ids_paysage)
    df_paysage = res_futures(sub_ids, req_paysage)

    df_paysage = df_paysage.loc[df_paysage["country"] == "France"]

    df_paysage2 = df_paysage[["id_api", "cityId"]].drop_duplicates().reset_index(drop=True)
    compte_df_paysage = df_paysage2.groupby("id_api").nunique(dropna=False).reset_index()
    code_paysage = list(compte_df_paysage.loc[compte_df_paysage["cityId"] == 1, "id_api"].unique())
    code_paysagem = list(compte_df_paysage.loc[compte_df_paysage["cityId"] > 1, "id_api"].unique())
    df_paysagem = df_paysage.loc[df_paysage["id_api"].isin(code_paysagem)]
    df_paysagem2 = df_paysagem.loc[df_paysagem["current"], ["id_api", "cityId"]]

    df_paysage2 = df_paysage2.loc[df_paysage2["id_api"].isin(code_paysage)]
    df_paysage2 = pd.concat([df_paysage2, df_paysagem2], ignore_index=True)
    communes = get_communes()
    df_paysage2 = pd.merge(df_paysage2, communes, left_on="cityId", right_on="COM_CODE", how="left")

    paris = pd.DataFrame(data={"code": ['75101',
                                        '75102',
                                        '75103',
                                        '75104',
                                        '75105',
                                        '75106',
                                        '75107',
                                        '75108',
                                        '75109',
                                        '75110',
                                        '75111',
                                        '75112',
                                        '75113',
                                        '75114',
                                        '75115',
                                        '75116',
                                        '75117',
                                        '75118',
                                        '75119',
                                        '75120'],
                               "postcode": ['75001',
                                            '75002',
                                            '75003',
                                            '75004',
                                            '75005',
                                            '75006',
                                            '75007',
                                            '75008',
                                            '75009',
                                            '75010',
                                            '75011',
                                            '75012',
                                            '75013',
                                            '75014',
                                            '75015',
                                            '75116',
                                            '75017',
                                            '75018',
                                            '75019',
                                            '75020'],
                               "ville2": [f"Paris {i + 1}e arrondissement" for i in range(20)]})

    paris.loc[paris["code"] == "75101", "ville2"] = "Paris 1er arrondissement"

    paris16 = pd.DataFrame(data={"code": ['75116'],
                                 "postcode": ['75016'],
                                 "ville2": "Paris 16e arrondissement"})

    paris = pd.concat([paris, paris16], ignore_index=True)
    paris = paris.sort_values(by=['code', 'postcode'])

    lyon = pd.DataFrame(data={"code": [f'6938{i + 1}' for i in range(9)],
                              "postcode": [f'6900{i + 1}' for i in range(9)],
                              "ville2": [f"Lyon {i + 1}e arrondissement" for i in range(9)]})

    lyon.loc[lyon["code"] == "69381", "ville2"] = "Lyon 1er arrondissement"

    marseille = pd.DataFrame(data={"code": ['13201',
                                            '13202',
                                            '13203',
                                            '13204',
                                            '13205',
                                            '13206',
                                            '13207',
                                            '13208',
                                            '13209',
                                            '13210', '13211', '13212', '13213', '13214', '13215', '13216'],
                                   "postcode": ['13001',
                                                '13002',
                                                '13003',
                                                '13004',
                                                '13005',
                                                '13006',
                                                '13007',
                                                '13008',
                                                '13009',
                                                '13010', '13011', '13012', '13013', '13014', '13015',
                                                '13016'],
                                   "ville2": [f"Marseille {i + 1}e arrondissement" for i in range(16)]})

    marseille.loc[marseille["code"] == "13201", "ville2"] = "Marseille 1er arrondissement"

    plm = pd.concat([paris, lyon, marseille], ignore_index=True)

    df_paysage2 = pd.merge(df_paysage2, plm, left_on="COM_CODE", right_on="code", how="left")

    df_paysage2.loc[df_paysage2["ville2"].notna(), "COM_NOM"] = df_paysage2.loc[df_paysage2["ville2"].notna(), "ville2"]

    df_paysage3 = df_paysage2[
        ["id_api", "COM_CODE", "COM_NOM", "DEP_ID", "DEP_NOM", "REG_NOM"]].drop_duplicates().reset_index(drop=True)

    df_paysage3 = df_paysage3.rename(
        columns={"COM_CODE": "com_code", "COM_NOM": "ville", "DEP_ID": "dep_id", "DEP_NOM": "dep_nom",
                 "REG_NOM": "reg_nom"})

    pays3 = pd.merge(pays2.drop(columns=["com_code", "ville", "dep_id", "dep_nom", "reg_nom"]), df_paysage3,
                     left_on="ids_paysage", right_on="id_api", how="inner")

    oui = pays3.loc[
        pays3["virgule"] == "virgule", ["key_appln_nr_person", "id_paysage", "ids_paysage", "com_code", "ville",
                                        "dep_id", "dep_nom", "reg_nom"]].drop_duplicates().sort_values(
        "key_appln_nr_person").reset_index(drop=True)

    oui["liste"] = oui["id_paysage"].str.split(",, ")

    if len(oui) > 0:

        liste_persons = []

        persons = list(oui["key_appln_nr_person"].unique())

        for person in persons:
            temp = oui.loc[oui["key_appln_nr_person"] == person]
            idp = temp["id_paysage"].unique()[0]
            dico = {"key_appln_nr_person": person, "id_paysage": idp}

            com2 = []
            ville2 = []
            depid2 = []
            depn2 = []
            reg2 = []
            for item in temp["liste"].values[0]:
                if item == "":
                    com2.append("")
                    ville2.append("")
                    depid2.append("")
                    depn2.append("")
                    reg2.append("")
                elif item and item != "":
                    comv = temp.loc[temp["ids_paysage"] == item, "com_code"]
                    villev = temp.loc[temp["ids_paysage"] == item, "ville"]
                    depidv = temp.loc[temp["ids_paysage"] == item, "dep_id"]
                    depnv = temp.loc[temp["ids_paysage"] == item, "dep_nom"]
                    regv = temp.loc[temp["ids_paysage"] == item, "reg_nom"]

                    if len(comv) > 0:
                        com2.append(comv.values[0])
                    else:
                        com2.append("")

                    if len(villev) > 0:
                        ville2.append(villev.values[0])
                    else:
                        ville2.append("")

                    if len(depidv) > 0:
                        depid2.append(depidv.values[0])
                    else:
                        depid2.append("")

                    if len(depnv) > 0:
                        depn2.append(depnv.values[0])
                    else:
                        depn2.append("")

                    if len(regv) > 0:
                        reg2.append(regv.values[0])
                    else:
                        reg2.append("")
                else:
                    com2.append("")
                    ville2.append("")
                    depid2.append("")
                    depn2.append("")
                    reg2.append("")

            com = ",, ".join(com2)
            ville = ",, ".join(ville2)
            depid = ",, ".join(depid2)
            depn = ",, ".join(depn2)
            reg = ",, ".join(reg2)

            dico["com_code"] = com
            dico["ville"] = ville
            dico["dep_id"] = depid
            dico["dep_nom"] = depn
            dico["reg_nom"] = reg

            liste_persons.append(dico)

        df_virgule = pd.DataFrame(data=liste_persons)
        df_virgule2 = pd.merge(
            pays3.drop(
                columns=["ids_paysage", "id_api", "virgule", "com_code", "ville", "dep_id", "dep_nom", "reg_nom"]),
            df_virgule,
            on=["key_appln_nr_person", "id_paysage"], how="inner").drop_duplicates().reset_index(drop=True)

        pays4 = pays3.drop(columns=["ids_paysage", "id_api", "virgule"])
        pays4 = pays4.loc[
            ~pays4["key_appln_nr_person"].isin(df_virgule2["key_appln_nr_person"])].drop_duplicates().reset_index(
            drop=True)
        pays4 = pd.concat([pays4, df_virgule2], ignore_index=True)
    else:
        pays4 = pays3.drop(columns=["ids_paysage", "id_api", "virgule"])

    return pays4


def get_applicant(res_part1, pn_) -> pd.DataFrame:
    global df1
    if isinstance(res_part1, dict):
        if isinstance(res_part1["reg:applicant"], list):
            df1 = pd.json_normalize([res_part1], record_path=["reg:applicant"],
                                    meta=["@change-date", "@change-gazette-num"])
            df1 = df1.drop(
                columns=["@app-type", "@designation", "reg:nationality.reg:country", "reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df1["address"] = df1[col_address[0]]
                for i in range(1, len(col_address)):
                    df1["address"] = df1["address"] + " " + df1[col_address[i]]
                    df1["address"] = df1["address"].apply(lambda a: a.strip())

                for col in col_address:
                    name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                    df1 = df1.rename(columns={col: name})

                df1 = df1.rename(columns={"@sequence": "sequence",
                                          "reg:addressbook.@cdsid": "cdsid",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country"})
                df1["pn"] = pn_
            else:
                df1 = df1.rename(columns={"@sequence": "sequence",
                                          "reg:addressbook.@cdsid": "cdsid",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country"})
                df1["pn"] = pn_

        else:
            df1 = pd.json_normalize([res_part1])
            df1 = df1.drop(columns=["reg:applicant.@app-type",
                                    "reg:applicant.@designation",
                                    "reg:applicant.reg:nationality.reg:country",
                                    "reg:applicant.reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:applicant.reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df1["address"] = df1[col_address[0]]
                for i in range(1, len(col_address)):
                    df1["address"] = df1["address"] + " " + df1[col_address[i]]
                    df1["address"] = df1["address"].apply(lambda a: a.strip())

                # df1 = df1.drop(columns=col_address)

                for col in col_address:
                    name = re.sub("reg:applicant.reg:addressbook.reg:address.reg:", "", col)
                    df1 = df1.rename(columns={col: name})

                df1 = df1.rename(columns={"reg:applicant.@sequence": "sequence",
                                          "reg:applicant.reg:addressbook.@cdsid": "cdsid",
                                          "reg:applicant.reg:addressbook.reg:name": "name",
                                          "reg:applicant.reg:addressbook.reg:address.reg:country": "country"})
                df1["pn"] = pn_
            else:
                df1 = df1.rename(columns={"reg:applicant.@sequence": "sequence",
                                          "reg:applicant.reg:addressbook.@cdsid": "cdsid",
                                          "reg:applicant.reg:addressbook.reg:name": "name",
                                          "reg:applicant.reg:addressbook.reg:address.reg:country": "country"})
                df1["pn"] = pn_

    else:
        set_liste = [isinstance(item["reg:applicant"], list) for item in res_part1]
        set_liste = set(set_liste)
        if len(set_liste) == 1 and True in set_liste:
            df1 = pd.json_normalize(res_part1, record_path=["reg:applicant"],
                                    meta=["@change-date", "@change-gazette-num"])
            df1 = df1.drop(
                columns=["@app-type", "@designation", "reg:nationality.reg:country", "reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df1["address"] = df1[col_address[0]]
                for i in range(1, len(col_address)):
                    df1["address"] = df1["address"] + " " + df1[col_address[i]]
                    df1["address"] = df1["address"].apply(lambda a: a.strip())

                # df1 = df1.drop(columns=col_address)

                for col in col_address:
                    name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                    df1 = df1.rename(columns={col: name})

                df1 = df1.rename(columns={"@sequence": "sequence",
                                          "reg:addressbook.@cdsid": "cdsid",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country"})
                df1["pn"] = pn_
            else:
                df1 = df1.rename(columns={"@sequence": "sequence",
                                          "reg:addressbook.@cdsid": "cdsid",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country"})
                df1["pn"] = pn_

        elif len(set_liste) > 1 and True in set_liste:
            liste_part = []
            for item in res_part1:
                if isinstance(item["reg:applicant"], list):
                    tmp = pd.json_normalize(item, record_path=["reg:applicant"],
                                            meta=["@change-date", "@change-gazette-num"])
                    tmp = tmp.drop(
                        columns=["@app-type", "@designation", "reg:nationality.reg:country",
                                 "reg:residence.reg:country"])

                    tmp = tmp.fillna("")

                    col_address = []

                    for col in list(tmp.columns):
                        if col.startswith("reg:addressbook.reg:address.reg:address-"):
                            col_address.append(col)

                    if len(col_address) > 0:
                        tmp["address"] = tmp[col_address[0]]
                        for i in range(1, len(col_address)):
                            tmp["address"] = tmp["address"] + " " + tmp[col_address[i]]
                            tmp["address"] = tmp["address"].apply(lambda a: a.strip())

                        tmp = tmp.drop(
                            columns=col_address)

                        tmp = tmp.rename(columns={"@sequence": "sequence",
                                                  "reg:addressbook.@cdsid": "cdsid",
                                                  "reg:addressbook.reg:name": "name",
                                                  "reg:addressbook.reg:address.reg:country": "country"})
                        liste_part.append(tmp)
                    else:
                        tmp = tmp.rename(columns={"@sequence": "sequence",
                                                  "reg:addressbook.@cdsid": "cdsid",
                                                  "reg:addressbook.reg:name": "name",
                                                  "reg:addressbook.reg:address.reg:country": "country"})
                        liste_part.append(tmp)
                else:
                    tmp = pd.json_normalize(item)
                    tmp = tmp.drop(columns=["reg:applicant.@app-type",
                                            "reg:applicant.@designation",
                                            "reg:applicant.reg:nationality.reg:country",
                                            "reg:applicant.reg:residence.reg:country"])

                    tmp = tmp.fillna("")

                    col_address = []

                    for col in list(tmp.columns):
                        if col.startswith("reg:applicant.reg:addressbook.reg:address.reg:address-"):
                            col_address.append(col)

                    if len(col_address) > 0:
                        tmp["address"] = tmp[col_address[0]]
                        for i in range(1, len(col_address)):
                            tmp["address"] = tmp["address"] + " " + tmp[col_address[i]]
                            tmp["address"] = tmp["address"].apply(lambda a: a.strip())

                        # df1 = df1.drop(columns=col_address)

                        for col in col_address:
                            name = re.sub("reg:applicant.reg:addressbook.reg:address.reg:", "", col)
                            tmp = tmp.rename(columns={col: name})

                        # tmp = tmp.drop(
                        #     columns=col_address)

                        tmp = tmp.rename(columns={"reg:applicant.@sequence": "sequence",
                                                  "reg:applicant.reg:addressbook.@cdsid": "cdsid",
                                                  "reg:applicant.reg:addressbook.reg:name": "name",
                                                  "reg:applicant.reg:addressbook.reg:address.reg:country": "country"})
                        liste_part.append(tmp)
                    else:
                        tmp = tmp.rename(columns={"reg:applicant.@sequence": "sequence",
                                                  "reg:applicant.reg:addressbook.@cdsid": "cdsid",
                                                  "reg:applicant.reg:addressbook.reg:name": "name",
                                                  "reg:applicant.reg:addressbook.reg:address.reg:country": "country"})
                        liste_part.append(tmp)

            df1 = pd.concat(liste_part)
            df1["pn"] = pn_

        else:
            df1 = pd.json_normalize(res_part1)
            df1 = df1.drop(columns=["reg:applicant.@app-type",
                                    "reg:applicant.@designation",
                                    "reg:applicant.reg:nationality.reg:country",
                                    "reg:applicant.reg:residence.reg:country"])

            df1 = df1.fillna("")

            col_address = []

            for col in list(df1.columns):
                if col.startswith("reg:applicant.reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df1["address"] = df1[col_address[0]]
                for i in range(1, len(col_address)):
                    df1["address"] = df1["address"] + " " + df1[col_address[i]]
                    df1["address"] = df1["address"].apply(lambda a: a.strip())

                # df1 = df1.drop(columns=col_address)

                for col in col_address:
                    name = re.sub("reg:applicant.reg:addressbook.reg:address.reg:", "", col)
                    df1 = df1.rename(columns={col: name})

                df1 = df1.rename(columns={"reg:applicant.@sequence": "sequence",
                                          "reg:applicant.reg:addressbook.@cdsid": "cdsid",
                                          "reg:applicant.reg:addressbook.reg:name": "name",
                                          "reg:applicant.reg:addressbook.reg:address.reg:country": "country"})

                df1["pn"] = pn_
            else:
                df1 = df1.rename(columns={"reg:applicant.@sequence": "sequence",
                                          "reg:applicant.reg:addressbook.@cdsid": "cdsid",
                                          "reg:applicant.reg:addressbook.reg:name": "name",
                                          "reg:applicant.reg:addressbook.reg:address.reg:country": "country"})

                df1["pn"] = pn_

    return df1


def get_inventor(res_part1, pn_) -> pd.DataFrame:
    global df3
    if isinstance(res_part1, dict):
        if isinstance(res_part1["reg:inventor"], list):
            df3 = pd.json_normalize([res_part1], record_path=["reg:inventor"],
                                    meta=["@change-date", "@change-gazette-num"])

            df3 = df3.fillna("")

            col_address = []

            for col in list(df3.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df3["address"] = df3[col_address[0]]
                for i in range(1, len(col_address)):
                    df3["address"] = df3["address"] + " " + df3[col_address[i]]
                    df3["address"] = df3["address"].apply(lambda a: a.strip())

                for col in col_address:
                    name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                    df3 = df3.rename(columns={col: name})

                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})
                df3["pn"] = pn_
            else:
                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})
                df3["pn"] = pn_

        else:
            df3 = pd.json_normalize([res_part1])

            df3 = df3.fillna("")

            col_address = []

            for col in list(df3.columns):
                if col.startswith("reg:inventor.reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df3["address"] = df3[col_address[0]]
                for i in range(1, len(col_address)):
                    df3["address"] = df3["address"] + " " + df3[col_address[i]]
                    df3["address"] = df3["address"].apply(lambda a: a.strip())

                # df3 = df3.drop(columns=col_address)

                for col in col_address:
                    name = re.sub("reg:inventor.reg:addressbook.reg:address.reg:", "", col)
                    df3 = df3.rename(columns={col: name})

                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})
                df3["pn"] = pn_
            else:
                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})
                df3["pn"] = pn_

    else:
        set_liste = [isinstance(item["reg:inventor"], list) for item in res_part1]
        set_liste = set(set_liste)
        if len(set_liste) == 1 and True in set_liste:
            df3 = pd.json_normalize(res_part1, record_path=["reg:inventor"],
                                    meta=["@change-date", "@change-gazette-num"])

            df3 = df3.fillna("")

            col_address = []

            for col in list(df3.columns):
                if col.startswith("reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df3["address"] = df3[col_address[0]]
                for i in range(1, len(col_address)):
                    df3["address"] = df3["address"] + " " + df3[col_address[i]]
                    df3["address"] = df3["address"].apply(lambda a: a.strip())

                # df3 = df3.drop(columns=col_address)

                for col in col_address:
                    name = re.sub("reg:addressbook.reg:address.reg:", "", col)
                    df3 = df3.rename(columns={col: name})

                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})
                df3["pn"] = pn_
            else:
                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})
                df3["pn"] = pn_

        elif len(set_liste) > 1 and True in set_liste:
            liste_part = []
            for item in res_part1:
                if isinstance(item["reg:inventor"], list):
                    tmp = pd.json_normalize(item, record_path=["reg:inventor"],
                                            meta=["@change-date", "@change-gazette-num"])

                    tmp = tmp.fillna("")

                    col_address = []

                    for col in list(tmp.columns):
                        if col.startswith("reg:addressbook.reg:address.reg:address-"):
                            col_address.append(col)

                    tmp["address"] = tmp[col_address[0]]
                    for i in range(1, len(col_address)):
                        tmp["address"] = tmp["address"] + " " + tmp[col_address[i]]
                        tmp["address"] = tmp["address"].apply(lambda a: a.strip())

                    tmp = tmp.drop(
                        columns=col_address)

                    tmp = tmp.rename(columns={"reg:inventor.@sequence": "sequence",
                                              "reg:inventor.reg:addressbook.reg:name": "name",
                                              "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                              "reg:addressbook.reg:name": "name",
                                              "reg:addressbook.reg:address.reg:country": "country",
                                              "@sequence": "sequence"})
                    liste_part.append(tmp)
                else:
                    tmp = pd.json_normalize(item)

                    tmp = tmp.fillna("")

                    col_address = []

                    for col in list(tmp.columns):
                        if col.startswith("reg:inventor.reg:addressbook.reg:address.reg:address-"):
                            col_address.append(col)

                    if len(col_address) > 0:
                        tmp["address"] = tmp[col_address[0]]
                        for i in range(1, len(col_address)):
                            tmp["address"] = tmp["address"] + " " + tmp[col_address[i]]
                            tmp["address"] = tmp["address"].apply(lambda a: a.strip())

                        # df3 = df3.drop(columns=col_address)

                        for col in col_address:
                            name = re.sub("reg:inventor.reg:addressbook.reg:address.reg:", "", col)
                            df3 = df3.rename(columns={col: name})

                        # tmp = tmp.drop(
                        #     columns=col_address)

                        tmp = tmp.rename(columns={"reg:inventor.@sequence": "sequence",
                                                  "reg:inventor.reg:addressbook.reg:name": "name",
                                                  "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                                  "reg:addressbook.reg:name": "name",
                                                  "reg:addressbook.reg:address.reg:country": "country",
                                                  "@sequence": "sequence"})
                        liste_part.append(tmp)
                    else:
                        tmp = tmp.rename(columns={"reg:inventor.@sequence": "sequence",
                                                  "reg:inventor.reg:addressbook.reg:name": "name",
                                                  "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                                  "reg:addressbook.reg:name": "name",
                                                  "reg:addressbook.reg:address.reg:country": "country",
                                                  "@sequence": "sequence"})
                        liste_part.append(tmp)

                df3 = pd.concat(liste_part)
                df3["pn"] = pn_

        else:
            df3 = pd.json_normalize(res_part1)

            df3 = df3.fillna("")

            col_address = []

            for col in list(df3.columns):
                if col.startswith("reg:inventor.reg:addressbook.reg:address.reg:address-"):
                    col_address.append(col)

            if len(col_address) > 0:
                df3["address"] = df3[col_address[0]]
                for i in range(1, len(col_address)):
                    df3["address"] = df3["address"] + " " + df3[col_address[i]]
                    df3["address"] = df3["address"].apply(lambda a: a.strip())

                # df3 = df3.drop(columns=col_address)

                for col in col_address:
                    name = re.sub("reg:inventor.reg:addressbook.reg:address.reg:", "", col)
                    df3 = df3.rename(columns={col: name})

                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})

                df3["pn"] = pn_
            else:
                df3 = df3.rename(columns={"reg:inventor.@sequence": "sequence",
                                          "reg:inventor.reg:addressbook.reg:name": "name",
                                          "reg:inventor.reg:addressbook.reg:address.reg:country": "country",
                                          "reg:addressbook.reg:name": "name",
                                          "reg:addressbook.reg:address.reg:country": "country",
                                          "@sequence": "sequence"})

                df3["pn"] = pn_

    return df3


def recursive_items(dictionary):
    for key, value in dictionary.items():
        if isinstance(value, dict):
            yield (key, value)
            yield from recursive_items(value)
        else:
            yield (key, value)


def recursive_inventors(djson2):
    clefs = ["ops:world-patent-data", "ops:register-search", "reg:register-documents", "reg:register-document",
             "reg:bibliographic-data", "reg:parties", "reg:inventors"]

    if isinstance(djson2, list):
        for item in djson2:
            liste2 = []
            for key, value in recursive_items(item):
                if key in clefs:
                    liste2.append(key)
            for clef in liste2:
                item = item.get(clef)
                yield from recursive_items(item)

    return item


def df_applicant(rt, pn_) -> pd.DataFrame:
    """
    Create dataframe with applicants
    """
    data = xmltodict.parse(rt.content)
    djson = json.dumps(data)
    djson2 = json.loads(djson)

    global df1

    try:
        ret1 = \
            djson2["ops:world-patent-data"]["ops:register-search"]["reg:register-documents"]["reg:register-document"][
                "reg:bibliographic-data"]["reg:parties"]["reg:applicants"]
        df1 = get_applicant(ret1, pn_)
    except:
        liste = []
        clefs = ["ops:world-patent-data", "ops:register-search", "reg:register-documents", "reg:register-document",
                 "reg:bibliographic-data", "reg:parties", "reg:applicants"]
        for key, value in recursive_items(djson2):
            if key in clefs:
                liste.append(key)

        for item in liste:
            djson2 = djson2.get(item)

        if isinstance(djson2, list):
            for item in djson2:
                liste2 = []
                for key, value in recursive_items(item):
                    if key in clefs:
                        liste2.append(key)
                for clef in liste2:
                    item = item.get(clef)
                df1 = get_applicant(item, pn_)

    return df1


def df_inventor(rt, pn_) -> pd.DataFrame:
    """
    Create dataframe with applicants
    """
    data = xmltodict.parse(rt.content)
    djson = json.dumps(data)
    djson2 = json.loads(djson)

    global df2

    try:
        ret1 = \
            djson2["ops:world-patent-data"]["ops:register-search"]["reg:register-documents"]["reg:register-document"][
                "reg:bibliographic-data"]["reg:parties"]["reg:inventors"]
        df2 = get_inventor(ret1, pn_)
    except:
        liste = []
        clefs = ["ops:world-patent-data", "ops:register-search", "reg:register-documents", "reg:register-document",
                 "reg:bibliographic-data", "reg:parties", "reg:inventors"]
        for key, value in recursive_items(djson2):
            if key in clefs:
                liste.append(key)

        for item in liste:
            djson2 = djson2.get(item)

        if isinstance(djson2, list):
            for item in djson2:
                liste2 = []
                for key, value in recursive_items(item):
                    if key in clefs:
                        liste2.append(key)
                for clef in liste2:
                    item = item.get(clef)
                if isinstance(item, dict):
                    if "reg:inventor" in item.keys():
                        df2 = get_inventor(item, pn_)
                elif isinstance(item, list):
                    for it in item:
                        liste3 = []
                        for key, value in recursive_items(it):
                            if key in clefs:
                                liste3.append(key)
                        for clef in liste3:
                            it = it.get(clef)
                        if isinstance(it, dict):
                            if "reg:inventor" in it.keys():
                                df2 = get_inventor(it, pn_)
                else:
                    df2 = pd.DataFrame(data={"pn": [pn_]})

    return df2


@retry(tries=3, delay=5, backoff=5)
def get_token_oeb():
    """
    Get token OEB
    """
    token_url = 'https://ops.epo.org/3.2/auth/accesstoken'
    key = os.getenv("OPS_ADDRESS_PART")
    data = {'grant_type': 'client_credentials'}
    headers = {'Authorization': key, 'Content-Type': 'application/x-www-form-urlencoded'}

    r = requests.post(token_url, data=data, headers=headers)

    rs = r.content.decode()
    response = json.loads(rs)

    tken = f"Bearer {response.get('access_token')}"

    return tken


@retry(tries=3, delay=5, backoff=5)
def get_part(pn: str, tkn: str) -> pd.DataFrame:
    """
    Get applicants from EPO query with publication number
    """
    global d_app, d_inv
    ret1 = requests.get(f"http://ops.epo.org/3.2/rest-services/register/search/biblio?q=pn%3D{pn}",
                        headers={"Authorization": tkn})

    status = ret1.status_code
    if status != 200:
        if ret1.text == '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' \
                        '<fault xmlns="http://ops.epo.org">\n    <code>SERVER.EntityNotFound</code>\n' \
                        '    <message>No results found</message>\n</fault>\n':
            pass
        elif ret1.text == '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n' \
                          '<fault xmlns="http://ops.epo.org">\n' \
                          '<code>CLIENT.InvalidQuery</code>\n' \
                          '<message>The request was invalid</message>\n' \
                          '</fault>':
            pass
        else:
            print(f"Le code d'erreur est {status}.")
            pass
    else:
        print("URL successfully accessed", flush=True)
        d_app = df_applicant(ret1, pn)
        d_inv = df_inventor(ret1, pn)

    return d_app, d_inv


@retry(tries=20, delay=5, backoff=5)
def req_ops_oeb(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_logger(threading.current_thread().name)
    logger.info("start query OPS OEB")
    pubon = list(df.loc[df["appln_auth"].isin(["WO", "EP"]), "pn"].unique())
    lpart = []

    for pno in pubon:
        tkn = get_token_oeb()
        print(pno)
        try:
            appln_prio, inv_prio = get_part(pno, tkn)
            appln_prio["type_party"] = "applicant"
            inv_prio["type_party"] = "inventor"
            prt_ops = pd.concat([appln_prio, inv_prio], ignore_index=True)
            prt_ops = prt_ops.loc[prt_ops["sequence"] != ""]
            prt_ops["sequence"] = prt_ops["sequence"].astype(int)
            prt_ops2 = prt_ops.loc[prt_ops["country"] == "FR"].reset_index().drop(columns="index")
            if len(prt_ops2) > 0:
                if len(set(prt_ops2["@change-gazette-num"])) == 1:
                    prt_ops2 = prt_ops2[
                        ["sequence", "address", "pn",
                         "type_party"]].drop_duplicates().reset_index(drop=True)
                    prt_ops2["address_complete_fr"] = prt_ops2["address"].copy()
                    prt_ops2 = prt_ops2.drop(columns="address")
                    prt_ops2 = pd.merge(df, prt_ops2, on=["sequence", "pn",
                                                          "type_party"], how="inner")
                    prt_ops2 = cleaning_address(prt_ops2, "address_complete_fr")
                    prt_ops2 = prt_ops2.drop_duplicates().reset_index(drop=True)
                    lprt_ops3 = call_api_adresse(prt_ops2)
                    print(lprt_ops3)
                    if len(lprt_ops3) > 0:
                        df_adm = pd.concat(lprt_ops3, ignore_index=True)

                        colonnes = ["feature", "geometry.type", "geometry.coordinates", "properties.id",
                                    "properties.banId",
                                    "properties.name", "properties.x", "properties.y", "properties.type",
                                    "properties.importance",
                                    "properties.housenumber", "properties.street"]
                        for col in colonnes:
                            if col in df_adm.columns:
                                df_adm = df_adm.drop(columns=col)

                        if "properties.label" in df_adm.columns:
                            df_adm2 = df_adm.loc[
                                ~df_adm["properties.label"].isin(["1 Cote Guy de Maupassant 76380 Canteleu"])]
                            df_adm2 = df_adm2.loc[df_adm2["properties.score"] >= 0.5]
                            ad_max = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                                "address_complete_fr").max().reset_index()
                            df_adm2 = pd.merge(df_adm2, ad_max, on=["address_complete_fr", "properties.score"],
                                               how="inner")

                            if len(df_adm2) == 0:
                                df_adm_final = df_adm.copy()
                                df_adm_final["dep_id"] = ""
                                df_adm_final["dep_nom"] = ""
                                df_adm_final["reg_nom"] = ""
                                df_adm_final["com_code"] = ""
                                df_adm_final["ville"] = ""
                                df_adm_final = df_adm_final.fillna("")
                            else:
                                max_score = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                                    "address_complete_fr").max().reset_index().drop_duplicates().reset_index(drop=True)

                                df_adm3 = pd.merge(df_adm2, max_score, on=["address_complete_fr", "properties.score"],
                                                   how="inner").sort_values(
                                    "address_complete_fr").reset_index(drop=True)

                                col_miss = ["properties.postcode", "properties.citycode", "properties.city",
                                            "properties.context", "properties.district"]
                                for col in col_miss:
                                    if col not in df_adm3.columns:
                                        df_adm3[col] = np.nan

                                df_adm4 = df_adm3[
                                    ["address_complete_fr", "properties.postcode", "properties.citycode",
                                     "properties.city",
                                     "properties.context", "properties.district"]].drop_duplicates().reset_index(
                                    drop=True)

                                df_adm5 = pd.merge(prt_ops2, df_adm4, on="address_complete_fr", how="left")

                                df_adm6 = plmf(df_adm5)

                                df_adm62 = df_adm6[
                                    ["address_complete_fr", "ad"]].drop_duplicates().reset_index(
                                    drop=True)

                                df_adm6_compe = df_adm62.groupby(["address_complete_fr"]).nunique(
                                    dropna=False).reset_index().rename(columns={"ad": "compte"})

                                df_adm6_compe_u = df_adm6_compe.loc[df_adm6_compe["compte"] == 1].reset_index(drop=True)

                                df_adm6_u = pd.merge(df_adm6, df_adm6_compe_u, on="address_complete_fr",
                                                     how="inner").drop(
                                    columns=["compte", "ad",
                                             "properties.postcode", "properties.city",
                                             "properties.district"]).drop_duplicates().reset_index(drop=True)

                                df_adm6_compe_m = df_adm6_compe.loc[df_adm6_compe["compte"] > 1].reset_index(drop=True)

                                if len(df_adm6_compe_m) > 0:
                                    df_adm6m = pd.merge(df_adm6, df_adm6_compe_m, on="address_complete_fr",
                                                        how="inner")

                                    dico = {}

                                    for _, r in df_adm6m.iterrows():
                                        ids = r["address_complete_fr"]
                                        pc = r["properties.postcode"]
                                        city = r["properties.city"]

                                        if ids not in dico:
                                            dico[ids] = {"ids": [], "city": []}

                                        dico[ids]["ids"].append(ids)

                                        if city not in dico[ids]["city"]:
                                            dico[ids]["city"].append(city)

                                    res = []
                                    for ids in dico:
                                        lng = len(dico[ids]["city"])
                                        if lng == 1:
                                            df_ids = pd.DataFrame(data={"ids": ids, "city": dico[ids]["city"]})
                                            res.append(df_ids)

                                    df_city = pd.concat(res).reset_index(drop=True)

                                    df_adm63 = df_adm6[["address_complete_fr", "properties.citycode",
                                                        "properties.context", "ville",
                                                        "address_complete_fr"]].drop_duplicates().reset_index(
                                        drop=True)
                                    df_adm64 = df_adm63.copy()
                                    df_adm64 = pd.merge(df_adm64, df_city, on="address_complete_fr", how="inner").drop(
                                        columns="city").drop_duplicates().reset_index(
                                        drop=True)

                                    df_adm7 = pd.concat([df_adm6_u, df_adm64], ignore_index=True)

                                if "df_adm7" in locals():
                                    df_adm_final = df_adm7.copy()
                                else:
                                    df_adm_final = df_adm6_u

                                df_adm_final = df_adm_final.fillna("")

                                df_adm_final = df_adm_final.rename(columns={"properties.citycode": "com_code"})
                                df_adm_final["liste_context"] = df_adm_final["properties.context"].str.split(", ")
                                df_adm_final["dep_id"] = df_adm_final["liste_context"].apply(
                                    lambda a: a[0] if isinstance(a, list) and len(a) == 3 else "")
                                df_adm_final["dep_nom"] = df_adm_final["liste_context"].apply(
                                    lambda a: a[1] if isinstance(a, list) and len(a) == 3 else "")
                                df_adm_final["reg_nom"] = df_adm_final["liste_context"].apply(
                                    lambda a: a[2] if isinstance(a, list) and len(a) == 3 else "")
                                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                                    lambda a: "0" + a if len(a) == 1 and a != "" else a)
                                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                                    lambda a: "0" + a if len(a) == 2 and a != "" else a)
                                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                                    lambda a: "D" + a if len(a) == 3 and a != "" else a)
                                df_adm_final = df_adm_final.drop(columns=["properties.context", "liste_context"])
                        else:
                            df_adm_final = df_adm.copy()
                            df_adm_final["dep_id"] = ""
                            df_adm_final["dep_nom"] = ""
                            df_adm_final["reg_nom"] = ""
                            df_adm_final["com_code"] = ""
                            df_adm_final["ville"] = ""
                            df_adm_final = df_adm_final.fillna("")
                    else:
                        df_adm_final = prt_ops2.copy()
                        df_adm_final["dep_id"] = ""
                        df_adm_final["dep_nom"] = ""
                        df_adm_final["reg_nom"] = ""
                        df_adm_final["com_code"] = ""
                        df_adm_final["ville"] = ""
                        df_adm_final = df_adm_final.fillna("")

                    df_adm_final = df_adm_final[
                        ["address_complete_fr", "com_code", "ville", "dep_id",
                         "dep_nom", "reg_nom"]].drop_duplicates().reset_index(drop=True)

                    prt_ops3 = pd.merge(prt_ops2, df_adm_final, on="address_complete_fr", how="inner")
                    lpart.append(prt_ops3)
                else:
                    prt_ops2["@change-date2"] = prt_ops2["@change-date"].apply(pd.to_datetime)
                    mdate = prt_ops2["@change-date2"].min()
                    prt_ops2 = prt_ops2.loc[prt_ops2["@change-date2"] == mdate]
                    prt_ops2 = prt_ops2[
                        ["sequence", "address", "pn", "type_party"]].drop_duplicates().reset_index(drop=True)
                    prt_ops2["address_complete_fr"] = prt_ops2["address"].copy()
                    prt_ops2 = prt_ops2.drop(columns="address")
                    prt_ops2 = pd.merge(df, prt_ops2, on=["sequence", "pn",
                                                          "type_party"], how="inner")
                    prt_ops2 = cleaning_address(prt_ops2, "address_complete_fr")
                    prt_ops2 = prt_ops2.drop_duplicates().reset_index(drop=True)
                    lprt_ops3 = call_api_adresse(prt_ops2)
                    print(lprt_ops3)
                    if len(lprt_ops3) > 0:
                        df_adm = pd.concat(lprt_ops3, ignore_index=True)

                        colonnes = ["feature", "geometry.type", "geometry.coordinates", "properties.id",
                                    "properties.banId",
                                    "properties.name", "properties.x", "properties.y", "properties.type",
                                    "properties.importance",
                                    "properties.housenumber", "properties.street"]
                        for col in colonnes:
                            if col in df_adm.columns:
                                df_adm = df_adm.drop(columns=col)

                        if "properties.label" in df_adm.columns:
                            df_adm2 = df_adm.loc[
                                ~df_adm["properties.label"].isin(["1 Cote Guy de Maupassant 76380 Canteleu"])]
                            df_adm2 = df_adm2.loc[df_adm2["properties.score"] >= 0.5]
                            ad_max = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                                "address_complete_fr").max().reset_index()
                            df_adm2 = pd.merge(df_adm2, ad_max, on=["address_complete_fr", "properties.score"],
                                               how="inner")

                            if len(df_adm2) == 0:
                                df_adm_final = df_adm.copy()
                                df_adm_final["dep_id"] = ""
                                df_adm_final["dep_nom"] = ""
                                df_adm_final["reg_nom"] = ""
                                df_adm_final["com_code"] = ""
                                df_adm_final["ville"] = ""
                                df_adm_final = df_adm_final.fillna("")
                            else:
                                max_score = df_adm2[["address_complete_fr", "properties.score"]].groupby(
                                    "address_complete_fr").max().reset_index().drop_duplicates().reset_index(drop=True)

                                df_adm3 = pd.merge(df_adm2, max_score, on=["address_complete_fr", "properties.score"],
                                                   how="inner").sort_values(
                                    "address_complete_fr").reset_index(drop=True)

                                col_miss = ["properties.postcode", "properties.citycode", "properties.city",
                                            "properties.context", "properties.district"]
                                for col in col_miss:
                                    if col not in df_adm3.columns:
                                        df_adm3[col] = np.nan

                                df_adm4 = df_adm3[
                                    ["address_complete_fr", "properties.postcode", "properties.citycode",
                                     "properties.city",
                                     "properties.context", "properties.district"]].drop_duplicates().reset_index(
                                    drop=True)

                                df_adm5 = pd.merge(prt_ops2, df_adm4, on="address_complete_fr", how="left")

                                df_adm6 = plmf(df_adm5)

                                df_adm62 = df_adm6[
                                    ["address_complete_fr", "ad"]].drop_duplicates().reset_index(
                                    drop=True)

                                df_adm6_compe = df_adm62.groupby(["address_complete_fr"]).nunique(
                                    dropna=False).reset_index().rename(columns={"ad": "compte"})

                                df_adm6_compe_u = df_adm6_compe.loc[df_adm6_compe["compte"] == 1].reset_index(drop=True)

                                df_adm6_u = pd.merge(df_adm6, df_adm6_compe_u, on="address_complete_fr",
                                                     how="inner").drop(
                                    columns=["compte", "ad",
                                             "properties.postcode", "properties.city",
                                             "properties.district"]).drop_duplicates().reset_index(drop=True)

                                df_adm6_compe_m = df_adm6_compe.loc[df_adm6_compe["compte"] > 1].reset_index(drop=True)

                                if len(df_adm6_compe_m) > 0:
                                    df_adm6m = pd.merge(df_adm6, df_adm6_compe_m, on="address_complete_fr",
                                                        how="inner")

                                    dico = {}

                                    for _, r in df_adm6m.iterrows():
                                        ids = r["address_complete_fr"]
                                        pc = r["properties.postcode"]
                                        city = r["properties.city"]

                                        if ids not in dico:
                                            dico[ids] = {"ids": [], "city": []}

                                        dico[ids]["ids"].append(ids)

                                        if city not in dico[ids]["city"]:
                                            dico[ids]["city"].append(city)

                                    res = []
                                    for ids in dico:
                                        lng = len(dico[ids]["city"])
                                        if lng == 1:
                                            df_ids = pd.DataFrame(data={"ids": ids, "city": dico[ids]["city"]})
                                            res.append(df_ids)

                                    df_city = pd.concat(res).reset_index(drop=True)

                                    df_adm63 = df_adm6[["address_complete_fr", "properties.citycode",
                                                        "properties.context", "ville",
                                                        "address_complete_fr"]].drop_duplicates().reset_index(
                                        drop=True)
                                    df_adm64 = df_adm63.copy()
                                    df_adm64 = pd.merge(df_adm64, df_city, on="address_complete_fr", how="inner").drop(
                                        columns="city").drop_duplicates().reset_index(
                                        drop=True)

                                    df_adm7 = pd.concat([df_adm6_u, df_adm64], ignore_index=True)

                                if "df_adm7" in locals():
                                    df_adm_final = df_adm7.copy()
                                else:
                                    df_adm_final = df_adm6_u

                                df_adm_final = df_adm_final.fillna("")

                                df_adm_final = df_adm_final.rename(columns={"properties.citycode": "com_code"})
                                df_adm_final["liste_context"] = df_adm_final["properties.context"].str.split(", ")
                                df_adm_final["dep_id"] = df_adm_final["liste_context"].apply(
                                    lambda a: a[0] if isinstance(a, list) and len(a) == 3 else "")
                                df_adm_final["dep_nom"] = df_adm_final["liste_context"].apply(
                                    lambda a: a[1] if isinstance(a, list) and len(a) == 3 else "")
                                df_adm_final["reg_nom"] = df_adm_final["liste_context"].apply(
                                    lambda a: a[2] if isinstance(a, list) and len(a) == 3 else "")
                                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                                    lambda a: "0" + a if len(a) == 1 and a != "" else a)
                                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                                    lambda a: "0" + a if len(a) == 2 and a != "" else a)
                                df_adm_final["dep_id"] = df_adm_final["dep_id"].apply(
                                    lambda a: "D" + a if len(a) == 3 and a != "" else a)
                                df_adm_final = df_adm_final.drop(columns=["properties.context", "liste_context"])
                        else:
                            df_adm_final = df_adm.copy()
                            df_adm_final["dep_id"] = ""
                            df_adm_final["dep_nom"] = ""
                            df_adm_final["reg_nom"] = ""
                            df_adm_final["com_code"] = ""
                            df_adm_final["ville"] = ""
                            df_adm_final = df_adm_final.fillna("")
                    else:
                        df_adm_final = prt_ops2.copy()
                        df_adm_final["dep_id"] = ""
                        df_adm_final["dep_nom"] = ""
                        df_adm_final["reg_nom"] = ""
                        df_adm_final["com_code"] = ""
                        df_adm_final["ville"] = ""
                        df_adm_final = df_adm_final.fillna("")

                    df_adm_final = df_adm_final[
                        ["address_complete_fr", "com_code", "ville", "dep_id",
                         "dep_nom", "reg_nom"]].drop_duplicates().reset_index(drop=True)

                    prt_ops3 = pd.merge(prt_ops2, df_adm_final, on="address_complete_fr", how="inner")
                    lpart.append(prt_ops3)
        except:
            pass

    pt_ops2 = pd.concat(lpart, ignore_index=True)
    pt_ops2 = pt_ops2.drop_duplicates().reset_index(drop=True)

    logger.info("end query OPS OEB")

    return pt_ops2


def create_df_address():
    # set working directory
    os.chdir(DATA_PATH)

    files = os.listdir()

    patents = pd.read_csv("patent.csv", sep="|", encoding="utf-8",
                          engine="python", dtype=types.patent_types)

    csv = ""
    if "part_p08_address.csv" in files:
        csv = "part_p08_address.csv"
    else:
        csv = "part_p08.csv"

    part = pd.read_csv(csv, sep="|", encoding="utf-8",
                       engine="python", dtype=types.partfin_types, parse_dates=["earliest_filing_date"])

    if "appln_publn_number" in list(part.columns):
        part = part.drop(columns="appln_publn_number")

    if "part_p08_address.csv" in files:
        for col in list(part.columns):
            col2 = col.replace("-", "_")
            part = part.rename(columns={col: col2})
        key_presents = list(
            part.loc[(part["address_complete_fr"].notna()) & (part["com_code"].notna()), "key_appln_nr"].unique())
        pat_fr = patents.loc[(patents["appln_auth"] == "FR") & (~patents["key_appln_nr"].isin(key_presents))]

    else:
        pat_fr = patents.loc[patents["appln_auth"] == "FR"]

    part["liste_key_person"] = part["key_appln_nr_person"].str.split("_")
    part["applt_seq_nr"] = part["liste_key_person"].apply(lambda a: int(a[-2]))
    part["invt_seq_nr"] = part["liste_key_person"].apply(lambda a: int(a[-1]))
    part = part.drop(columns="liste_key_person")
    part["type_party"] = part.apply(
        lambda a: "applicant" if a["applt_seq_nr"] > 0 and a["invt_seq_nr"] == 0 else "inventor", axis=1)
    part["sequence"] = part.apply(
        lambda a: a["applt_seq_nr"] if a["type_party"] == "applicant" else a["invt_seq_nr"], axis=1)

    pat_pn = pat_fr[["key_appln_nr", "appln_publn_number"]].drop_duplicates().reset_index(drop=True)

    part = pd.merge(part, pat_pn, on="key_appln_nr", how="left")

    publication = list(pat_fr["appln_publn_number"].unique())

    list_dir = os.listdir(f"{DATA_PATH}INPI/")
    list_dir.sort()

    dico = {}
    dirfile = {"fullpath": []}
    for dir in list_dir:
        for dirpath, dirs, files in os.walk(f"{DATA_PATH}INPI/{dir}/", topdown=True):
            if "NEW" in dirpath and dirpath != f"{DATA_PATH}INPI/{dir}/":
                dico[dir] = files
                for item in files:
                    if item not in ["index.xml", "Volumeid"]:
                        flpath = dirpath + "/" + item
                        dirfile["fullpath"].append(flpath)

    df_files = pd.DataFrame(data=dirfile)
    df_files["file"] = df_files["fullpath"].str.split("/")
    df_files["pn"] = df_files["file"].apply(lambda a: [x.replace(".xml", "") for x in a if ".xml" in x][0])
    df_files = df_files.loc[df_files["pn"].isin(publication)]
    sub_files = subset_df(df_files)
    addresses = res_futures(sub_files, read_xml)

    addresses = addresses.drop_duplicates().reset_index(drop=True)

    if csv == "part_p08_address.csv":
        part_jointure = part.loc[part["address_complete_fr"].isna()]
        part_jointure2 = part.loc[(part["address_complete_fr"].notna()) & (part["com_code"].isna())]
        part_jointure_final = pd.concat([part_jointure, part_jointure2], ignore_index=True)
        part_jointure_final = part_jointure_final.drop(
            columns=["address_complete_fr", "com_code", "ville", "dep_id", "dep_nom", "reg_nom"]).reset_index(drop=True)
        addresses2 = pd.merge(part_jointure_final, addresses, left_on=["appln_publn_number", "type_party", "sequence"],
                              right_on=["publication_number", "type_party", "sequence"], how="left").drop(
            columns=["publication_number", "type_party", "sequence", "applt_seq_nr",
                     "invt_seq_nr"]).reset_index(drop=True)
        part_autres = part.loc[~part["key_appln_nr_person"].isin(part_jointure_final["key_appln_nr_person"])]
        addresses2 = pd.concat([addresses2, part_autres], ignore_index=True)
    else:
        addresses2 = pd.merge(part, addresses, left_on=["appln_publn_number", "type_party", "sequence"],
                              right_on=["publication_number", "type_party", "sequence"], how="left").drop(
            columns=["publication_number", "type_party", "sequence", "applt_seq_nr",
                     "invt_seq_nr"]).reset_index(drop=True)

    ad_siren = addresses2.loc[addresses2["siren"].notna(), ["docdb_family_id", "siren", "com_code",
                                                            "ville",
                                                            "dep_id",
                                                            "dep_nom",
                                                            "reg_nom"]].drop_duplicates().reset_index(
        drop=True)

    ad_siren = ad_siren.loc[ad_siren["com_code"].notna()].drop_duplicates().reset_index(drop=True)
    ad_siren = ad_siren.loc[ad_siren["com_code"] != ""].drop_duplicates().reset_index(drop=True)

    ad_siren_compte = ad_siren[["docdb_family_id", "siren", "com_code"]].groupby(["docdb_family_id", "siren"]).nunique(
        dropna=False).reset_index().rename(
        columns={"com_code": "compte"})

    com_codeu = ad_siren_compte.loc[ad_siren_compte["compte"] == 1].reset_index(drop=True)

    ad_sirenu = pd.merge(ad_siren, com_codeu, on=["docdb_family_id", "siren"], how="inner").drop(columns="compte")

    addresses_siren_pm = addresses2.loc[
        (addresses2["siren"].isin(com_codeu["siren"])) & (addresses2["type"] == "pm") & (
            addresses2["com_code"].isna())].drop(columns=["com_code",
                                                          "ville",
                                                          "dep_id",
                                                          "dep_nom",
                                                          "reg_nom"]).drop_duplicates().reset_index(drop=True)

    addresses_siren_pm = pd.merge(addresses_siren_pm, ad_sirenu, on=["docdb_family_id", "siren"], how="inner")

    reste_addresses = addresses2.loc[~addresses2["key_appln_nr_person"].isin(addresses_siren_pm["key_appln_nr_person"])]

    addresses3 = pd.concat([addresses_siren_pm, reste_addresses], ignore_index=True)

    fam_name = addresses3.loc[addresses3["com_code"].notna(), ["docdb_family_id", "name_corrected",
                                                               "com_code",
                                                               "ville",
                                                               "dep_id",
                                                               "dep_nom",
                                                               "reg_nom"]].drop_duplicates().reset_index(
        drop=True)

    fam_name_compte = fam_name[["docdb_family_id", "name_corrected", "com_code"]].groupby(
        ["docdb_family_id", "name_corrected"]).nunique(dropna=False).reset_index().rename(
        columns={"com_code": "compte"})

    com_nomu = fam_name_compte.loc[fam_name_compte["compte"] == 1]

    fam_nameu = pd.merge(fam_name, com_nomu, on=["docdb_family_id", "name_corrected"], how="inner")
    fam_nameu = fam_nameu.drop(columns=["compte"])

    addresses_fam_name = pd.merge(addresses3.loc[addresses3["com_code"].isna()].drop(columns=["com_code",
                                                                                              "ville",
                                                                                              "dep_id",
                                                                                              "dep_nom",
                                                                                              "reg_nom"]), fam_nameu,
                                  on=["docdb_family_id", "name_corrected"],
                                  how="inner").sort_values(
        ["docdb_family_id", "key_appln_nr_person"]).drop_duplicates().reset_index(drop=True)

    reste_addresses2 = addresses3.loc[
        ~addresses3["key_appln_nr_person"].isin(addresses_fam_name["key_appln_nr_person"])]

    addresses4 = pd.concat([addresses_fam_name, reste_addresses2], ignore_index=True)
    addresses4 = addresses4.fillna("")

    addresses4.to_csv("part_p08_address.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'part_p08_address.csv')

    missing_fr = addresses4.loc[(addresses4["country_corrected"].isin(["FR", ""]))]
    missing_fr2 = missing_fr.loc[missing_fr["com_code"] == ""]
    missing_fr2.to_csv("missing_fr_address.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'missing_fr_address.csv')

    pourcentage = round(len(missing_fr2) / len(missing_fr) * 100, 2)
    print(f"Le pourcentage d'adresses manquantes pour les Français est de {pourcentage}.", flush=True)

    sub_missing = subset_df(missing_fr2)
    missing2 = res_futures(sub_missing, ad_missing)

    addresses5 = addresses4.loc[~addresses4["key_appln_nr_person"].isin(missing2["key_appln_nr_person"])]
    addresses5 = pd.concat([addresses5, missing2], ignore_index=True)
    addresses5 = addresses5.drop(columns="appln_publn_number")

    ad_siren = addresses5.loc[addresses5["siren"] != "", ["docdb_family_id", "siren", "com_code",
                                                          "ville",
                                                          "dep_id",
                                                          "dep_nom",
                                                          "reg_nom"]].drop_duplicates().reset_index(
        drop=True)

    ad_siren = ad_siren.loc[ad_siren["com_code"] != ""].drop_duplicates().reset_index(drop=True)
    ad_siren = ad_siren.loc[ad_siren["com_code"] != ""].drop_duplicates().reset_index(drop=True)

    ad_siren_compte = ad_siren[["docdb_family_id", "siren", "com_code"]].groupby(["docdb_family_id", "siren"]).nunique(
        dropna=False).reset_index().rename(
        columns={"com_code": "compte"})

    com_codeu = ad_siren_compte.loc[ad_siren_compte["compte"] == 1].reset_index(drop=True)

    ad_sirenu = pd.merge(ad_siren, com_codeu, on=["docdb_family_id", "siren"], how="inner").drop(columns="compte")

    addresses_siren_pm = addresses5.loc[
        (addresses5["siren"].isin(com_codeu["siren"])) & (addresses5["type"] == "pm") & (
                addresses5["com_code"] == "")].drop(columns=["com_code",
                                                             "ville",
                                                             "dep_id",
                                                             "dep_nom",
                                                             "reg_nom"]).drop_duplicates().reset_index(drop=True)

    addresses_siren_pm = pd.merge(addresses_siren_pm, ad_sirenu, on=["docdb_family_id", "siren"], how="inner")

    reste_addresses = addresses5.loc[~addresses5["key_appln_nr_person"].isin(addresses_siren_pm["key_appln_nr_person"])]

    addresses6 = pd.concat([addresses_siren_pm, reste_addresses], ignore_index=True)

    fam_name = addresses6.loc[addresses6["com_code"] != "", ["docdb_family_id", "name_corrected",
                                                             "com_code",
                                                             "ville",
                                                             "dep_id",
                                                             "dep_nom",
                                                             "reg_nom"]].drop_duplicates().reset_index(
        drop=True)

    fam_name_compte = fam_name[["docdb_family_id", "name_corrected", "com_code"]].groupby(
        ["docdb_family_id", "name_corrected"]).nunique(dropna=False).reset_index().rename(
        columns={"com_code": "compte"})

    com_nomu = fam_name_compte.loc[fam_name_compte["compte"] == 1]

    fam_nameu = pd.merge(fam_name, com_nomu, on=["docdb_family_id", "name_corrected"], how="inner")
    fam_nameu = fam_nameu.drop(columns=["compte"])

    addresses_fam_name = pd.merge(addresses6.loc[addresses6["com_code"] == ""].drop(columns=["com_code",
                                                                                             "ville",
                                                                                             "dep_id",
                                                                                             "dep_nom",
                                                                                             "reg_nom"]), fam_nameu,
                                  on=["docdb_family_id", "name_corrected"],
                                  how="inner").sort_values(
        ["docdb_family_id", "key_appln_nr_person"]).drop_duplicates().reset_index(drop=True)

    reste_addresses5 = addresses6.loc[
        ~addresses6["key_appln_nr_person"].isin(addresses_fam_name["key_appln_nr_person"])]

    addresses7 = pd.concat([addresses_fam_name, reste_addresses5], ignore_index=True)
    addresses7 = addresses7.fillna("")

    addresses7["id_paysage"] = addresses7["id_paysage"].str.replace("N2X5f", "n2X5f", regex=False)
    addresses7["id_paysage"] = addresses7["id_paysage"].str.replace("XjdyB", "xJdyB", regex=False)

    paysage = addresses7.loc[
        (addresses7["country_corrected"].isin(["FR", ""])) & (addresses7["com_code"] == "") & (
                    addresses7["id_paysage"] != "")]

    paysage2 = create_paysage(paysage)

    addresses8 = addresses7.loc[~addresses7["key_appln_nr_person"].isin(paysage2["key_appln_nr_person"])]
    addresses8 = pd.concat([addresses8, paysage2], ignore_index=True)

    addresses8 = addresses8.fillna("")

    addresses8.to_csv("part_p08_address2.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'part_p08_address2.csv')

    missing_fr5 = addresses8.loc[(addresses8["country_corrected"].isin(["FR", ""]))]
    missing_fr6 = missing_fr5.loc[missing_fr5["com_code"] == ""]

    pourcentage2 = round(len(missing_fr6) / len(missing_fr5) * 100, 2)
    print(f"Le 2ème pourcentage d'adresses manquantes pour les Français est de {pourcentage2}.", flush=True)

    missing_oeb = patents.loc[patents["key_appln_nr"].isin(missing_fr6["key_appln_nr"])]

    missing_fr6["liste_key_person"] = missing_fr6["key_appln_nr_person"].str.split("_")
    missing_fr6["applt_seq_nr"] = missing_fr6["liste_key_person"].apply(lambda a: int(a[-2]))
    missing_fr6["invt_seq_nr"] = missing_fr6["liste_key_person"].apply(lambda a: int(a[-1]))
    missing_fr6 = missing_fr6.drop(columns="liste_key_person")
    missing_fr6["type_party"] = missing_fr6.apply(
        lambda a: "applicant" if a["applt_seq_nr"] > 0 and a["invt_seq_nr"] == 0 else "inventor", axis=1)
    missing_fr6["sequence"] = missing_fr6.apply(
        lambda a: a["applt_seq_nr"] if a["type_party"] == "applicant" else a["invt_seq_nr"], axis=1)

    pat_oeb = missing_oeb[["key_appln_nr", "appln_auth", "appln_publn_number"]].drop_duplicates().reset_index(drop=True)
    pat_oeb = pat_oeb.loc[pat_oeb["appln_auth"].isin(["EP", "WO"])]
    pat_oeb["pn"] = pat_oeb["appln_auth"] + pat_oeb["appln_publn_number"]
    pat_oeb = pat_oeb.drop(columns=["appln_auth", "appln_publn_number"])

    missing_fr6 = pd.merge(missing_fr6, pat_oeb, on="key_appln_nr", how="left")

    pubon = list(pat_oeb["pn"].unique())
    print(f"Nombre de numéros de publication à chercher : {len(pubon)}.", flush=True)

    part_ops = req_ops_oeb(missing_fr6)

    addresses9 = addresses8.loc[~addresses8["key_appln_nr_person"].isin(part_ops["key_appln_nr_person"])]
    addresses9 = pd.concat([addresses9, part_ops], ignore_index=True)

    ad_siren = addresses9.loc[addresses9["siren"].notna(), ["docdb_family_id", "siren", "com_code",
                                                            "ville",
                                                            "dep_id",
                                                            "dep_nom",
                                                            "reg_nom"]].drop_duplicates().reset_index(
        drop=True)

    ad_siren = ad_siren.loc[ad_siren["com_code"].notna()].drop_duplicates().reset_index(drop=True)
    ad_siren = ad_siren.loc[ad_siren["com_code"] != ""].drop_duplicates().reset_index(drop=True)

    ad_siren_compte = ad_siren[["docdb_family_id", "siren", "com_code"]].groupby(["docdb_family_id", "siren"]).nunique(
        dropna=False).reset_index().rename(
        columns={"com_code": "compte"})

    com_codeu = ad_siren_compte.loc[ad_siren_compte["compte"] == 1].reset_index(drop=True)

    ad_sirenu = pd.merge(ad_siren, com_codeu, on=["docdb_family_id", "siren"], how="inner").drop(columns="compte")

    addresses_siren_pm = addresses9.loc[
        (addresses9["siren"].isin(com_codeu["siren"])) & (addresses9["type"] == "pm") & (
            addresses9["com_code"].isna())].drop(columns=["com_code",
                                                          "ville",
                                                          "dep_id",
                                                          "dep_nom",
                                                          "reg_nom"]).drop_duplicates().reset_index(drop=True)

    addresses_siren_pm = pd.merge(addresses_siren_pm, ad_sirenu, on=["docdb_family_id", "siren"], how="inner")

    reste_addresses = addresses9.loc[~addresses9["key_appln_nr_person"].isin(addresses_siren_pm["key_appln_nr_person"])]

    addresses10 = pd.concat([addresses_siren_pm, reste_addresses], ignore_index=True)

    fam_name = addresses10.loc[addresses10["com_code"].notna(), ["docdb_family_id", "name_corrected",
                                                                 "com_code",
                                                                 "ville",
                                                                 "dep_id",
                                                                 "dep_nom",
                                                                 "reg_nom"]].drop_duplicates().reset_index(
        drop=True)

    fam_name_compte = fam_name[["docdb_family_id", "name_corrected", "com_code"]].groupby(
        ["docdb_family_id", "name_corrected"]).nunique(dropna=False).reset_index().rename(
        columns={"com_code": "compte"})

    com_nomu = fam_name_compte.loc[fam_name_compte["compte"] == 1]

    fam_nameu = pd.merge(fam_name, com_nomu, on=["docdb_family_id", "name_corrected"], how="inner")
    fam_nameu = fam_nameu.drop(columns=["compte"])

    addresses_fam_name = pd.merge(addresses10.loc[addresses10["com_code"].isna()].drop(columns=["com_code",
                                                                                                "ville",
                                                                                                "dep_id",
                                                                                                "dep_nom",
                                                                                                "reg_nom"]), fam_nameu,
                                  on=["docdb_family_id", "name_corrected"],
                                  how="inner").sort_values(
        ["docdb_family_id", "key_appln_nr_person"]).drop_duplicates().reset_index(drop=True)

    reste_addresses2 = addresses10.loc[
        ~addresses10["key_appln_nr_person"].isin(addresses_fam_name["key_appln_nr_person"])]

    addresses11 = pd.concat([addresses_fam_name, reste_addresses2], ignore_index=True)
    addresses11 = addresses11.fillna("")

    communes = get_communes()

    communes2 = communes[["COM_CODE", "DEP_ID", "DEP_NOM", "REG_NOM"]].drop_duplicates().reset_index(drop=True)

    addresses11 = pd.merge(addresses11, communes2, left_on="com_code", right_on="COM_CODE", how="left")

    addresses11.loc[(addresses11["com_code"] != "") & (addresses11["dep_id"] == "") & (
        addresses11["DEP_ID"].notan()), "dep_id"] = addresses11.loc[
        (addresses11["com_code"] != "") & (addresses11["dep_id"] == "") & (addresses11["DEP_ID"].notan()), "DEP_ID"]

    addresses11.loc[(addresses11["com_code"] != "") & (addresses11["dep_nom"] == "") & (
        addresses11["DEP_NOM"].notan()), "dep_nom"] = addresses11.loc[
        (addresses11["com_code"] != "") & (addresses11["dep_nom"] == "") & (addresses11["DEP_NOM"].notan()), "DEP_NOM"]

    addresses11.loc[(addresses11["com_code"] != "") & (addresses11["reg_nom"] == "") & (
        addresses11["REG_NOM"].notan()), "reg_nom"] = addresses11.loc[
        (addresses11["com_code"] != "") & (addresses11["reg_nom"] == "") & (addresses11["REG_NOM"].notan()), "REG_NOM"]

    addresses11 = addresses11.drop(columns=["COM_CODE", "DEP_ID", "DEP_NOM", "REG_NOM"])

    addresses11.to_csv("part_p08_address3.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'part_p08_address3.csv')

    missing_fr7 = addresses11.loc[(addresses11["country_corrected"].isin(["FR", ""]))]
    missing_fr8 = missing_fr7.loc[missing_fr7["com_code"] == ""]

    pourcentage3 = round(len(missing_fr8) / len(missing_fr7) * 100, 2)
    print(f"Le 3ème pourcentage d'adresses manquantes pour les Français est de {pourcentage3}.", flush=True)
    missing_fr8.to_csv("missing_fr_address3.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'missing_fr_address3.csv')