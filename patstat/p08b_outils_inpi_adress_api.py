#!/usr/bin/env python
# coding: utf-8

import os
import re
import concurrent.futures

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


@retry(tries=3, delay=5, backoff=5)
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

    df_dico2.loc[df_dico2["address_complete_fr"] == "F-75014 PARIS", "address_complete_fr"] = "75014 PARIS"
    df_dico2.loc[df_dico2["address_complete_fr"] == "F-75015 PARIS", "address_complete_fr"] = "75015 PARIS"
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"\"", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"«", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"»", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"\'\'", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"\ufeff3", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"^-+\s?", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"^\.+\s?", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"^\/+\s?", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"&", " ", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r",", " ", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"\_{1,}", " ", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"[c|C]\/[o|O]", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"\n", " ", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.replace(r"\s{2,}", "", regex=True)
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].apply(
        lambda a: "".join(c for c in a if c.isalnum() or c == " "))
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.strip()
    df_dico2["address_complete_fr"] = df_dico2["address_complete_fr"].str.lower()

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

    res_adm = []
    for _, r in df_nevite.iterrows():
        ad = r["address_complete_fr"]
        res = requests.get(f"https://api_adresse.data.gouv.fr/search/?q={ad}&format=json")
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

                df_adm5.loc[df_adm5["properties.district"].notna(), "ville"] = df_adm5.loc[
                    df_adm5["properties.district"].notna(), "properties.district"]
                df_adm5.loc[df_adm5["properties.district"].isna(), "ville"] = df_adm5.loc[
                    df_adm5["properties.district"].isna(), "properties.city"]

                df_adm5["properties.postcode"] = df_adm5["properties.postcode"].astype(object)

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

                df_adm6 = pd.merge(df_adm5, plm, on="properties.postcode", how="left")

                df_adm6.loc[
                    (df_adm6["code"].notna()) & (
                            df_adm6["code"] != df_adm6["properties.citycode"]), "properties.citycode"] = \
                    df_adm6.loc[(df_adm6["code"].notna()) & (df_adm6["code"] != df_adm6["properties.citycode"]), "code"]

                df_adm6.loc[(df_adm6["code"].notna()) & (df_adm6["ville"] != df_adm6["ville2"]), "ville"] = \
                    df_adm6.loc[(df_adm6["code"].notna()) & (df_adm6["ville"] != df_adm6["ville2"]), "ville2"]

                df_adm6["ad"] = df_adm6["properties.postcode"] + " " + df_adm6["properties.citycode"] + " " + df_adm6[
                    "ville"] + " " + df_adm6["properties.context"]

                df_adm6 = df_adm6.drop(columns=["code", "ville2"])

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


@retry(tries=3, delay=5, backoff=5)
def ad_missing(miss_fr2: pd.DataFrame) -> pd.DataFrame:
    logger = get_logger(threading.current_thread().name)
    logger.info("start query missing address")
    miss2 = miss_fr2.loc[(miss_fr2["address_complete_fr"] == "") & (miss_fr2["address_source"] != "") & (
        miss_fr2["country_source"].isin(['FR', '  ']))]

    miss2["address_complete_fr2"] = miss2["address_source"].copy()

    miss2.loc[miss2["address_complete_fr2"] == "F-75014 PARIS", "address_complete_fr2"] = "75014 PARIS"
    miss2.loc[miss2["address_complete_fr2"] == "F-75015 PARIS", "address_complete_fr2"] = "75015 PARIS"
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"\"", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"«", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"»", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"\'\'", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"\ufeff3", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"^-+\s?", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"^\.+\s?", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"^\/+\s?", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"&", " ", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r",", " ", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"\_{1,}", " ", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"[c|C]\/[o|O]", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"\n", " ", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.replace(r"\s{2,}", "", regex=True)
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].apply(
        lambda a: "".join(c for c in a if c.isalnum() or c == " "))
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.strip()
    miss2["address_complete_fr2"] = miss2["address_complete_fr2"].str.lower()
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

    res_adm = []

    start = timer()
    for _, r in miss2.iterrows():
        prct = lng / lng2 * 100
        if prct % 10 == 0:
            print(f"Il reste {prct} % de requêtes à effectuer")
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
    print(f"Durée de traitement : {time_str}")

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

                df_adm5.loc[df_adm5["properties.district"].notna(), "ville"] = df_adm5.loc[
                    df_adm5["properties.district"].notna(), "properties.district"]
                df_adm5.loc[df_adm5["properties.district"].isna(), "ville"] = df_adm5.loc[
                    df_adm5["properties.district"].isna(), "properties.city"]

                df_adm5["properties.postcode"] = df_adm5["properties.postcode"].astype(object)

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

                df_adm6 = pd.merge(df_adm5, plm, on="properties.postcode", how="left")

                df_adm6.loc[
                    (df_adm6["code"].notna()) & (
                            df_adm6["code"] != df_adm6["properties.citycode"]), "properties.citycode"] = \
                    df_adm6.loc[(df_adm6["code"].notna()) & (df_adm6["code"] != df_adm6["properties.citycode"]), "code"]

                df_adm6.loc[(df_adm6["code"].notna()) & (df_adm6["ville"] != df_adm6["ville2"]), "ville"] = \
                    df_adm6.loc[(df_adm6["code"].notna()) & (df_adm6["ville"] != df_adm6["ville2"]), "ville2"]

                df_adm6["ad"] = df_adm6["properties.postcode"] + " " + df_adm6["properties.citycode"] + " " + df_adm6[
                    "ville"] + " " + df_adm6["properties.context"]

                df_adm6 = df_adm6.drop(columns=["code", "ville2"])

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


@retry(tries=3, delay=5, backoff=5)
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

    return pays4


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
        (part["country_corrected"].isin(["FR", ""])) & (part["com-code"]=="") & (part["id_paysage"]!="")]

    paysage2 = create_paysage(paysage)

    addresses8 = addresses7.loc[~addresses7["key_appln_nr_person"].isin(paysage2["key_appln_nr_person"])]
    addresses8 = pd.concat([addresses8, paysage2], ignore_index=True)

    addresses8 = addresses8.fillna("")

    addresses8.to_csv("part_p08_address2.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'part_p08_address2.csv')

    missing_fr5 = addresses8.loc[(addresses8["country_corrected"].isin(["FR", ""]))]
    missing_fr6 = missing_fr5.loc[missing_fr5["com_code"] == ""]

    missing_fr6.to_csv("missing_fr_address2.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'missing_fr_address2.csv')

    pourcentage2 = round(len(missing_fr6) / len(missing_fr5) * 100, 2)
    print(f"Le 2ème pourcentage d'adresses manquantes pour les Français est de {pourcentage2}.", flush=True)



