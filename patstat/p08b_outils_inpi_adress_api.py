#!/usr/bin/env python
# coding: utf-8

import os
import re

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import requests
from patstat import dtypes_patstat_declaration as types
from utils import swift

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')


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
            lg_apps = len(apps)
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

    return df


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

    dico = {'publication-number': [], 'address-1': [], 'address-2': [], 'address-3': [], 'mailcode': [],
            'pobox': [], 'room': [], 'address-floor': [], 'building': [], 'street': [], 'city': [],
            'county': [], 'state': [], 'postcode': [], 'country': [], 'sequence': [], 'type-party': [],
            'data-format-person': [], 'prefix': [], 'first-name': [], 'middle-name': [],
            'last-name': [], 'orgname': [], 'name-complete': [], 'suffix': [], 'siren': [], 'role': [],
            'department': [], 'synonym': [], 'designation': [], 'application-number-fr': [],
            'city2': [], 'postcode2': [], 'cedex': [], 'address-complete-fr': []}

    for _, r in df_per.iterrows():
        if r["country"] in ["XX", "FR", ""]:
            nom = ""
            if r["address-1"] != "":
                nom = nom + r["address-1"]
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

        dico['publication-number'].append(r["publication-number"])
        dico['address-1'].append(r["address-1"])
        dico['address-2'].append(r["address-2"])
        dico['address-3'].append(r["address-3"])
        dico['mailcode'].append(r["mailcode"])
        dico['pobox'].append(r["pobox"])
        dico['room'].append(r["room"])
        dico['address-floor'].append(r["address-floor"])
        dico['building'].append(r["building"])
        dico['street'].append(r["street"])
        dico['city'].append(r["city"])
        dico['county'].append(r["county"])
        dico['state'].append(r["state"])
        dico['postcode'].append(r["postcode"])
        dico['country'].append(r["country"])
        dico['sequence'].append(r["sequence"])
        dico['type-party'].append(r["type-party"])
        dico['data-format-person'].append(r["data-format-person"])
        dico['prefix'].append(r["prefix"])
        dico['first-name'].append(r["first-name"])
        dico['middle-name'].append(r["middle-name"])
        dico['last-name'].append(r["last-name"])
        dico['orgname'].append(r["orgname"])
        dico['name-complete'].append(r["name-complete"])
        dico['suffix'].append(r["suffix"])
        dico['siren'].append(r["siren"])
        dico['role'].append(r["role"])
        dico['department'].append(r["department"])
        dico['synonym'].append(r["synonym"])
        dico['designation'].append(r["designation"])
        dico['application-number-fr'].append(r["application-number-fr"])
        dico['city2'].append(r["city2"])
        dico['postcode2'].append(r["postcode2"])
        dico['cedex'].append(r["cedex"])
        dico['address-complete-fr'].append(nom)

    df_dico2 = pd.DataFrame(dico)

    df_dico2.loc[df_dico2["address-complete-fr"] == "F-75014 PARIS", "address-complete-fr"] = "75014 PARIS"
    df_dico2.loc[df_dico2["address-complete-fr"] == "F-75015 PARIS", "address-complete-fr"] = "75015 PARIS"
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"\"", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"«", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"»", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"\'\'", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"\ufeff3", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"^-+\s?", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"^\.+\s?", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"^\/+\s?", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"\n", " ", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.replace(r"\s{2,}", "", regex=True)
    df_dico2["address-complete-fr"] = df_dico2["address-complete-fr"].str.strip()

    df_dico2 = df_dico2[['publication-number', 'address-1', 'address-2', 'address-3', 'mailcode',
                         'pobox', 'room', 'address-floor', 'building', 'street', 'city',
                         'county', 'state', 'postcode', 'country', 'sequence', 'type-party',
                         'data-format-person', 'prefix', 'first-name', 'middle-name',
                         'last-name', 'orgname', 'name-complete', 'suffix', 'siren', 'role',
                         'department', 'synonym', 'designation', 'application-number-fr', 'address-complete-fr']]

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

    df_evite = df_dico2.loc[df_dico2["address-complete-fr"].isin(evite)]
    df_nevite = df_dico2.loc[~df_dico2["address-complete-fr"].isin(evite)]

    res_adm = []
    for _, r in df_nevite.iterrows():
        ad = r["address-complete-fr"]
        res = requests.get(f"https://api-adresse.data.gouv.fr/search/?q={ad}&format=json")
        if res.status_code != 200:
            print(f"L'adresse {ad} a renvoyé l'erreur {res.status_code}", flush=True)
            tadm = pd.DataFrame(data={"address-complete-fr": [ad]})
            res_adm.append(tadm)
        else:
            res = res.json()
            if "features" not in res.keys():
                tadm = pd.DataFrame(data={"address-complete-fr": [ad]})
                res_adm.append(tadm)
            else:
                res2 = res["features"]
                for r2 in res2:
                    r2["address-complete-fr"] = ad
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

        df_adm2 = df_adm.loc[~df_adm["properties.label"].isin(["1 Cote Guy de Maupassant 76380 Canteleu"])]
        df_adm2 = df_adm2.loc[df_adm2["properties.score"] >= 0.5]

        if len(df_adm2) == 0:
            df_adm_final = df_nevite.copy()
            df_adm_final["dep-id"] = ""
            df_adm_final["dep-nom"] = ""
            df_adm_final["reg-nom"] = ""
            df_adm_final["com-code"] = ""
            df_adm_final["ville"] = ""
            df_adm_final = df_adm_final.fillna("")
        else:
            max_score = df_adm2[["address-complete-fr", "properties.score"]].groupby(
                "address-complete-fr").max().reset_index().drop_duplicates().reset_index(drop=True)

            df_adm3 = pd.merge(df_adm2, max_score, on=["address-complete-fr", "properties.score"],
                               how="inner").sort_values(
                "address-complete-fr").reset_index(drop=True)

            if "properties.district" not in df_adm3.columns:
                df_adm3["properties.district"] = np.nan

            df_adm4 = df_adm3[["address-complete-fr", "properties.postcode", "properties.citycode", "properties.city",
                               "properties.context", "properties.district"]].drop_duplicates().reset_index(drop=True)

            df_adm5 = pd.merge(df_nevite, df_adm4, on="address-complete-fr", how="left")

            df_adm5.loc[df_adm5["properties.district"].notna(), "ville"] = df_adm5.loc[
                df_adm5["properties.district"].notna(), "properties.district"]
            df_adm5.loc[df_adm5["properties.district"].isna(), "ville"] = df_adm5.loc[
                df_adm5["properties.district"].isna(), "properties.city"]

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
                                       "ville2": [f"Paris {i + 1}e arrondissment" for i in range(20)]})

            paris.loc[paris["code"] == "75101", "ville2"] = "Paris 1er arrondissment"

            paris16 = pd.DataFrame(data={"code": ['75116'],
                                         "postcode": ['75016'],
                                         "ville2": "Paris 16e arrondissment"})

            paris = pd.concat([paris, paris16], ignore_index=True)
            paris = paris.sort_values(by=['code', 'postcode'])

            lyon = pd.DataFrame(data={"code": [f'6938{i + 1}' for i in range(9)],
                                      "postcode": [f'6900{i + 1}' for i in range(9)],
                                      "ville2": [f"Lyon {i + 1}e arrondissment" for i in range(9)]})

            lyon.loc[lyon["code"] == "69381", "ville2"] = "Lyon 1er arrondissment"

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
                                                        '13010', '13011', '13012', '13013', '13014', '13015', '13016'],
                                           "ville2": [f"Marseille {i + 1}e arrondissment" for i in range(16)]})

            marseille.loc[marseille["code"] == "13201", "ville2"] = "Marseille 1er arrondissment"

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

            df_adm62 = df_adm6[["publication-number", "sequence", "type-party", "ad"]].drop_duplicates().reset_index(
                drop=True)

            df_adm6_compe = df_adm62.groupby(["publication-number", "sequence", "type-party"]).nunique(
                dropna=False).reset_index().rename(columns={"ad": "compte"})

            df_adm6_compe_u = df_adm6_compe.loc[df_adm6_compe["compte"] == 1].sort_values(
                ["publication-number", "sequence"]).reset_index(drop=True)

            df_adm6_u = pd.merge(df_adm6, df_adm6_compe_u, on=["publication-number", "sequence", "type-party"],
                                 how="inner").drop(
                columns=["compte", "ad",
                         "properties.postcode", "properties.city",
                         "properties.district"]).drop_duplicates().reset_index(drop=True)

            df_adm6_compe_m = df_adm6_compe.loc[df_adm6_compe["compte"] > 1].sort_values(
                ["publication-number", "sequence"]).reset_index(drop=True)

            if len(df_adm6_compe_m) > 0:
                df_adm6m = pd.merge(df_adm6, df_adm6_compe_m, on=["publication-number", "sequence", "type-party"],
                                    how="inner")

                dico = {}

                for _, r in df_adm6m.iterrows():
                    ids = r.id
                    pubn = r["publication-number"]
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

                df_adm63 = df_adm6[["publication-number", "sequence", "type-party", "properties.citycode",
                                    "properties.context", "ville",
                                    "address-complete-fr"]].drop_duplicates().reset_index(
                    drop=True)
                df_adm64 = df_adm63.copy()
                df_adm64["ids"] = df_adm64["publication-number"] + " " + df_adm64["sequence"].astype(str) + " " + \
                                  df_adm64[
                                      "type-party"]
                df_adm64 = pd.merge(df_adm64, df_city, on="ids", how="inner").drop(
                    columns=["ids", "city"]).drop_duplicates().reset_index(
                    drop=True)

                df_adm7 = pd.concat([df_adm6_u, df_adm64], ignore_index=True)

            if "df_adm7" in locals():
                df_adm_final = pd.concat([df_adm7, df_evite], ignore_index=True)
            else:
                df_adm_final = pd.concat([df_adm6_u, df_evite], ignore_index=True)

            df_adm_final = df_adm_final.fillna("")

            df_adm_final = df_adm_final.rename(columns={"properties.citycode": "com-code"})
            df_adm_final["liste_context"] = df_adm_final["properties.context"].str.split(", ")
            df_adm_final["dep-id"] = df_adm_final["liste_context"].apply(
                lambda a: a[0] if isinstance(a, list) and len(a) == 3 else "")
            df_adm_final["dep-nom"] = df_adm_final["liste_context"].apply(
                lambda a: a[1] if isinstance(a, list) and len(a) == 3 else "")
            df_adm_final["reg-nom"] = df_adm_final["liste_context"].apply(
                lambda a: a[2] if isinstance(a, list) and len(a) == 3 else "")
            df_adm_final["dep-id"] = df_adm_final["dep-id"].apply(lambda a: "0" + a if len(a) == 1 and a != "" else a)
            df_adm_final["dep-id"] = df_adm_final["dep-id"].apply(lambda a: "0" + a if len(a) == 2 and a != "" else a)
            df_adm_final["dep-id"] = df_adm_final["dep-id"].apply(lambda a: "D" + a if len(a) == 3 and a != "" else a)
            df_adm_final = df_adm_final.drop(columns=["properties.context", "liste_context"])
    else:
        df_adm_final = df_evite.copy()
        df_adm_final["dep-id"] = ""
        df_adm_final["dep-nom"] = ""
        df_adm_final["reg-nom"] = ""
        df_adm_final["com-code"] = ""
        df_adm_final["ville"] = ""
        df_adm_final = df_adm_final.fillna("")

    df_adm_final = df_adm_final[
        ["publication-number", "type-party", "sequence", "address-complete-fr", "com-code", "ville", "dep-id",
         "dep-nom", "reg-nom"]].drop_duplicates().reset_index(drop=True)

    return df_adm_final


def create_df_address():
    # set working directory
    os.chdir(DATA_PATH)

    patents = pd.read_csv("patent.csv", sep="|", encoding="utf-8",
                          engine="python", dtype=types.patent_types)

    part = pd.read_csv("part_p08.csv", sep="|", encoding="utf-8",
                       engine="python", dtype=types.partfin_types, parse_dates=["earliest_filing_date"])

    pat_fr = patents.loc[patents["appln_auth"] == "FR"]

    part["liste_key_person"] = part["key_appln_nr_person"].str.split("_")
    part["applt_seq_nr"] = part["liste_key_person"].apply(lambda a: int(a[-2]))
    part["invt_seq_nr"] = part["liste_key_person"].apply(lambda a: int(a[-1]))
    part = part.drop(columns="liste_key_person")
    part["type-party"] = part.apply(
        lambda a: "applicant" if a["applt_seq_nr"] > 0 and a["invt_seq_nr"] == 0 else "inventor", axis=1)
    part["sequence"] = part.apply(
        lambda a: a["applt_seq_nr"] if a["type-party"] == "applicant" else a["invt_seq_nr"], axis=1)

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
    dict_files = {"fullpath": list(df_files["fullpath"])}

    liste2 = []

    for file in dict_files["fullpath"]:
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

    addresses = pd.concat(liste2)
    addresses = addresses.drop_duplicates().reset_index(drop=True)

    addresses2 = pd.merge(part, addresses, left_on=["appln_publn_number", "type-party", "sequence"],
                          right_on=["publication-number", "type-party", "sequence"], how="left").drop(
        columns=["publication-number", "type-party", "sequence", "applt_seq_nr",
                 "invt_seq_nr"]).reset_index(drop=True)

    ad_siren = addresses2.loc[addresses2["siren"].notna(), ["docdb_family_id", "siren", "com-code",
                                                            "ville",
                                                            "dep-id",
                                                            "dep-nom",
                                                            "reg-nom"]].drop_duplicates().reset_index(
        drop=True)

    ad_siren = ad_siren.loc[ad_siren["com-code"].notna()].drop_duplicates().reset_index(drop=True)
    ad_siren = ad_siren.loc[ad_siren["com-code"] != ""].drop_duplicates().reset_index(drop=True)

    ad_siren_compte = ad_siren[["docdb_family_id", "siren", "com-code"]].groupby(["docdb_family_id", "siren"]).nunique(
        dropna=False).reset_index().rename(
        columns={"com-code": "compte"})

    com_codeu = ad_siren_compte.loc[ad_siren_compte["compte"] == 1].reset_index(drop=True)

    ad_sirenu = pd.merge(ad_siren, com_codeu, on=["docdb_family_id", "siren"], how="inner").drop(columns="compte")

    addresses_siren_pm = addresses2.loc[
        (addresses2["siren"].isin(com_codeu["siren"])) & (addresses2["type"] == "pm") & (
            addresses2["com-code"].isna())].drop(columns=["com-code",
                                                          "ville",
                                                          "dep-id",
                                                          "dep-nom",
                                                          "reg-nom"]).drop_duplicates().reset_index(drop=True)

    addresses_siren_pm = pd.merge(addresses_siren_pm, ad_sirenu, on=["docdb_family_id", "siren"], how="inner")

    reste_addresses = addresses2.loc[~addresses2["key_appln_nr_person"].isin(addresses_siren_pm["key_appln_nr_person"])]

    addresses3 = pd.concat([addresses_siren_pm, reste_addresses], ignore_index=True)

    fam_name = addresses3.loc[addresses3["com-code"].notna(), ["docdb_family_id", "name_corrected",
                                                               "com-code",
                                                               "ville",
                                                               "dep-id",
                                                               "dep-nom",
                                                               "reg-nom"]].drop_duplicates().reset_index(
        drop=True)

    fam_name_compte = fam_name[["docdb_family_id", "name_corrected", "com-code"]].groupby(
        ["docdb_family_id", "name_corrected"]).nunique(dropna=False).reset_index().rename(
        columns={"com-code": "compte"})

    com_nomu = fam_name_compte.loc[fam_name_compte["compte"] == 1]

    fam_nameu = pd.merge(fam_name, com_nomu, on=["docdb_family_id", "name_corrected"], how="inner")
    fam_nameu = fam_nameu.drop(columns=["compte"])

    addresses_fam_name = pd.merge(addresses3.loc[addresses3["com-code"].isna()].drop(columns=["com-code",
                                                                                              "ville",
                                                                                              "dep-id",
                                                                                              "dep-nom",
                                                                                              "reg-nom"]), fam_nameu,
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
    missing_fr2 = missing_fr.loc[missing_fr["com-code"] == ""]
    missing_fr2.to_csv("missing_fr_address.csv", index=False, sep="|", encoding="utf-8")
    swift.upload_object('patstat', 'missing_fr_address.csv')

    pourcentage = round(len(missing_fr2) / len(missing_fr) * 100, 2)
    print(f"Le pourcentage d'adresses manquantes pour les Français est de {pourcentage}.", flush=True)

