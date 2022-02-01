#!/usr/bin/env python
# coding: utf-8

import glob
import os
import zipfile

import numpy as np
import pandas as pd
from lxml import etree

from patstat import a01_outils_divers as a01
from patstat import text_functions as tf

# directory where the files are
# DATA_PATH = os.getenv('MOUNTED_VOLUME')
DATA_PATH = "/run/media/julia/DATA/test/"
# todo : change with env path
inpi_path = "/run/media/julia/DATA/DONNEES/PATENTS/SOURCES/INPI/"

# set working directory
os.chdir(DATA_PATH)


def parse_one_file(file_path: str) -> list:
    arbre = etree.parse(file_path)
    l_obj = []
    parties = ['inventor', 'applicant']
    for partie in parties:
        if arbre.xpath('//' + partie):
            for partie_root in arbre.xpath('//' + partie):
                objet = {"type": partie, "sequence": partie_root.get("sequence"),
                         "numpubli": arbre.getroot().get("doc-number"), "kind": arbre.getroot().get("kind")}
                if arbre.getroot().get("family-id"):
                    objet["family_id"] = arbre.getroot().get('family-id')
                addressbook = partie_root.xpath("addressbook")
                address = addressbook[0].xpath("address")[0]
                if len(addressbook):
                    lastname = addressbook[0].xpath("last-name")
                    firstname = addressbook[0].xpath("first-name")
                    orgname = addressbook[0].xpath("orgname")
                    siren = addressbook[0].xpath("iid")
                    if len(lastname):
                        objet["lastname"] = lastname[0].text
                    if len(firstname):
                        objet["firstname"] = firstname[0].text
                    if len(orgname):
                        objet["orgname"] = orgname[0].text
                    if len(siren):
                        objet["siren"] = siren[0].text
                if len(address):
                    adresse = address.xpath("address-1")
                    city = address.xpath("city")
                    postcode = address.xpath("postcode")
                    country = address.xpath("country")
                    if len(adresse):
                        objet["address"] = adresse[0].text
                    if len(city):
                        objet["city"] = city[0].text
                    if len(postcode):
                        objet["postcode"] = postcode[0].text
                    if len(country):
                        objet["country"] = country[0].text
                l_obj.append(objet)
    return l_obj


def main():
    # On commence par dézipper tous les fichiers nouveaux, puis on supprime les zip

    dossiers_zip = glob.glob(inpi_path + "2010-2020/*.zip")

    for dos in dossiers_zip:
        with zipfile.ZipFile(dos, 'r') as zip_ref:
            zip_ref.extractall(dos.replace(".zip", ""))

    for dos in dossiers_zip:
        os.remove(dos)

    dossiers = glob.glob(inpi_path + "2010-2020/*")
    for dos in dossiers:
        l_to_dezip = glob.glob(dos + '/doc/*')
        for file_to_dezip in l_to_dezip:
            with zipfile.ZipFile(file_to_dezip, 'r') as zip_ref:
                zip_ref.extractall(dos.replace(".zip", ""))

    # Ensuite on parcourt tous les XML pour récupérer les SIREN et noms des entreprises dans une liste l_objets,
    # qu'on transforme ensuite en tableau

    listxml = []
    for (dirpath, dirnames, filenames) in os.walk(inpi_path + "2010-2020"):
        listxml += glob.glob(dirpath + '/*.xml')

    l_objets = []
    for fichier in listxml:
        l_objets.append(parse_one_file(fichier))

    a01.save_object(l_objets, os.path.join('list_adress_inpi.p'))

    list_adress = a01.load_object(os.path.join('list_adress_inpi.p'))

    list_adresses_inpi = []
    for list_brevt_adr in list_adress:
        for part_adr in list_brevt_adr:
            list_adresses_inpi.append(part_adr)

    adresses_inpi = pd.DataFrame(list_adresses_inpi)

    for col in adresses_inpi.columns:
        adresses_inpi[col] = adresses_inpi[col].replace(np.nan, '')
    adresses_inpi = adresses_inpi.drop_duplicates()

    adresses_inpi['name'] = (
            adresses_inpi['lastname'] + ' ' + adresses_inpi['firstname'] + ' ' + adresses_inpi['orgname'])
    adresses_inpi['name'] = adresses_inpi['name'].apply(lambda a: tf.remove_multiple_spaces(a)).apply(
        lambda b: tf.remove_first_end_spaces(b))
    adresses_inpi['name'] = adresses_inpi['name'].apply(lambda x: x.lower())

    # quand on a des doublons d'adresses, pour le même couple numpubli/name :
    # On choisit de garder l'adresse de l'inventeur

    set_doublons = set(adresses_inpi[['numpubli', 'name', 'address', 'city', 'postcode']].groupby(['numpubli', 'name'])[
                           'postcode'].count().reset_index().query('postcode > 1')['numpubli'])

    adresses_doublons = \
        adresses_inpi[(adresses_inpi['numpubli'].isin(set_doublons)) & (adresses_inpi['type'] == 'inventor')][
            ['numpubli', 'name', 'sequence', 'address', 'city', 'postcode']].drop_duplicates()

    adresses_doublons = adresses_doublons.groupby(['numpubli', 'name']).first().reset_index().drop(columns={'sequence'})

    adresses_inpi2 = pd.concat([adresses_inpi[~(adresses_inpi['numpubli'].isin(set_doublons))][
                                    ['numpubli', 'name', 'address', 'city', 'postcode']], adresses_doublons])

    adresses_inpi2.to_csv('adresses_inpi.csv', sep='|', index=False)

    # on garde l'info corrigée des noms et prénoms séparés

    inpi_first_and_last_names = adresses_inpi[adresses_inpi['firstname'] != ''][
        ['name', 'firstname', 'lastname']].drop_duplicates()

    inpi_first_and_last_names.to_csv('inpi_first_and_last_names.csv', sep='|', index=False)


if __name__ == '__main__':
    main()
