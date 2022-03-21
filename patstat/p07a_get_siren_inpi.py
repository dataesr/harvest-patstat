#!/usr/bin/env python
# coding: utf-8

import glob
import os
import re
import zipfile

import pandas as pd
from lxml import etree

from patstat import text_functions as tf

# directory where the files are
DATA_PATH = os.getenv('MOUNTED_VOLUME_TEST')
DATA_INPI_PATH = os.getenv("MOUNTED_VOLUM_INPI_DATA")

COOKIE_NAME = "JSESSIONID"

# set working directory
os.chdir(DATA_PATH)


def dezip_inpi_dos(inpi_path: str) -> str:
    """   This function dezip inpi repertories and removes the original zip

    param inpi_path: the path where the inpi zip are
    type inpi_path: string

    :return:  only a string when done

    """

    dossiers_zip = glob.glob(inpi_path + "*.zip")
    for dos in dossiers_zip:
        with zipfile.ZipFile(dos, 'r') as zip_ref:
            zip_ref.extractall(dos.replace(".zip", ""))
    for dos in dossiers_zip:
        os.remove(dos)

    return 'dézippage terminé'


def get_siren_inpi(inpi_path: str) -> pd.DataFrame:
    """   This function reads all the xml files in 'inpi_path' in input gets some information and return it in a
    dataframe

    param inpi_path: the path where the inpi zip are
    type inpi_path: string

    :return:  a dataframe with 'siren', 'nom', 'numpubli' and 'kind' informations

    """

    # NEW sont les nouvelles demandes de brevet
    # AMD sont les actualisations de dossiers déjà existants à l'INPI précédemment déposés
    dossiers_new = glob.glob(inpi_path + '*NEW*')
    dossiers_amd = glob.glob(inpi_path + '*AMD*')
    l_objets = []
    nbxml = 0
    print('Nombre de fichiers xml lus', flush=True)
    # il y a un dossier par semaine
    for typedos in [dossiers_new, dossiers_amd]:
        for dossier in typedos:
            # pour chaque dossier semaine, il y a une liste de dossiers correspondant à des demandes de brevets :
            # pour chacun il y a un dossier 'doc' contenant
            # le xml à lire
            dos = dossier + '/' + os.listdir(dossier)[0] + '/doc/'
            for fichier in glob.glob(dos + '*.xml'):
                nbxml += 1
                if nbxml % 10000 == 0:
                    print(nbxml, end=',', flush=True)
                try:
                    # on parse le xml, et à l'aide des path on se rend directement dans l'arbre à l'endroit contenant
                    # les variables voulues
                    # on extrait l'information du champ grâce à 'text'
                    arbre = etree.parse(fichier)
                    for applicant in arbre.xpath("//applicant"):
                        objet = {}
                        if applicant.xpath("addressbook/iid"):
                            for siren in applicant.xpath("addressbook/iid"):
                                objet["siren"] = siren.text
                            for nom in applicant.xpath("addressbook/orgname"):
                                objet["nom"] = nom.text
                                # le numpubli est le numéro de publication qui servira à faire le lien avec patstat
                            objet["numpubli"] = arbre.getroot().get("doc-number")
                            objet["kind"] = arbre.getroot().get("kind")
                            l_objets.append(objet)
                except:
                    print("erreur pour le fichier" + fichier + "dans le dossier" + dossier, flush=True)
    siren_inpi = pd.DataFrame(l_objets).drop_duplicates()
    result = siren_inpi[~pd.isna(siren_inpi['siren'])]

    return result


def clean_siren(string: str) -> object:
    """   This function verify if the SIREN in input has 9 characters with digits after cleaning (spaces, punctuations)

    param string: SIREN to clean
    type string: string

    :return:  the given SIREN if he's OK, None otherwise

    """

    siren_regex = re.compile(r'^\d{9,9}$')
    siren_clean = tf.remove_all_spaces(tf.remove_punctuations(string))
    if not pd.isnull(siren_clean):
        mo = siren_regex.search(siren_clean.replace(" ", ""))
        if (mo is None) or (siren_clean == '000000000'):
            return None
        else:
            return siren_clean
    else:
        return None


def clean_siren_table(siren_table: pd.DataFrame, siren_var: str, name_var: str) -> pd.DataFrame:
    """   This function verify if the SIREN in input has 9 characters with digits after cleaning (spaces, punctuations)

    param string: SIREN to clean
    type string: string

    :return:  the given SIREN if he's OK, None otherwise

    """

    siren_table[siren_var] = siren_table[siren_var].astype(str).apply(clean_siren)
    siren_table_clear = siren_table[
        (~pd.isna(siren_table[siren_var])) & (siren_table[name_var] != '')].drop_duplicates()
    for col in siren_table_clear.columns:
        siren_table_clear[col] = siren_table_clear[col].fillna('')
        siren_table_clear[col] = siren_table_clear[col].astype(str)

    return siren_table_clear


def creat_patent_siren_table(siren_table: pd.DataFrame, siren_var: str, name_var: str, patent_var: str) -> pd.DataFrame:
    """
    Keep only cases where there is a single SIREN for a single patent-name combination

    :param siren_table: df with name, patent and SIREN
    :param siren_var: name SIREN variable
    :param name_var: name name vraiable
    :param patent_var: name patent variable
    :return: df
    """
    siren_tb = siren_table[[patent_var, name_var, siren_var]].drop_duplicates().copy()
    # on compte le nombre de siren par nom d'entp et patent
    nom_doublon = siren_tb.groupby([patent_var, name_var]).count().reset_index()
    # on sélectionne les couples patent/noms qui n'ont qu'un seul siren correspondant
    noms_sans_doublons = nom_doublon[nom_doublon[siren_var] < 2][[patent_var, name_var]]
    patent_siren_table = siren_tb.merge(noms_sans_doublons, on=[patent_var, name_var], how='inner')

    return patent_siren_table


def creat_general_siren_table(siren_table: pd.DataFrame, siren_var: str, name_var: str) -> pd.DataFrame:
    """
    Keep only cases where there is a single SIREN for a single name

    :param siren_table: df with name and SIREN
    :param siren_var: name SIREN variable
    :param name_var: name name vraiable
    :return: df
    """
    siren_tb = siren_table[[siren_var, name_var]].drop_duplicates().copy()
    # on compte le nombre de siren par nom d'entp
    nom_doublon = siren_tb.groupby(name_var).count().reset_index()
    # on sélectionne les noms qui n'ont qu'un seul siren correspondant
    set_siren_unique = set(nom_doublon[nom_doublon[siren_var] < 2][name_var])
    general_siren_table = siren_tb[siren_tb[name_var].isin(set_siren_unique)]

    return general_siren_table


def get_siren():
    # Dans le dossier 'inpi_path', on met tous les nouveaux zip de l'INPI, téléchargés sur le serveur ftp :
    # Bfrbibli@www.inpi.net
    # le mot de passe est sur azendoo
    # Inutile de tout récupérer, ne prendre que la dernière année, tout mettre au même endroit

    # On commence par dézipper tous les fichiers nouveaux, puis on supprime les zip

    dezip_inpi_dos(DATA_INPI_PATH)
    print("dezip_inpi_dos fini", flush=True)

    # Ensuite on parcourt tous les XML pour récupérer les SIREN et noms des entreprises dans une liste l_objets,
    # qu'on transforme ensuite en tableau

    siren_inpi = get_siren_inpi(DATA_INPI_PATH)
    print("get_siren_inpi terminé", flush=True)

    # On enlève les SIREN erronés (suppression des espaces et sélection des suites de 9 numéros seulement)

    siren_inpi2 = clean_siren_table(siren_inpi, 'siren', 'nom').drop(columns={'kind'}).drop_duplicates()
    print("clean_siren_table terminé", flush=True)

    # on enlève les doublons par couple numpubli/nom

    siren_inpi_brevet = creat_patent_siren_table(siren_inpi2, 'siren', 'nom', 'numpubli')
    print("creat_patent_siren_table terminé", flush=True)

    siren_inpi_brevet.to_csv('siren_inpi_brevet.csv', sep='|', index=False)
    print("siren_inpi_brevet to csv terminé", flush=True)

    # on crée une autre table avec les correspondances de nom et siren sans les numéros de brevets
    # on enlève donc les correspondances multiples : noms avec plusieurs siren différents attribués

    siren_inpi_generale = creat_general_siren_table(siren_inpi_brevet, 'siren', 'nom')
    print("creat_general_siren_table terminé", flush=True)

    siren_inpi_generale.to_csv('siren_inpi_generale.csv', sep='|', index=False)
    print("siren_inpi_generale to csv terminé", flush=True)
