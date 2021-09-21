# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de requêter l'API de Patstat et de télécharger les fichiers zippés.
# This script collects PATSTAT Global data from EPO's API, download and write zipped folders

from bs4 import BeautifulSoup
import config_emmanuel as config
import re
import requests
from retry import retry

URL_PATSTAT = "https://publication.epo.org/raw-data"


# fonction pour faire les requêtes GET sur l'API Patstat
# function that produces a GET request and check if it's successful
def get_url(url, _session):
    response = _session.get(url)
    status = response.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed")
    return response


# fonction pour s'authentifier sur l'API Patstat
# function to authenticate on PATSTAT API
def connexion_api(address):
    _session = requests.session()  # ouverture de la session de requête
    res = _session.get(address)
    rep = BeautifulSoup(res.text, "lxml")
    stat = rep.find("authentication").attrs.get("authenticated")
    if stat != "true":
        raise ConnectionError("Failed while trying to authenticate")
    else:
        print("Authenticated")
    return _session


# fonction pour extraire les URLs du résultat de la requête GET sur l'API Patstat
# function to extract URLs from the GET request on the PATSTAT API
def extraction_url(content):
    soup = BeautifulSoup(content.text, "lxml")
    listurl = [elt.text for elt in soup.findAll("url")]
    return listurl


# fonction pour ne garder que le nom du fichier sans le reste de l'URL
# function to keep only the file name from the URL
def name_list(listurl):
    namelist = list(map(lambda a: re.sub(f"{URL_PATSTAT}/products/86/editions/6891/files/", "", a), listurl))
    return namelist


@retry(tries=3, delay=2)
# fonction pour télécharger les fichiers zip et les enregistrer en local
# function to dowload, name and write the zipped folders
def download_write(listurl, _session, namelist):
    for i in range(len(listurl)):
        req = get_url(listurl[i], _session)
        with open(namelist[i], "wb") as code:
            code.write(req.content)


def main():
    # adresse authentifiction
    # authentication address
    url_id = f"{URL_PATSTAT}/authentication?login={config.USER}&pwd={config.PWD}&action=1&format=1"

    # authentification
    # authentication
    session = connexion_api(url_id)

    # requête API Patstat pour accéder à liste de tous les URLs des fichiers zip à télécharger
    # API request on Patstat to access the list of all the zipped folders URLs to download
    requete = get_url(f"{URL_PATSTAT}/products/86/editions/6891/files", session)

    # extraction des URLs
    # URLs extraction
    list_url = extraction_url(requete)

    # extraction des noms de fichier
    # files name extraction
    list_name = name_list(list_url)

    # # téléchargement et écriture des fichiers zip
    # download and write zipped files
    download_write(list_url, session, list_name)


if __name__ == '__main__':
    main()
