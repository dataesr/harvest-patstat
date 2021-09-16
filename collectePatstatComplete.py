# !/usr/bin/env python
# coding: utf-8

# Ce programme permet de requêter l'API de Patstat et de télécharger les fichiers zippés.

from bs4 import BeautifulSoup
import requests
import config_emmanuel as config
import re


# fonction pour faire les requêtes GET sur l'API Patstat
def appel_patstat(url, _session):
    response = _session.get(url)
    status = response.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to access the URL")
    else:
        print("URL successfully accessed")
    return _session.get(url)


# fonction pour s'authentifier sur l'API Patstat
def connexion_api(adresse):
    _session = requests.session()  # ouverture de la session de requête
    _session.get(adresse)
    # identification sur Patstat
    response = _session.get(adresse)
    status = response.status_code
    if status != 200:
        raise ConnectionError("Failed while trying to authenticate")
    else:
        print("Authenticated")
    return _session


# fonction pour extraire les URLs du résultat de la requête GET sur l'API Patstat
def extraction_url(contenu):
    soup = BeautifulSoup(contenu.text, "lxml")
    listurl = [elt.text for elt in soup.findAll("url")]
    return listurl


# fonction pour ne garder que le nom du fichier sans le reste de l'URL
def liste_noms(listurl):
    listnoms = list(map(lambda a: re.sub("https://publication.epo.org/raw-data/products/86/editions/6891/files/",
                                         "",
                                         a),
                        listurl))
    return listnoms


# fonction pour télécharger les fichiers zip et les enregistrer en local
def telechargement_ecriture(listurl, _session, listnoms):
    for i in range(len(listurl)):
        req = appel_patstat(listurl[i], _session)
        with open(listnoms[i], "wb") as code:
            code.write(req.content)
            print(f"File {listnoms[i]} successfully loaded")




def main():
    # adresse authentifiction
    urlId = f"https://publication.epo.org/raw-data/authentication?login={config.USER}&pwd={config.PWD}&action=1&format=1"

    # authentification
    session = connexion_api(urlId)

    # requête API Patstat pour accéder à liste de tous les URLs des fichiers zip à télécharger
    requete = appel_patstat("https://publication.epo.org/raw-data/products/86/editions/6891/files", session)

    # extraction des URLs
    listUrl = extraction_url(requete)

    # extraction des noms de fichier
    listNoms = liste_noms(listUrl)
    listNoms = listNoms

    # # téléchargement et écriture des fichiers zip
    telechargement_ecriture(listUrl, session, listNoms)


if __name__ == '__main__':
    main()