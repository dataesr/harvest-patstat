from bs4 import BeautifulSoup
import requests
import config
import re


def appel_patstat(n):
    return s.get(n)


def connexion_api(adresse):
    session = requests.session()  # ouverture de la session de requête
    # identification sur Patstat
    session.get(adresse)
    return session


def extraction_url(contenu):
    soup = BeautifulSoup(contenu.text, "lxml")
    listurl = [elt.text for elt in soup.findAll("url")]
    return listurl


def liste_noms(listurl):
    listnoms = list(map(lambda a: re.sub("https://publication.epo.org/raw-data/products/86/editions/6891/files/",
                                         "",
                                         a),
                        listurl))
    return listnoms


def telechargement_ecriture(listurl, listnoms):
    for i in range(len(listurl)):
        req = appel_patstat(listurl[i])
        with open(listnoms[i], "wb") as code:
            code.write(req.content)


urlId = f"https://publication.epo.org/raw-data/authentication?login={config.USER}&pwd={config.PWD}&action=1&format=1"

s = connexion_api(urlId)

r = appel_patstat("https://publication.epo.org/raw-data/products/86/editions/6891/files")

listUrl = extraction_url(r)

listNoms = liste_noms(listUrl)

telechargement_ecriture(listUrl, listNoms)
