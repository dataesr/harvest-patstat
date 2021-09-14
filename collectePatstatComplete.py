from bs4 import BeautifulSoup
import requests
import config
import re

# fonction pour faire les requêtes GET sur l'API Patstat
def appel_patstat(n):
    return s.get(n)

# fonction pour s'authentifier sur l'API Patstat
def connexion_api(adresse):
    session = requests.session()  # ouverture de la session de requête
    # identification sur Patstat
    session.get(adresse)
    return session

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
def telechargement_ecriture(listurl, listnoms):
    for i in range(len(listurl)):
        req = appel_patstat(listurl[i])
        with open(listnoms[i], "wb") as code:
            code.write(req.content)

# adresse authentifiction
urlId = f"https://publication.epo.org/raw-data/authentication?login={config.USER}&pwd={config.PWD}&action=1&format=1"

# authentification
s = connexion_api(urlId)

# requête API Patstat pour accéder à liste de tous les URLs des fichiers zip à télécharger
r = appel_patstat("https://publication.epo.org/raw-data/products/86/editions/6891/files")

# extraction des URLs
listUrl = extraction_url(r)

# extraction des noms de fichier
listNoms = liste_noms(listUrl)

# téléchargement et écriture des fichiers zip
telechargement_ecriture(listUrl, listNoms)
