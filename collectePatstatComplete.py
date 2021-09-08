from bs4 import BeautifulSoup
import requests
import config

# ouverture de la session de requête
s = requests.session()

# identification sur Patstat
s.get(f'https://publication.epo.org/raw-data/authentication?login={config.USER }&pwd={config.PWD}&action=1&format=1')

# requête pour accéder au XML de la page contenant les liens vers tous les fichiers zippés
r = s.get("https://publication.epo.org/raw-data/products/86/editions/6891/files")

# "parse" le XML pour extraire la liste des URLs
soup = BeautifulSoup(r.text, "lxml")
listUrl = [elt.text for elt in soup.findAll("url")]


def appel_patstat(n):
    return s.get(n)

for i in range(len(listUrl)):
    x = appel_patstat(listUrl[i])
