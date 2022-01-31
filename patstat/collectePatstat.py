"""
Patstat met à disposition une API pour charger les données.
Son utilisation doit faciliter la collecte sur Patstat.
Ce fichier teste la collecte d'un seul fichier zippé après l'ouverture d'une session.
"""

import requests
import config

# ouverture de la session
s = requests.session()

# identification sur Patstat
s.get(f'https://publication.epo.org/raw-data/authentication?login={config.USER }&pwd={config.PWD}&action=1&format=1')

# requête pour accéder au fichier zippé contenant le 1er paquet de des données Patstat
r = s.get("https://publication.epo.org/raw-data/products/86/editions/6891/files/data_PATSTAT_Global_2021_Spring_01.zip")

# enregistrement du fichier zip dans le dossier projet
with open("data_PATSTAT_Global_2021_Spring_01.zip", "wb") as code:
    code.write(r.content)