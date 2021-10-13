# Documentation des traitements de la base de données PATSTAT

## Organisation des programmes

<ol>
<li>collectePatstatComplete : requête la base de données PATSTAT et télécharge tous les fichiers zippés de la base</li>
<li>dezippage : dézippe tous les fichiers zippés de Patstat et range les fichiers CSV issus du dézippage dans le dossier correspondant à leur table PATSTAT</li>
<li>csv_files_querying : permet de charger les CSV issus de PATSTAT dans Python par morceau, tout en appliquant des filtres - permet de charger les fichiers et de s'en servir en tant que dataframe</li>
<li>dtypes_patstat_declaration : type des données des fichiers CSV issus de PATSTAT - facilite la création des dataframes</li>
<li>p01_family_scope : limite la sélection aux demandes françaises. Inputs = tls 201, 206 et 207. Outputs = docdb_family_scope et patent_scope</li>
<li>p02_titles_abstracts : extrait les titres et les résumés des demandes. Inputs = patent_scope, tls 202 et 203. Outputs = titles et abstracts</li>
<li>p03_patents : extrait l'information sur les publications et consolide le jeu de données &mdash; aux demandes françaises sont ajoutés :
<ul>
<li>phase européeen en cours ou qui a eu lieu</li>
<li>phase internationale en cours ou qui a eu lieu</li>
<li>informations sur la 1ère publication et le 1er octroi du brevet</li>
<li>l'existence d'une priorité</li>
<li>les informations concernant le titre et le résumé.</li>
</ul>
Inputs = patent_scope, abstracts, titles, tls 204 et tls 211. Outputs = publications et patent.
</li>
<li>p04_families : extrait les informations sur les familles de brevets (1ère publication, 1er octroi,...). Inputs = patent, tls 209, tls 224, tls 225 et lib_cpc.csv (fichier extrait du XML de classification coopérative des brevets &mdash; programme commun OEB et USPTO &mdash; reste à voir comment récupérer et traiter ces données). Outputs : families et families_technologies.</li>
<li>p05_creat_participants</li>
<li>p06_clean_participants_individuals</li>
<li>p07a_get_siren_inpi</li>
<li>p07b_clean_participants_entp</li>
<li>p08_participants_final</li>
<li>p09_geoloc</li>
</ol>

## Notes

### Enregistrements fichiers

<ul>
<li>217 760 enregistrements dans docdb_family_scope</li>
<li>581 733 enregistrements pour patent_scope</li>
<li>579 217 enregistrements pour titles (titres des brevets)</li>
<li>542 372 enregistrements pour abstracts (résumés des brevets)</li>
<li>Manque des titres et des résumés pour les demandes. Ce ne sont pas les demandes artificielles car il n'y en a que 2 (appln_kind "D")</li>
</ul>

### Dans patent_scope :

Table qui donne les informations sur les demandes de brevet (application - appln). Correspond à la table 201 de PATSTAT mais filtrée (ne conserve que les demandes françaises).

#### appln_id : 
ID de la demande (valeur unique, normalement stable dans le temps depuis 2011 mais il peut arriver que cet ID soit changé)

#### appln_auth : 
Autorité nationale, régionale ou internationale en charge du traitement de la demande

#### appln_nr : 
Numéro de demande pour les brevets européens le cas échéant. Vide si demande artificielle ou si pas trouvée dans DOCDB

#### appln_kind : 
Type de demande (brevet, modèle et certificat d'utilité). 

Dans patent_scope, les valeurs de appln_kind = "A", "C", "D", "F", "K", "L", "T", "U", "W".

"D" est pour les demandes &raquo;&nbsp;artifielles&nbsp;&raquo;, càd dont PATSTAT n'a pas la trace en tant que telles mais pour lesqeulles il existe une priorité.

Application kind-codes D and Q : Application kind-codes 'D' and 'Q' identify "dummy" applications. Distinction between 'D' and 'Q' is made to help identify the corrective action required : Issues involving application kind-code 'D' can be resolved by an automated back-file correction exercise. Issues involving application kind-code 'Q' need intellectual effort and are being tackled manually, one by one.

Application kind-codes K, L, M, N, O. A limited number of countries, e.g. MC PH RU SU, supply identical application-identifications for separate publications. In order to resolve that issue, kind-code 'K' is allocated to the first duplicate encountered for a given application-identification, 'L' to the second etc.

French applications with kind-codes E, F, M : The kind-code of these French applications should be 'A'.Applications with kind-code E, F or M have been loaded with an incorrect kind-code at the time.

#### appln_filing_date et appln_filing_year : 
Date de la demande

#### appln_nr_epodoc : 
Numéro de demande EPODOC de l'Office européen des brevets &mdash; &laquo;&nbsp;deprecated&nbsp;&raquo; &mdash; sera supprimé dans une des prochaines éditions de PATSTAT. Je propose d'utiliser la clef alternative indiquée par le manuel PATSTAT pour avoir un identifiant unique et stable key_appln_nr : la concaténation de appln_auth, appln_nr, appln_kind et receiving_office

#### appln_nr_original : 
Numéro de demande originel

#### ipr_type : 
Domaine de propriété intellectuelle couvert par la demande (brevet, modèle ou certificat d'utilité).

#### receiving_office : 
Bureau où la demande internationale a été effectuée - vide si demande nationale ou régionale.

#### internat_appln_id : 
Numéro d'identification de la demande de brevet de la procédure Patent Cooperation Treaty (PCT). Si le numéro est égal à zéro, pas de demande PCT préalable.

#### int, nat, reg_phase 
Ces variables disent est-ce que la demande est ou a été dans cette phase - vide pour les demandes pour lesquelles l'info n'est pas connue
Routes possibles d'une demande : <br>


|International phase | Regional phase| National phase|
|--------------------|---------------|---------------|
| Y/N/N              | Y/Y/N         | Y/Y/Y         |
|                    |               | Y/N/Y         |
|                    | N/Y/N         | N/Y/Y         |
|                    |               | N/N/Y         |
<br>

Les combinaisons disponibles de int, nat, reg_phase dans patent_scope printemps 2021 sont : <br>


|International phase | Regional phase| National phase|Nombre |
|--------------------|---------------|---------------|-------|
| N                  |               | Y             | 129   |
| N                  | N             | Y             | 202754|
| N                  | Y             | N             | 36912 |
| N                  | Y             | Y             | 11911 |
| Y                  |               | Y             | 211   |
| Y                  | N             | N             | 86681 |
| Y                  | N             | Y             | 173951|
| Y                  | Y             | N             | 54650 |
| Y                  | Y             | Y             | 14534 |


<br>

#### earliest_filing_date
1ère date de demande : date la plus ancienne parmi toutes les possibilités possibles (demande nationale, internationale, priorité convention de Paris,...)

Pour patent_scope printemps 2021, les dates vont du 22 janvier 1999 au 18 décembre 2020.

#### earliest_filing_year
Année de la 1ère demande (mêmes conditions que earliest_filing_date)

#### earliest_filing_id
ID de la 1ère demande : ID la plus ancienne parmi toutes les possibilités possibles (demande nationale, internationale, priorité convention de Paris,...)

Il y 170 647 ID uniques pour la table patent_scope printemps 2021.

#### earliest_publn_date
1ère date de publication de la demande &mdash; les demandes de la famille antérieures ne sont pas prises en compte dans ce cas

POur patent_scope printemps 2021, les dates vont du 17 février 2010 au 28 janvier 2021.

#### earliest_publn_year
Année de la 1ère publication (mêmes conditions que earliest_publn_date)

#### earliest_pat_publn_id
ID de la 1ère publication (mêmes conditions que earliest_publn_date)

#### granted
Y si la demande a été octroyée et N si elle ne l'a pas été. N signifie exactement qu'il n'y a <U>AUCUNE</U> indication dans les données disponibles que la demande a été octroyée.

Dans patent_scope, il y a 273 237 Y (environ 47 %) et 308 496 N (53 %).

#### docdb_family_id
ID de la famille DOCDB qui est une variable créée automatiquement par l'OEB : une famille DOCDB signifie que les différentes demandes partagenet exactement les mêmes priorités

Pour patent_scope printemps 2021, il y a 173 410 ID uniques.

#### inpadoc_family_id
ID de la famille INPADOC : variable qui indique que la demande partage une priorité directe ou via une demande tiers. Chaque application appartient à une et une seule famille INPADOC.

Pour patent_scope printemps 2021, il y a 167 708 ID uniques.

#### docdb_family_size
Nombre de demandes au sein de chaque famille DOCDB.

La taille des familles va de 1 à 242. La taille médiane est de 5, la moyenne de 7 et le mode de 2. Plus de 85 % des enregistrements ont une taille de famille compris entre 1 et 10 inclus.

#### nb_citing_docdb_fam
Nombre de citations par une famille DOCDB d'au moins une publication ou demande de la famille DOCDB de la demande actuelle.

Le nombre de citations va de 0 à 768. La médiane est de 1, la moyenne de 4,876 et le mode de 0. Plus de 90 % des demandes ont moins de 11 citations.

#### nb_applicants
Nombre de demandeurs pour chaque demande selon la publication la plus récente qui contient des noms de personnes en caractères latins (si pas de caractères, le nombre de demandeurs est fixé à zéro &mdash; ça ne veut pas dire qu'il n'y a pas de demandeurs).

Le nombre de demandeurs va de 0 à 30. La médiane est de 1, la moyenne de 1,391 et le mode de 1. Plus de 98 % des demandes ont moins de 6 demandeurs et 3,54 % ont &laquo;&nbsp;zéro&nbsp;&raquo; demandeur.

#### nb_inventors
Nombre d'inventeurs pour chaque demande selon la publication la plus récente qui contient des noms de personnes en caractères latins (si pas de caractères, le nombre d'inventeurs est fixé à zéro &mdash; ça ne veut pas dire qu'il n'y a pas d'inventeurs).

Le nombre d'inventeurs va de 0 à 40. La médiane est de 2, la moyenne de 2,617 et le mode de 2. Plus de 93 % des demandes ont moins de 6 inventeurs et 4,43 % ont &laquo;&nbsp;zéro&nbsp;&raquo; inventeur.

### Dans titles :
Ce fichier est issu de la table 202 et contient 3 variables :
<ul>
<li>appln_id</li>
<li>appln_title_lg</li>
<li>appln_title</li>
</ul>


Aucune valeur manquante. 69,24 % des titres en anglais et 19,19 % en français. Anglais comme langue par défaut : les
titres dans les autres langues ne sont considérés que s'il n'existe pas de titre en anglais. Langues des titres :
<ul>
<li>tr = turc</li>
<li>el = grec</li>
<li>pt = portugais</li>
<li>pl = polonais</li>
<li>ar = arabe</li>
<li>lt = lituanien</li>
<li>et = estonien</li>
<li>hr = croate</li>
<li>he = hébreu</li>
<li>uk = ukrainien</li>
<li>en = anglais</li>
<li>fr = français</li>
<li>ja = japonais</li>
<li>es = espagnol</li>
<li>ko = coréen</li>
<li>de = allemand</li>
<li>da = danois</li>
<li>ru = russe</li>
<li>zh = chinois</li>
<li>it = italien</li>
<li>no = norvégien</li>
<li>sv = suédois</li>
<li>ar = arabe</li>
<li>nl = néerlandais</li>
<li>fi = finnois</li>
</ul>
<br>


|langues|occurences|pourcentage|
|-------|----------|-----------|
|en     |401076    |69,24      |
|fr     |111173    |19,19      |
|ja     |19493     |3,37       |
|es     |16865     |2,91       |
|pt     |13258     |2,29       |
|ko     |4556      |0,79       |
|de     |3641      |0,63       |
|da     |3194      |0,55       |
|ru     |2851      |0,49       |
|zh     |1205      |0,21       |
|tr     |673       |0,12       |
|it     |321       |0,06       |
|el     |300       |0,05       |
|no     |226       |0,04       |
|uk     |199       |0,03       |
|nl     |59        |0,01       |
|sv     |57        |0,01       |
|ar     |56        |0,01       |
|fi     |6         |0,00       |
|pl     |3         |0,00       |
|lt     |2         |0,00       |
|et     |1         |0,00       |
|hr     |1         |0,00       |
|he     |1         |0,00       |


### Dans abstracts :
Ce fichier est issu de la table 203 et contient 3 variables :
<ul>
<li>appln_id</li>
<li>appln_title_lg</li>
<li>appln_title</li>
</ul>

Il y a des valeurs manquantes dans appln_abstract - 3 dans le cas du jeu de données PATSTAT Spring 2021 (appln_id :
446022198, 503487228 et 503602912). Comme pour les titres, l'anglais est la langue par défaut : les autres langues
n'apparaissent que si un résumé en anglais n'est pas disponible. langues des titres :
<ul>
<li>sr : serbe</li>
<li>ro : roumain</li>
<li>cs : tchèque</li>
<li>me : monténégrin</li>
</ul>


|langues|occurences|pourcentage|
|-------|----------|-----------|
|en|344927|63,60|
|fr|133354|24,59|
|ja|15936|2,94|
|es|15634|2,88|
|ko|12264|2,26|
|pt|11413|2,10|
|de|4074|0,75|
|ru|1713|0,32|
|zh|1206|0,22|
|tr|673|0,12|
|uk|321|0,06|
|el|299|0,06|
|no|241|0,04|
|sr|109|0,02|
|ar|77|0,01|
|pl|49|0,01|
|nl|39|0,01|
|sv|13|0,00|
|da|11|0,00|
|ro|9|0,00|
|hr|3|0,00|
|cs|2|0,00|
|me|2|0,00|
|fi|1|0,00|
|it|1|0,00|
|lv|1|0,00|

### Dans publications


### Dans patent
