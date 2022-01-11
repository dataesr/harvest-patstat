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
<li>correction_type : programme qui permet d'élaborer le modèle de classification entre les personnes morales et les personnes physiques sur la base des noms. Jeu de données suréchantillonné car plus de personnes physiques que morales. Test algo fasttext et extreme gradient boosting avec différentes variables. Sélectionne le modèle qui obtient le meilleur résultat. Actuellement, fasttext avec label, doc_std_name et person_name. Inputs : tls206, tls207, patent et part_init. Outputs : modèles et l'affichage du meilleur modèle dans la console.</li>
<li>recuperation : appln_nr_epodoc est "deprecated" mais servait d'identifiant pour old_part de p05. Ce programme a pour but de chercher les numéros de demande appln_id correspondant aux identifiants appln_nr_epodoc et permet de rajouter la clef d'identification alternative de tls 201 key_appln_nr (appln_auth, appln_nr, appln_king and receiving_office). L'id_participant, basé sur appln_nr_epodoc est toutefois maintenu assurer la continuité. La récupération est finalisée dans p05 avec les lignes commentées au début de la fonction main. Inputs : tls201 et partfin. Output : old_part_key.</li>
<li>p05_creat_participants : récupère les données de la précédente édition et ajoute les informations concernant le type de personne (morale ou physique) en mettant en &oelig;uvre le modèle issu de correction_type. Clef composée de key_appln_nr et person_id : 1 ligne = 1 demande de brevet associée à chacune des personnes de la demande. Inputs : tls206, tls207, patent, part_init et meilleur modèle. Output : part_init et part</li>
<li>clean_participants : séparation entre les personnes physiques et morales, déduplication des participants par famille INPADOC et premier nettoyage des noms. Input : part. Output : part_ind et part_entp</li>
<li>p06_clean_participants_individuals : nettoyage approfondi des noms des personnes physiques, attribution d'un seul pays par personne (celui le plus fréquent) et attribution d'un genre (probablité en fonction du nom). Inputs : part_ind et API dataesr pour le genre. Outputs : sex_table, part_individuals</li>
<li>p07a_get_siren_inpi : récupère les numéros de publication, noms et SIREN des personnes morales ayant fait des demandes de brevet auprès de l'INPI. Input : base de données brevets de l'INPI au format XML. Outputs : siren_inpi_brevet et siren_inpi_generale.</li>
<li>p07b_clean_participants_entp</li>
<li>p08_participants_final</li>
<li>p09_geoloc</li>
</ol>

## Notes :

### Enregistrements fichiers :

<ul>
<li>217&nbsp;760 enregistrements dans docdb_family_scope</li>
<li>581&nbsp;733 enregistrements pour patent_scope</li>
<li>579&nbsp;217 enregistrements pour titles (titres des brevets)</li>
<li>542&nbsp;372 enregistrements pour abstracts (résumés des brevets)&nbsp;: il manque des titres et des résumés pour les demandes. Ce ne sont pas les demandes artificielles, car il n'y en a que 2 (appln_kind "D")</li>
<li>839&nbsp;073 enregistrements pour publications</li>
<li>581&nbsp;733 enregistrements pour patent</li>
</ul>

### Dans patent_scope :

Table qui donne les informations sur les demandes de brevet (application - appln). Correspond à la table 201 de PATSTAT mais filtrée (ne conserve que les demandes françaises).

#### appln_id : 
ID de la demande (valeur unique, normalement stable dans le temps depuis 2011, mais il peut arriver que cet ID soit changé)

#### appln_auth : 
Autorité nationale, régionale ou internationale en charge du traitement de la demande

#### appln_nr : 
Numéro de demande pour les brevets européens le cas échéant. Vide si demande artificielle ou si pas trouvée dans DOCDB

#### appln_kind : 
Type de demande (brevet, modèle et certificat d'utilité). 

Dans patent_scope, les valeurs d'appln_kind = "A", "C", "D", "F", "K", "L", "T", "U", "W".

"D" est pour les demandes &raquo;&nbsp;artifielles&nbsp;&raquo;, c.-à-d. dont PATSTAT n'a pas la trace en tant que telles mais pour lesquelles il existe une priorité.

Application kind-codes D and Q : Application kind-codes 'D' and 'Q' identify "dummy" applications. Distinction between 'D' and 'Q' is made to help identify the corrective action required : Issues involving application kind-code 'D' can be resolved by an automated back-file correction exercise. Issues involving application kind-code 'Q' need intellectual effort and are being tackled manually, one by one.

Application kind-codes K, L, M, N, O. A limited number of countries, e.g. MC PH RU SU, supply identical application-identifications for separate publications. In order to resolve that issue, kind-code 'K' is allocated to the first duplicate encountered for a given application-identification, 'L' to the second etc.

French applications with kind-codes E, F, M : The kind-code of these French applications should be 'A'.Applications with kind-code E, F or M have been loaded with an incorrect kind-code at the time.

#### appln_filing_date et appln_filing_year : 
Date de la demande

#### appln_nr_epodoc : 
Numéro de demande EPODOC de l'Office européen des brevets &mdash; &laquo;&nbsp;deprecated&nbsp;&raquo; &mdash; sera supprimé dans une des prochaines éditions de PATSTAT. Je propose d'utiliser la clef alternative indiquée par le manuel PATSTAT pour avoir un identifiant unique et stable key_appln_nr : la concaténation d'appln_auth, appln_nr, appln_kind et receiving_office

#### appln_nr_original : 
Numéro de demande originel

#### ipr_type : 
Domaine de propriété intellectuelle couvert par la demande (brevet, modèle ou certificat d'utilité).

#### receiving_office : 
Bureau où la demande internationale a été effectuée - vide si demande nationale ou régionale.

#### internat_appln_id : 
Numéro d'identification de la demande de brevet de la procédure Patent Cooperation Treaty (PCT). Si le numéro est égal à zéro, pas de demande PCT préalable.

#### int, nat, reg_phase :
Ces variables disent est-ce que la demande est ou a été dans cette phase - vide pour les demandes pour lesquelles l'info n'est pas connue
Routes possibles d'une demande : <br>


| International phase | Regional phase | National phase |
|---------------------|----------------|----------------|
| Y/N/N               | Y/Y/N          | Y/Y/Y          |
|                     |                | Y/N/Y          |
|                     | N/Y/N          | N/Y/Y          |
|                     |                | N/N/Y          |
<br>

Les combinaisons disponibles d'int, nat, reg_phase dans patent_scope printemps 2021 sont : <br>


| International phase | Regional phase | National phase | Nombre |
|---------------------|----------------|----------------|--------|
| N                   |                | Y              | 129    |
| N                   | N              | Y              | 202754 |
| N                   | Y              | N              | 36912  |
| N                   | Y              | Y              | 11911  |
| Y                   |                | Y              | 211    |
| Y                   | N              | N              | 86681  |
| Y                   | N              | Y              | 173951 |
| Y                   | Y              | N              | 54650  |
| Y                   | Y              | Y              | 14534  |


<br>

#### earliest_filing_date :
1ère date de demande : date la plus ancienne parmi toutes les possibilités possibles (demande nationale, internationale, priorité convention de Paris,...)

Pour patent_scope printemps 2021, les dates vont du 22 janvier 1999 au 18 décembre 2020.

#### earliest_filing_year :
Année de la 1ère demande (mêmes conditions qu'earliest_filing_date)

#### earliest_filing_id :
ID de la 1ère demande : ID la plus ancienne parmi toutes les possibilités possibles (demande nationale, internationale, priorité convention de Paris,...)

Il y 170 647 ID uniques pour la table patent_scope printemps 2021.

#### earliest_publn_date :
1ère date de publication de la demande &mdash; les demandes de la famille antérieures ne sont pas prises en compte dans ce cas

Pour patent_scope printemps 2021, les dates vont du 17 février 2010 au 28 janvier 2021.

#### earliest_publn_year :
Année de la 1ère publication (mêmes conditions qu'earliest_publn_date)

#### earliest_pat_publn_id :
ID de la 1ère publication (mêmes conditions qu'earliest_publn_date)

#### granted :
Y si la demande a été octroyée et N si elle ne l'a pas été. N signifie exactement qu'il n'y a <U>AUCUNE</U> indication dans les données disponibles que la demande a été octroyée.

Dans patent_scope, il y a 273 237 Y (environ 47 %) et 308 496 N (53 %).

#### docdb_family_id :
ID de la famille DOCDB qui est une variable créée automatiquement par l'OEB : une famille DOCDB signifie que les différentes demandes partagent exactement les mêmes priorités

Pour patent_scope printemps 2021, il y a 173 410 ID uniques.

#### inpadoc_family_id :
ID de la famille INPADOC : variable qui indique que la demande partage une priorité directe ou via une demande tierce. Chaque application appartient à une et une seule famille INPADOC.

Pour patent_scope printemps 2021, il y a 167 708 ID uniques.

#### docdb_family_size :
Nombre de demandes au sein de chaque famille DOCDB.

La taille des familles va de 1 à 242. La taille médiane est de 5, la moyenne de 7 et le mode de 2. Plus de 85 % des enregistrements ont une taille de famille comprise entre 1 et 10 inclus.

#### nb_citing_docdb_fam :
Nombre de citations par une famille DOCDB d'au moins une publication ou demande de la famille DOCDB de la demande actuelle.

Le nombre de citations va de 0 à 768. La médiane est de 1, la moyenne de 4,876 et le mode de 0. Plus de 90 % des demandes ont moins de 11 citations.

#### nb_applicants :
Nombre de demandeurs pour chaque demande selon la publication la plus récente qui contient des noms de personnes en caractères latins (si pas de caractères, le nombre de demandeurs est fixé à zéro &mdash; ça ne veut pas dire qu'il n'y a pas de demandeurs).

Le nombre de demandeurs va de 0 à 30. La médiane est de 1, la moyenne de 1,391 et le mode de 1. Plus de 98 % des demandes ont moins de 6 demandeurs et 3,54 % ont &laquo;&nbsp;zéro&nbsp;&raquo; demandeur.

#### nb_inventors :
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

| code ISO langues | langues     | occurences | pourcentage |
|------------------|-------------|------------|-------------|
| en               | anglais     | 401076     | 69,24       |
| fr               | français    | 111173     | 19,19       |
| ja               | japonais    | 19493      | 3,37        |
| es               | espagnol    | 16865      | 2,91        |
| pt               | portugais   | 13258      | 2,29        |
| ko               | coréen      | 4556       | 0,79        |
| de               | allemand    | 3641       | 0,63        |
| da               | danois      | 3194       | 0,55        |
| ru               | russe       | 2851       | 0,49        |
| zh               | chinois     | 1205       | 0,21        |
| tr               | turc        | 673        | 0,12        |
| it               | italien     | 321        | 0,06        |
| el               | grec        | 300        | 0,05        |
| no               | norvégien   | 226        | 0,04        |
| uk               | ukrainien   | 199        | 0,03        |
| nl               | néerlandais | 59         | 0,01        |
| sv               | suédois     | 57         | 0,01        |
| ar               | arabe       | 56         | 0,01        |
| fi               | finnois     | 6          | 0,00        |
| pl               | polonais    | 3          | 0,00        |
| lt               | lituanien   | 2          | 0,00        |
| et               | estonien    | 1          | 0,00        |
| hr               | croate      | 1          | 0,00        |
| he               | hébreu      | 1          | 0,00        |


### Dans abstracts :
Ce fichier est issu de la table 203 et contient 3 variables :
<ul>
<li>appln_id</li>
<li>appln_title_lg</li>
<li>appln_title</li>
</ul>

Il y a des valeurs manquantes dans appln_abstract - 3 dans le cas du jeu de données PATSTAT Spring 2021 (appln_id :
446022198, 503487228 et 503602912). Comme pour les titres, l'anglais est la langue par défaut : les autres langues
n'apparaissent que si un résumé en anglais n'est pas disponible. Langues des titres :

| code ISO langues | langues     | occurences | pourcentage |
|------------------|-------------|------------|-------------|
| en               | anglais     | 344927     | 63,60       |
| fr               | français    | 133354     | 24,59       |
| ja               | japonais    | 15936      | 2,94        |
| es               | espagnol    | 15634      | 2,88        |
| ko               | coréen      | 12264      | 2,26        |
| pt               | portugais   | 11413      | 2,10        |
| de               | allemand    | 4074       | 0,75        |
| ru               | russe       | 1713       | 0,32        |
| zh               | chinois     | 1206       | 0,22        |
| tr               | turc        | 673        | 0,12        |
| uk               | ukrainien   | 321        | 0,06        |
| el               | grec        | 299        | 0,06        |
| no               | norvégien   | 241        | 0,04        |
| sr               | serbe       | 109        | 0,02        |
| ar               | arabe       | 77         | 0,01        |
| pl               | polonais    | 49         | 0,01        |
| nl               | néerlandais | 39         | 0,01        |
| sv               | sudéois     | 13         | 0,00        |
| da               | danois      | 11         | 0,00        |
| ro               | roumain     | 9          | 0,00        |
| hr               | croate      | 3          | 0,00        |
| cs               | tchèque     | 2          | 0,00        |
| me               | monténégrin | 2          | 0,00        |
| fi               | finnois     | 1          | 0,00        |
| it               | italien     | 1          | 0,00        |
| lv               | letton      | 1          | 0,00        |

### Dans publications :
Ce fichier est issu de la table 211 et contient 10 variables :
<ul>
<li>pat_publn_id </li>
<li>publn_auth (autorité responsable de la publication)</li>
<li>publn_nr (numéro de publication)</li>
<li>publn_nr_original</li>
<li>publn_kind (type de publication)</li>
<li>appln_id (ID de la demande)</li>
<li>publn_date (date de publication)</li>
<li>publn_lg (langue de publication)</li>
<li>publn_first_grant (est-ce la 1ère publication ou non d'un brevet octroyé&nbsp;?)</li>
<li>publn_claims (nombre de revendications)</li>
</ul>

Seuls les identifiants des demandes &laquo;&nbsp;françaises&nbsp;&raquo; (appln_id) et ceux qui correspondent à la 1ère date de publication sont conservées (earliest_pat_publn_id = pat_publn_id).

#### Autorités de publication (publn_auth) :

Près de 67&nbsp;% des publications sont effectuées auprès de l'INPI, de l'Office européen des brevets, de USPTO et de WIPO.

Il y a un gros écart entre la part de l'autorité chinoise pour la publication et la part du chinois dans les langues des titres et résumés.

| publn_auth | Pays ou organisation                                 | occurences | pourcentage |
|------------|------------------------------------------------------|------------|-------------|
| FR         | France                                               | 214092     | 25,52       |
| EP         | Office européen des brevets                          | 138140     | 16,46       |
| US         | &Eacute;tats-Unis                                    | 122150     | 14,56       |
| WO         | Organisation mondiale de la propriété intellectuelle | 94595      | 11,27       |
| CN         | Chine                                                | 66978      | 7,98        |
| JP         | Japon                                                | 39449      | 4,70        |
| KR         | République de Corée                                  | 22899      | 2,73        |
| CA         | Canada                                               | 21205      | 2,53        |
| BR         | Brésil                                               | 15486      | 1,85        |
| RU         | Russie                                               | 15427      | 1,84        |
| AU         | Australie                                            | 12996      | 1,55        |
| ES         | Espagne                                              | 12308      | 1,47        |
| MX         | Mexique                                              | 7530       | 0,90        |
| TW         | Taiwan                                               | 5738       | 0,68        |
| GB         | Royaume-Uni                                          | 5315       | 0,63        |
| PL         | Pologne                                              | 5077       | 0,61        |
| DE         | Allemagne                                            | 4001       | 0,48        |
| IL         | Israël                                               | 3387       | 0,40        |
| DK         | Danemark                                             | 3309       | 0,39        |
| SG         | Singapour                                            | 3094       | 0,37        |
| EA         | Organisation eurasienne des brevets                  | 2583       | 0,31        |
| PT         | Portugal                                             | 2501       | 0,30        |
| AR         | Argentine                                            | 2019       | 0,24        |
| ZA         | Afrique du Sud                                       | 1915       | 0,23        |
| HK         | Hong Kong                                            | 1571       | 0,19        |
| HU         | Hongrie                                              | 1267       | 0,15        |
| IN         | Inde                                                 | 1081       | 0,13        |
| MA         | Maroc                                                | 966        | 0,12        |
| CL         | Chili                                                | 999        | 0,12        |
| NO         | Norvège                                              | 751        | 0,09        |
| SI         | Slovénie                                             | 758        | 0,09        |
| NZ         | Nouvelle-Zélande                                     | 694        | 0,08        |
| HR         | Croatie                                              | 687        | 0,08        |
| TR         | Turquie                                              | 674        | 0,08        |
| UA         | Ukraine                                              | 630        | 0,08        |
| CO         | Colombie                                             | 579        | 0,07        |
| PH         | Philippines                                          | 573        | 0,07        |
| RS         | Serbie                                               | 485        | 0,06        |
| TN         | Tunisie                                              | 476        | 0,06        |
| PE         | Pérou                                                | 387        | 0,05        |
| LT         | Lituanie                                             | 430        | 0,05        |
| CH         | Suisse                                               | 326        | 0,04        |
| BE         | Belgique                                             | 294        | 0,04        |
| IT         | Italie                                               | 294        | 0,04        |
| UY         | Uruguay                                              | 320        | 0,04        |
| MY         | Malaisie                                             | 332        | 0,04        |
| CY         | Chypre                                               | 300        | 0,04        |
| AP         | ARIPO &mdash; African Regional Property Organization | 246        | 0,03        |
| CR         | Costa Rica                                           | 235        | 0,03        |
| NL         | Pays-Bas                                             | 130        | 0,02        |
| SE         | Suède                                                | 135        | 0,02        |
| CU         | Cuba                                                 | 161        | 0,02        |
| EC         | &Eacute;quateur                                      | 190        | 0,02        |
| AT         | Autriche                                             | 68         | 0,01        |
| LU         | Luxembourg                                           | 56         | 0,01        |
| DO         | République dominicaine                               | 106        | 0,01        |
| MD         | Moldavie                                             | 120        | 0,01        |
| GE         | Géorgie                                              | 70         | 0,01        |
| SM         | République de Saint-Marin                            | 46         | 0,01        |
| GT         | Guatemala                                            | 94         | 0,01        |
| JO         | Jordanie                                             | 43         | 0,01        |
| ME         | Montenegro                                           | 54         | 0,01        |
| NI         | Nicaragua                                            | 79         | 0,01        |
| CZ         | République tchèque                                   | 10         | 0,00        |
| SK         | Slovaquie                                            | 3          | 0,00        |
| EE         | Estonie                                              | 7          | 0,00        |
| FI         | Finlande                                             | 18         | 0,00        |
| GR         | Grèce                                                | 7          | 0,00        |
| IE         | République d'Irlande                                 | 8          | 0,00        |
| RO         | Roumanie                                             | 31         | 0,00        |
| SV         | Salvador                                             | 27         | 0,00        |
| HN         | Honduras                                             | 6          | 0,00        |
| EG         | Egypte                                               | 14         | 0,00        |
| MC         | Monaco                                               | 17         | 0,00        |
| BG         | Bulgarie                                             | 6          | 0,00        |
| SA         | Arabie saoudite                                      | 16         | 0,00        |
| LV         | Lettonie                                             | 1          | 0,00        |
| ID         | Indonésie                                            | 1          | 0,00        |

#### Numéro de publication (publn_nr) :
Numéro donné par l'autorité de publication. Il n'est pas unique dans 179&nbsp;951 cas (environ 21&nbsp;%).

#### Numéro de publication au format originel (publn_nr_original) :
En moyenne, environ 20&nbsp;% de toutes les publications ont un numéro au format originel selon le dictionnaire de données de PATSTAT.

Dans le cas des demandes &laquo;&nbsp;françaises&nbsp;&raquo;&nbsp;:
<ul>
<li>7,77&nbsp;% n'ont pas de numéro,</li>
<li>39,71&nbsp;% n'ont pas un numéro unique et</li>
<li>52,52 ont un numéro unique.</li>
</ul>
 

#### Type de publication (publn_kind) :
Type de publication tel qu'attribué par l'autorité de publication&nbsp;: leur signification dépend de l'autorité.

| pubn_kind | occurences | pourcentage |
|-----------|------------|-------------|
| A1        | 404189     | 48,17       |
| B1        | 141702     | 16,89       |
| A         | 113397     | 13,51       |
| B2        | 62363      | 7,43        |
| B         | 27530      | 3,28        |
| A2        | 26431      | 3,15        |
| T3        | 20103      | 2,40        |
| A3        | 10726      | 1,28        |
| C2        | 5662       | 0,67        |
| D0        | 4548       | 0,54        |
| C         | 4350       | 0,52        |
| A4        | 3111       | 0,37        |
| T         | 2405       | 0,29        |
| U         | 1889       | 0,23        |
| T1        | 1847       | 0,22        |
| A8        | 1552       | 0,18        |
| T2        | 1255       | 0,15        |
| C1        | 1013       | 0,12        |
| T4        | 714        | 0,09        |
| A9        | 650        | 0,08        |
| T5        | 624        | 0,07        |
| B8        | 618        | 0,07        |
| U1        | 549        | 0,07        |
| E         | 531        | 0,06        |
| B4        | 248        | 0,03        |
| A0        | 199        | 0,02        |
| B3        | 187        | 0,02        |
| B9        | 144        | 0,02        |
| A7        | 108        | 0,01        |
| R1        | 90         | 0,01        |
| A5        | 78         | 0,01        |
| C9        | 52         | 0,01        |
| T8        | 45         | 0,01        |
| U2        | 38         | 0,00        |
| Y         | 33         | 0,00        |
| S         | 17         | 0,00        |
| B6        | 17         | 0,00        |
| Y1        | 17         | 0,00        |
| T9        | 9          | 0,00        |
| I1        | 6          | 0,00        |
| U3        | 3          | 0,00        |
| T7        | 3          | 0,00        |
| C8        | 3          | 0,00        |
| U9        | 2          | 0,00        |
| T6        | 2          | 0,00        |
| U8        | 2          | 0,00        |
| B5        | 2          | 0,00        |
| U0        | 1          | 0,00        |
| R2        | 1          | 0,00        |
| W         | 1          | 0,00        |
| C5        | 1          | 0,00        |
| Y3        | 1          | 0,00        |
| E2        | 1          | 0,00        |
| Y8        | 1          | 0,00        |
| U5        | 1          | 0,00        |
| Y4        | 1          | 0,00        |

#### Langue de publication (publn_lg) :

Le français et l'anglais sont les langues les plus fréquentes et représentent 74&nbsp;% des langues de publication.
L'allemand, qui est la 3ème langue officielle des brevets européens, arrive bien derrière avec 1&nbsp;%.
Le chinois, à près de 8&nbsp;%, est la 3ème langue la plus utilisée.

| langues            | publn_lg | occurences | pourcentage |
|--------------------|----------|------------|-------------|
| français           | fr       | 370600     | 44,17       |
| anglais            | en       | 252323     | 30,07       |
| chinois            | zh       | 66932      | 7,98        |
| japonais           | ja       | 32600      | 3,89        |
| espagnol           | es       | 25064      | 2,99        |
| coréen             | ko       | 22942      | 2,73        |
| russe              | ru       | 18171      | 2,17        |
| portugais          | pt       | 17513      | 2,09        |
| valeurs manquantes |          | 10147      | 1,21        |
| allemand           | de       | 8407       | 1,00        |
| polonais           | pl       | 5077       | 0,61        |
| danois             | da       | 3296       | 0,39        |
| hongrois           | hu       | 1253       | 0,15        |
| slovène            | sl       | 758        | 0,09        |
| croate             | hr       | 686        | 0,08        |
| turc               | tr       | 669        | 0,08        |
| ukrainien          | uk       | 520        | 0,06        |
| lituanien          | lt       | 430        | 0,05        |
| serbe              | sr       | 376        | 0,04        |
| italien            | it       | 312        | 0,04        |
| grec               | el       | 307        | 0,04        |
| norvégien          | no       | 182        | 0,02        |
| roumain            | ro       | 137        | 0,02        |
| suédois            | sv       | 122        | 0,01        |
| néerlandais        | nl       | 93         | 0,01        |
| monténégrin        | me       | 54         | 0,01        |
| arabe              | ar       | 53         | 0,01        |
| moldave            | mo       | 14         | 0,00        |
| finnois            | fi       | 12         | 0,00        |
| tchèque            | cs       | 10         | 0,00        |
| bulgare            | bg       | 6          | 0,00        |
| estonien           | et       | 3          | 0,00        |
| slovaque           | sk       | 2          | 0,00        |
| letton             | lv       | 1          | 0,00        |
| indonésien         | id       | 1          | 0,00        |

#### Publication du 1er octroi (publn_frist_grant) :

68&nbsp;% des cas ne correspondent pas à publication du 1er octroi et 32&nbsp;% le sont.

#### Nombre de revendications (publn_claims) :

Environ 85&nbsp;% des publications n'ont aucune revendication. Le nombre maximal de revendications est de 177.
La médiane et le mode sont de zéro. La moyenne est de 2,014.

### Dans patent :
Patent a 42 variables. Le fichier reprend les variables de patent_scope auxquelles s'ajoutent&nbsp;:
<ul>
<li>key_appln_nr</li>
<li>oeb</li>
<li>international</li>
<li>appln_publn_id</li>
<li>appln_publn_number</li>
<li>appln_publn_date</li>
<li>publn_kind</li>
<li>grant_publn_id</li>
<li>grant_publn_number</li>
<li>grant_publn_date</li>
<li>ispriority</li>
<li>appln_title_lg</li>
<li>appln_title</li>
<li>appln_abstract_lg</li>
<li>appln_abstract</li>
</ul>

Patent est le résultat de la jointure (sur appln_id) des données sur la 1ère publication, le 1er octroi, la présence de priorités, les titres et les résumés (cf. Dans titles et Dans abstracts).

#### reg_phase, nat_phase, oeb et international :
Reg_phase et nat_phase sont transformés en variables &laquo;&nbsp;dummy&nbsp;&raquo; avec 0 pour N et 1 pour les autres cas&nbsp;: les valeurs manquantes représentées par une chaîne de caractères vides prennent donc une valeur de 1.
OEB est une variable &laquo;&nbsp;dummy&nbsp;&raquo; avec 1 s'il y a à la fois une valeur de 1 dans reg_phase et EP dans appln_auth sinon 0. Les valeurs manquantes prennent une valeur de 0.
International reprend la variable int_phase en la transformant en variable &laquo;&nbsp;dummy&nbsp;&raquo; avec 1 pour Y et 0 pour les autres valeurs&nbsp;: les valeurs manquantes prennent donc une valeur de 0.

Pour la variable oeb, il y a 15,44&nbsp;% de demandes auprès de l'Office européen des brevets.

#### 1ère publication
Suite à une jointure entre patent_scope (earliest_pat_publn_id) et publications (pat_publn_id), on obtient la 1ère date de publication pour chaque demande. On garde les variables publn_nr, publn_date et pat_publn_id qui sont renommées appln_publn_number, appln_publn_date et appln_publn_id.

#### 1er octroi
Suite à une jointure sur les ID de demandes entre patent_scope et publications (pour les enregistrements qui ont une valeur de Y à la variable publn_first_grant), on obtient la 1ère date d'octroi. On garde publn_nr, publn_date, et pat_publn_id qui sont renommées grant_publn_number, grant_publn_date et grant_publn_id.

#### Présence de priorités
Les informations sur les priorités sont présentes dans la table 204 : seuls les ID de demandes présents à la fois dans patent_scope et la table 204 sont chargés. Ensuite, on crée une variable &laquo;&nbsp;dummy&nbsp;&raquo; indiquant s'il y a une priorité ou non dans la famille de brevets.
17&nbsp;% des demandes ont des priorités.

### Dans families :
Le fichier reprend les données de patent, mais les présente du point de la famille du brevet (docdb family ID).
Les langues sont classées selon un ordre de priorité pour titles et abstracts (allemand, puis langues romanes, anglais-français et ensuite les autres langues du monde).

### Dans families_technologies :
&Agrave; chaque DOCDB family ID est associé les sections, classes, sous-classes et groupes, codes et libellés issus des catégories CPC. Les catégories CPC (classification coopérative des brevets) est une extension de la Classification internationale des brevets (CIB) et est gérée conjointement par l'Office européen des brevets et l'Office des brevets et des marques des &Eacute;tats-Unis.
Elle est divisée en neuf sections, A-H et Y, à leur tour subdivisées en classes, sous-classes, groupes et sous-groupes. La CPC comporte environ 250 000 entrées de classification.

Les neuf sections de la CPC

| Section | Description                                                                                                                                                                                                                                                 |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| A       | Nécessités courantes de la vie                                                                                                                                                                                                                              |
| B       | Techniques industrielles diverses; transports                                                                                                                                                                                                               |
| C       | Chimie; métallurgie                                                                                                                                                                                                                                         |
| D       | Textiles; papier                                                                                                                                                                                                                                            |
| E       | Constructions fixes                                                                                                                                                                                                                                         |
| F       | Mécanique; éclairage; chauffage; armement; sautage                                                                                                                                                                                                          |
| G       | Physique                                                                                                                                                                                                                                                    |
| H       | Electricité                                                                                                                                                                                                                                                 |
| Y       | Regroupe les nouveaux développements technologiques; rassemble les technologies qui participent de plusieurs sections issues de diverses sections de la CIB ; concepts techniques couverts par d'anciens recueils de références croisées de l'USPC [XRACs]. |

La CPC s'étend constamment à mesure que de nouveaux domaines techniques apparaissent.

Des informations plus détaillées et une vue d'ensemble complète de la CPC sont disponibles sur le site https://www.cooperativepatentclassification.org/index

### Dans part_init
La table part_init originelle est le résultat du programme recuperation. Elle est ensuite actualisée de manière itérative.

Part_init reprend les informations de patent et les met en relation avec les informations sur les personnes. L'identifiant dans cette table est constitué de key_appln_nr et de person_id.
Les mêmes personnes, si elles ont fait plusieurs demandes de brevets, peuvent revenir plusieurs fois. De même, si une demande de brevet est présentée plusieurs par plusieurs personnes, elle peut revenir plusieurs fois.

Part_init contient 36 colonnes&nbsp;:
<ul>
<li>id_participant</li>
<li>id_patent</li>
<li>person_id</li>
<li>docdb_family_id</li>
<li>inpadoc_family_id</li>
<li>applt_seq_nr</li>
<li>doc_std_name</li>
<li>doc_std_name_id</li>
<li>earliest_filing_date</li>
<li>invt_seq_nr</li>
<li>name_source</li>
<li>address_source</li>
<li>country_source</li>
<li>psn_sector</li>
<li>psn_id</li>
<li>psn_name</li>
<li>publication_number</li>
<li>appln_auth</li>
<li>appln_id</li>
<li>appln_nr</li>
<li>appln_kind</li>
<li>receiving_office</li>
<li>key_appln_nr_person</li>
<li>key_appln_nr</li>
<li>old_name</li>
<li>country_corrected</li>
<li>siren</li>
<li>siret</li>
<li>id_paysage</li>
<li>rnsr</li>
<li>grid</li>
<li>sexe</li>
<li>id_personne</li>
<li>type</li>
<li>isascii</li>
<li>name_clean</li>
</ul>

Id_participant est la concaténation d'appln_nr_epodoc et de person_id, et key_appln_nr_person de key_appln_nr et de person_id.
Les variables appln_nr_epodoc, person_name, person_address, person_ctry_code et appln_publn_number sont renommées en id_patent, name_source, address_source, country_source et publication_number.

L'identification précise des personnes, morales ou physiques, est compliquée. Dans la version PATSTAT Spring 2021, on obtient 845&nbsp;030 person_id différents, 303&nbsp;904 doc_std_name_id (identifiants associés aux noms standardisés selon la procédure DOCDB) et 407&nbsp;564 psn_id (identifiants associés aux noms standardisés selon la procédure développée par ECOOM - K.U. Leuven).
De même, il est difficile de déterminer la personnalité juridique de chaque personne. Seules les personnes physiques peuvent être des inventeurs. Le manuel de PATSTAT indique que si invt_seq_nr est supérieur à 0, alors on a affaire à un inventeur. Or, il existe des cas où une personne morale a une valeur supérieure à 0 et, inversement, des cas où une personne physique à une valeur égale à 0.
Psn_sector, information issue du travail d'ECOOM - K.U. Leuven, sert de base pour identifier les personnes physiques et morales. Si psn_sector vaut "INDIVIDUAL" alors le type est personne physique. Pour toutes les autres valeurs non nulles, le type est personne morale. Dans certains cas, une personne identifiée par son doc_sd_name_id a plusieurs personnalités. Dans ce cas, on fait la somme d'invt_seq_nr. Si elle est supérieure à 0, alors il s'agit d'une personne physique.
Psn_sector est rempli à environ 48&nbsp;%. Pour les valeurs manquantes, on utilise un modèle de classification statistique s'appuyant sur les noms (name_source et doc_std_name). La part des personnes physiques est plus importante que celle des personnes morales dans la variable psn_sector (55&nbsp;% de pp et 45&nbsp;% de pm). L'algorithme de classification avait des difficultés à identifier les personnes morales. Il est donc nécessaire de suréchantillonner les personnes morales.

La variable isascii indique si les noms sont en caractères latins ou non. Seuls ceux en caractères latins seront conservés pour les traitements futurs (probabilité d'un genre et recherche d'un SIREN).

### Dans partfin et old_part_key
Partfin et old_part_key sont similaires à part_init mais ne contient pas doc_std_name, doc_std_name_id, psn_sector, psn_id, psn_name, publication_number, old_name et name_clean (et sans key_appln_nr pour partfin originel).

### Dans part
Part est similaire à part_init mais ne contient pas les variables applt_seq_nr et invt_seq_nr.

### Dans part_ind et part_entp
Part_ind et part_entp sont créés à partir de part_init. Part_ind ne contient que les enregistrements concernant les personnes physiques, et part_entp ceux concernant les personnes morales. Part_entp contient une variable supplémentaire : name_corrected.

### Dans sex_table
Sex_table contient 5 variables :
<ul>
<li>name</li>
<li>sex</li>
<li>proba</li>
<li>occurence</li>
<li>error</li>
</ul>

Name correspond à name_source nettoyé et mis en forme.

### Dans part_individuals
Part_individuals correspond à part_ind avec les noms et pays corrigés, ainsi que le genre.

### Dans part_ent_final


### Dans siren_inpi_brevet et siren_inpi_generale
Siren_inpi_brevet contient 3 variables&nbsp;:
<ul>
<li>numpubli</li>
<li>nom</li>
<li>siren</li>
</ul>
Siren_inpi_generale ne contient pas la variable numpubli (numéro de publication du brevet).

### Dans idext


# Dans role


