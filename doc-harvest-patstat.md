# Documentation des traitements de la base de données PATSTAT

## Organisation des programmes
<ol>
<li>collectePatstatComplete : requête la base de données PATSTAT et télécharge tous les fichiers zippés de la base</li>
<li>dezippage : dézippe tous les fichiers zippés de Patstat et range les fichiers CSV issus du dézippage dans le dossier correspondant à leur table PATSTAT</li>
<li>csv_files_querying : permet de charger les CSV issus de PATSTAT dans Python par morceau, tout en appliquant des filtres - permet de charger les fichiers et de s'en servir en tant que dataframe</li>
<li>dtypes_patstat_declaration : type des données des fichiers CSV issus de PATSTAT - facilite la création des dataframes</li>
<li>p01_family_scope : limite la sélection aux demandes françaises. Inputs = tls 201, 206 et 207. Outputs = family_scope et patent_scope</li>
<li>p02_titles_abstracts : extrait les titres et les résumés des demandes. Inputs = patent_scope, tls 202 et 203. Outputs = titles et abstracts</li>
<li>p03_patents : </li>
</ol>


## Notes

### Enregistrements fichiers
<ul>
<li>581 733 enregistrements pour patent_scope</li>
<li>579 217 enregistrements pour titles</li>
<li>542 372 enregistrements pour abstracts</li>
<li>Manque des titres et des résumés pour les demandes. Ce ne sont pas les demandes artificielles car il n'y en a que 2 (appln_kind "D")</li>
</ul>


### Dans patent_scope :
<ul>
<li>valeurs de appln_kind = "A", "C", "D", "F", "K", "L", "T", "U", "W".</li>
<li>"D" est pour les demandes &laquo; artifielles &raquo;, càd dont PATSTAT n'a pas la trace en tant que telles mais pour lesqeulles il existe une priorité</li>
<li>Application kind-codes D and Q : Application kind-codes 'D' and 'Q' identify "dummy" applications. Distinction between 'D' and 'Q' is made to help identify the corrective action required : Issues involving application kind-code 'D' can be resolved by an automated back-file correction exercise. Issues involving application kind-code 'Q' need intellectual effort and are being tackled manually, one by one.</li>
<li>Application kind-codes K, L, M, N, OA limited number of countries, e.g. MC PH RU SU, supply identical application-identifications for separate publications. In order to resolve that issue, kind-code 'K' is allocated to the first duplicate encountered for a given application-identification, 'L' to the second etc.</li>
<li>French applications with kind-codes E, F, M : The kind-code of these French applications should be 'A'.Applications with kind-code E, F or M have been loaded with an incorrect kind-code at the time.</li>
<li>appln_nr_original vide si demande artificielle ou si pas trouvée dans DOCDB</li>
<li>receiving_office : bureau où la demande internationale a été effectuée - vide si demande nationale ou régionale</li>
<li>int, nat, reg_phase disent est-ce que la demande est ou a été dans cette phase - vide pour les demandes pour lesquelles l'info n'est pas connue
Routes possibles d'une demande : <br>

------------------------------------------------------
|International phase | Regional phase| National phase|
|--------------------|---------------|---------------|
| Y/N/N              | Y/Y/N         | Y/Y/Y         |
|                    |               | Y/N/Y         |
|                    | N/Y/N         | N/Y/Y         |
|                    |               | N/N/Y         |
------------------------------------------------------
<br>
Les combinaisons disponibles dans patent_scope sont : <br>

-------------------------------------------------------------
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
--------------------------------------------------------------
</li>
</ul>

### Dans abstracts :
Il y a des valeurs manquantes dans appln_abstract - 3 dans le cas du jeu de données PATSTAT Spring 2021 (appln_id : 446022198, 503487228 et 503602912).
Comme pour les titres, l'anglais est la langue par défaut : les autres langues n'apparaissent que si un résumé en anglais n'est pas disponible.

--------------------------------
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

### Dans titles :
Aucune valeur manquante. 69,24 % des titres en anglais et 19,19 % en français. Anglais comme langue par défaut : les titres dans les autres langues ne sont considérés que s'il n'existe pas de titre en anglais.
Langues des titres :
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

-------------------
|langues|occurences|
|-------|----------|
|en     |401076    |
|fr     |111173    |
|ja     |19493     |
|es     |16865     |
|pt     |13258     |
|ko     |4556      |
|de     |3641      |
|da     |3194      |
|ru     |2851      |
|zh     |1205      |
|tr     |673       |
|it     |321       |
|el     |300       |
|no     |226       |
|uk     |199       |
|nl     |59        |
|sv     |57        |
|ar     |56        |
|fi     |6         |
|pl     |3         |
|lt     |2         |
|et     |1         |
|hr     |1         |
|he     |1         |
--------------------