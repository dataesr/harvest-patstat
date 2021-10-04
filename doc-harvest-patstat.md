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

Dans patent_scope :
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

|--------------------|---------------|---------------|
|International phase | Regional phase| National phase|
|--------------------|---------------|---------------|
| Y/N/N              | Y/Y/N         | Y/Y/Y         |
|                    |---------------|---------------|
|                    |               | Y/N/Y         |
|--------------------|---------------|---------------|
|                    | N/Y/N         | N/Y/Y         |
|--------------------|---------------|---------------|
|                    |               | N/N/Y         |
|--------------------|---------------|---------------|
<br>
</li>
</ul>
