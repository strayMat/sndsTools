Le script `build.population.R` permet de récupérer les données démographiques correspondant à une liste d'identifiants extraits du PMSI ou du DCIR.
IL est librement adapté du repo Outils_SNDS de l'équipe DRUGS SAFER, initialement publié en SAS.

## Table d'entréee :
 - `tab_xtr_name`: Le nom d'une table présente sur Orauser contenant une colonne BEN_NIR_PSA qui rassemble les identifiants récupérés de la population d'intérêt
## Tables de sortie :
 - `{project_name}_{population_name}_DEM` : Une table sur Orauser qui rassemble les données démographiques de l'ensemble des patients extraits, une fois appliqués des critères de qualité sur les identifiants et sur les données démographiques
 - `{project_name}_{population_name}_IDS` : Une table sur Orauser qui rassemble les différents BEN_NIR_PSA pour l'ensemble des BEN_IDT_ANO des patients extraits, une fois appliqués des critères de qualité sur les identifiants et sur les données démographiques. C'est une table d'alias qui permet de faire la correspondance entre les identifiants "uniques" BEN_IDT_ANO utilisés dans `{project_name}_{population_name}_DEM` et les différents BEN_NIR_PSA correspondants apparaissant dans le DCIR et le PMSI

## Autres arguments d'entrée :
 - `project_name` et `population_name`: Des abbrévations de noms de projets et de population
 - `opt_nir` : Une option pour le filtre sur la variable BEN_CDI_NIR de la table des bénéficiaires
 - `build_ir_ben`: Un booléen qui indique si l'on doit reconstruire ou non la table de l'ensemble des bénéficiaires à partir de IR_BEN_R et IR_BEN_R_arc. Noter que `build.population.R` enregistre dans Orauser une table "REF_IR_BEN". Elle sera réutilisée si `build_ir_ben` == FALSE

## Autre données de sortie :
`build.population.R` utilise des print pour afficher les effectifs concernés par les étapes successives de nettoyage des données
