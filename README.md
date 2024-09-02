[![codecov](https://codecov.io/gh/strayMat/sndsTools/graph/badge.svg?token=NKPHJFLAN8)](https://codecov.io/gh/strayMat/sndsTools)

# Extraction de recours aux soins sur le SNDS en R 

🚧 Projet en cours de développement.

## Description

Ce package R permet d'extraire des données de recours aux soins du SNDS (Système National des Données de Santé) pour une population donnée. 

## Contexte technique

### Données 

Les données sont issues du SNDS et sont hébergées sur le [portail de l'assurance maladie (CNAM)](https://portail.sniiram.ameli.fr/). Pour mener utiliser ce package, il est nécessaire d'avoir un accès aux [données individuelles bénéficiaires exhaustives de consommation de soins](https://documentation-snds.health-data-hub.fr/snds/formation_snds/documents_cnam/guides_pedagogiques_snds/guide_pedagogique_acces_permanents.html#qui-a-acces-au-snds-et-a-quelles-donnees). 

###  Schéma de flux de données

TODO (figure à mettre dans man/figures)

### Technologies

- Langage de programmation : R
- Packages utilisés : dplyr, dbplyr, DBI, glue, lubridate, progress


## Liens utiles 

- **lien vers cette page de la documentation** : [https://straymat.github.io/sndsTools/index.html](https://straymat.github.io/sndsTools/index.html)

- **Dépôt du code source** : [https://github.com/straymat/sndsTools](https://github.com/straymat/sndsTools)

- **Description des extractions et des tables de données créées** : TODO [Flux de données](articles/data.html)

- **Prise en main rapide** : [Prise en main](articles/sndsTools.html)

- **Documentation des fonctions utilisées** : [Référence](reference/index.html)

- **Guide de contribution** : TODO [Guide de contribution](articles/contributing.html)
