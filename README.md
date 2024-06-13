# Extraire des consommations de soin du SNDS avec R

🚧 Projet en cours de développement.

## Description

Ce package R permet d'extraire des données de consommation de soins du SNDS (Système National des Données de Santé) pour une population donnée. Il permet de réaliser des analyses sur les consommations de soins des bénéficiaires de l'assurance maladie.

## Contexte technique

### Données 

Les données sont issues du SNDS (Système National des Données de Santé) et sont hébergées sur le [portail de l'assurance maladie (CNAM)](https://portail.sniiram.ameli.fr/). Pour mener utiliser ce package, il est nécessaire d'avoir un accès aux [données individuelles bénéficiaires exhaustives de consommation de soins](https://documentation-snds.health-data-hub.fr/snds/formation_snds/documents_cnam/guides_pedagogiques_snds/guide_pedagogique_acces_permanents.html#qui-a-acces-au-snds-et-a-quelles-donnees). 

###  Schéma de flux de données

TODO (figure à mettre dans man/figures)

### Technologies

- Langage de programmation : R
- Packages utilisés : haven, here, dplyr, dbplyr, openxlsx, lubridate, feather

### Besoin de maintenance


## Liens utiles 

- **lien vers cette page de la documentation** : [https://has-sante.pages.has-sante.fr/deai/acces_precoces](https://has-sante.pages.has-sante.fr/deai/acces_precoces)

- **Dépôt du code source** : [https://gitlab.has-sante.fr/has-sante/deai/acces_precoces](https://gitlab.has-sante.fr/has-sante/deai/acces_precoces)

- **Description des flux et des données** : TODO [Prise en main](articles/data.html)

- **Faire tourner le script principal** : TODO [Prise en main](articles/rapac.html)

- **Documentation des fonctions utilisées** : [Référence](reference/index.html)
