# data-gcp

Repo pour la team data sur GCP.

Ce repo contient les DAGs Airflow et les scripts nécessaires pour l'orchestration des jobs.

- Les DAGs sont dans `orchestration/dags/`
- Les scripts appelés dans les DAGs sont à mettre dans `jobs/`, et divisés en 2 catégories :
  - ETL jobs : pour l'extraction, la transformation et le chargement des données
  - ML jobs : pour les micro services de machine learning

## Organisation

```
+-- orchestration : DAGS Airflow (Cloud Composer)
| +-- airflow
| +-- dags
| +-- tests
|
+-- jobs
| +-- etl_jobs
|   +-- external 
|     +-- adage
|     +-- addresses
|     +-- appsflyer
|     +-- contentful
|     +-- dms
|     +-- downloads
|     +-- metabase-archiving
|     +-- qualtrics
|     +-- sendinblue
|     +-- siren
|     +-- batch
|     +-- ...
|
|   +-- internal
|     +-- cold-data
|     +-- human_ids
|     +-- import_api_referentials
|     +-- ...
|
| +-- ml_jobs
|   +-- algo_training
|   +-- embeddings
|   +-- record_linkage
|   +-- ranking_endpoint
|   +-- clusterisation
|   +-- retrieval_endpoint
|   +-- ...

```

## INSTALL

### Analytics (BigQuery)

#### 0. Prérequis

- [pyenv](https://github.com/pyenv/pyenv-installer)
  - ⚠ Don't forget to [install the prerequisites](https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites)
- [pyenv virtualenv](https://github.com/pyenv/pyenv-virtualenv#installation)
- Accès aux comptes de services GCP
- [Gcloud CLI](https://cloud.google.com/sdk/docs/install?hl=fr)

#### 1. Installation du projet

- Cloner le projet

  ```bash
  git clone git@github.com:pass-culture/data-gcp.git
  cd data-gcp
  ```

- [LINUX] Installation de quelques librairies nécessaires à l'install du projet

  ```bash
  make install_ubuntu_libs
  ```

- [VM DEBIAN] Installation d'autres librairies et fix de l'environnement pour les VM :

  ```bash
  make install_on_debian_vm
  ```

- Installation du projet
  - La première fois : installation from scratch, avec création des environnements virtuels

    ```bash
    make clean_install
    ```

  - Installation rapide des nouveaux packages

    ```bash
    make install
    ```

#### 2. Config .env.local

Dans le fichier `.env.local`, renseigne les valeurs des variables manquantes en utilisant [cette page](https://www.notion.so/passcultureapp/Les-secrets-du-repo-data-gcp-085759e27a664a95a65a6886831bde54)

## Orchestration

Orchestration des jobs dags analytics & data science.

[plus de détails dans dags/README.md](/orchestration/README.md)

Les dags sont déployés automatiquement lors d'un merge sur master / production

## CI/CD


On utilise Github Actions pour la CI et la CD.

### CI

La CI est déclenchée sur chaque push sur une branche, et permet de tester le code avant de merger une PR :

- *linter* : tester le bon formattage du code de tout le repo en utilisant `Black`
  TODO: Ajouter un linter tel que Ruff ou Pylint qui vérifie aussi la qualité du code
- *dbt-compile* : tester la compilation des modèles dbt
- *test-orchestration* : tester les différents DAGs
- *test-jobs* : tester les différents jobs (ETL ou ML)

### CD

La CD est déclenchée sur chaque merge sur la branche `master` / `production`, et permet de déployer les différents DAGs sur composer :

- *composer-deploy* : déployer le dossier `dags` dans le bucket du Cloud Composer sur Cloud Storage
  - Lorsque l'on merge sur `master`: le déploiement est automatique sur le Composer de **staging**
  - Lorsque l'on merge sur `production`: le déploiement est automatique sur le Composer de **production** et de **dev**

## Automatisations

### ML Jobs

Pour créer un nouveau micro service de ML :

```bash
MS_NAME=mon_micro_service make create_microservice
```

où mon_micro_service est le nom du micro service. Exemple :

```bash
MS_NAME=algo_llm make create_microservice
```

Cela va :

1. créer un dossier `algo_llm` dans `jobs/ml_jobs` avec les fichiers nécessaires pour le micro service.
2. rajouter le micro service dans la target install du Makefile
3. Commiter les changements
4. Lancer l'installation du nouveau micro service
