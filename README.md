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
|     +-- brevo
|     +-- siren
|     +-- batch
|     +-- ...
|
|   +-- internal
|     +-- gcs_seed
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

- Accès aux comptes de services GCP
- [Gcloud CLI](https://cloud.google.com/sdk/docs/install?hl=fr)
- Make installé
  - linux : `sudo apt install make`
  - mac : `brew install make`

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

- Installation du projet :

  - Pour les Data Analysts :

    ```bash
    make install_analytics
    ```

      > Cette installation est simplifiée pour les Data Analysts, afin de mettre tous les requirements nécecssaires à la DA dans un même venv. Elle installe également des **pre-commit** hooks pour le projet, ce qui permet de coder juste du premier coup.

  - Pour la team DE/DS

    - Pour les Data Scientists :

      ```bash
      make install_science
      ```

    - Pour les Data Engineers :

      ```bash
      make install_engineering
      ```

    > Ces commande créé différents sous-environnements virtuels pour les différents types de jobs spécifiés dans le fichier `Makefile`. Elle installe également des **pre-commit** hooks pour le projet, ce qui permet de coder juste du premier coup.

  - TROUBLESHOOTING : si l'environnment virtuel ne change pas au passage dans le dossier d'un microservice

    ```bash
    source deactivate
    ```

#### 2. Config .env.local

Dans le fichier `.env.local`, renseigne les valeurs des variables manquantes en utilisant [cette page](https://www.notion.so/passcultureapp/Les-secrets-du-repo-data-gcp-085759e27a664a95a65a6886831bde54)

## Orchestration

Les DAGs Airflow orchestrant les différents jobs DA/DE/DS sont détaillés dans [le README.md du dossier orchestration](/orchestration/README.md)

Les dags sont déployés automatiquement lors d'un merge sur master / production (voir [la doc de déploiement continu](.github/workflows/README.md))

## CI/CD

Pipelines détaillées dans le [README de github Action](.github/workflows/README.md)

## Automatisations

### ML Jobs

Pour créer un nouveau micro service de ML ou d'ETL, nous pouvons utiliser les 3 commandes suivantes :

- `MS_NAME=mon_micro_service make create_microservice_ml` :  Créé un micro service de ML dans le dossier `jobs/ml_jobs`
- `MS_NAME=mon_micro_service make create_microservice_etl_internal` :  Créé un micro service de ML dans le dossier `jobs/etl_jobs/internal`
- `MS_NAME=mon_micro_service make create_microservice_etl_external` :  Créé un micro service de ML dans le dossier `jobs/etl_jobs/external`

où mon_micro_service est le nom du micro service. Exemple :

```bash
MS_NAME=algo_llm make create_microservice_ml
```

Cela va :

1. créer un dossier `algo_llm` dans le dossier `jobs/ml_jobs` avec les fichiers nécessaires pour le micro service.
2. Commiter les changements
3. Lancer l'installation du nouveau micro service
