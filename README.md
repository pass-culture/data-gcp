# data-gcp

Repo pour la team data sur GCP

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

**Prérequis** :
- [pyenv](https://github.com/pyenv/pyenv-installer)
  - ⚠ Don't forget to [install the prerequisites](https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites)
- [pyenv virtualenv](https://github.com/pyenv/pyenv-virtualenv#installation)
- accès aux comptes de services GCP
- [Gcloud CLI](https://cloud.google.com/sdk/docs/install?hl=fr)

**1. Installation du projet**

- Cloner le projet
  ```
  git clone git@github.com:pass-culture/data-gcp.git
  cd data-gcp
  ```
- [LINUX] Installation de quelques librairies nécessaires à l'install du projet
  ```
  make install_ubuntu_libs
  ```
- [VM DEBIAN] Installation d'autres librairies et fix de l'environnement pour les VM :
  ```
  make install_on_debian_vm
  ```
- Installation du projet
  - La première fois : installation from scratch, avec création des environnements virtuels
    ```
    make clean_install
    ```
  - Installation rapide des nouveaux packages
    ```
    make install
    ```


**2. Config .env.local**

Dans le fichier `.env.local`, renseigne les valeurs des variables manquantes en utilisant [cette page](https://www.notion.so/passcultureapp/Les-secrets-du-repo-data-gcp-085759e27a664a95a65a6886831bde54)


## Orchestration

Orchestration des jobs dags analytics & data science.

[plus de détails dans dags/README.md](/orchestration/README.md)

Les dags sont déployés automatiquement lors d'un merge sur master / production


## CI/CD
### CI
On utilise CircleCI pour lancer des tests sur les différentes parties du repo.
Les tests sont lancés sur toutes les branches git et sont répartis entre les jobs suivants :
- *linter* : tester le bon formattage du code de tout le repo en utilisant `Black`
- *orchestration-tests* : tester les différents DAGs d'orchestration

### CD
Pour la CD, on utilise deux outils : CircleCI et Cloud Build.
#### CircleCI
Voici les jobs créés pour le déploiement :
- *vertex-ai-deploy* : déployer les modèles de ML via MLFlow dans Cloud Storage puis l'utiliser pour mettre à jour la version du modèle sur VertexAI
- *composer-deploy* : déployer le dossier `dags` dans le bucket du Cloud Composer sur Cloud Storage

Ces déploiements sont déclenchés sur les branches `master` / `production`.

