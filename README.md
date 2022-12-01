# data-gcp

Repo pour la team data sur GCP

## Organisation

```
+-- recommendation: API Recommandation (Cloud Run)
|
+-- orchestration : DAGS Airflow (Cloud Composer)
|
+-- additional_data_sources
| +-- cloud_fn :  Cloud Functions gérés via les dags Airflow
| +-- jobs :  Jobs ML ou Scripts python gérés via les dags Airflow

```

## INSTALL
### Analytics (BigQuery)

**Prérequis** :
- [pyenv](https://github.com/pyenv/pyenv-installer)
  - ⚠ Don't forget to [install the prerequisites](https://github.com/pyenv/pyenv/wiki/Common-build-problems#prerequisites)
- [pyenv virtualenv](https://github.com/pyenv/pyenv-virtualenv#installation)
- accès aux comptes de services GCP

**1. Installation du projet**

```
git clone git@github.com:pass-culture/data-gcp.git
cd data-gcp
make install
```

**2. SA GCP**

Crée un SA via GCP afin de configurer ton compte GCP en local. 

**3. Config .env.local**

Dans le fichier `.env.local`, renseigne les valeurs des variables manquantes en utilisant [cette page](https://www.notion.so/passcultureapp/Les-secrets-du-repo-data-gcp-085759e27a664a95a65a6886831bde54)
```
DATA_GCP_TEST_POSTGRES_PORT=
GCP_PROJECT=
ENV_SHORT_NAME=
```


## Orchestration

Orchestration des jobs dags analytics & data science.

[plus de détails dans dags/README.md](/orchestration/README.md)

Les dags sont déployés automatiquement lors d'un merge sur master / production

## Recommandation

API pour la brique de recommandation sur l'application.

[plus de détails dans recommendation/README.md](/recommendation/README.md)


## CI/CD
### CI
On utilise CircleCI pour lancer des tests sur les différentes parties du repo.
Les tests sont lancés sur toutes les branches git et sont répartis entre les jobs suivants :
- *linter* : tester le bon formattage du code de tout le repo en utilisant `Black`
- *recommendation-db-tests* : tester l'ingestion des données dans le CloudSQL pour l'algorithme de recommendation
- *recommendation-api-tests* : tester l'API de recommendation
- *orchestration-tests* : tester les différents DAGs d'orchestration

### CD
Pour la CD, on utilise deux outils : CircleCI et Cloud Build.
#### CircleCI
Voici les jobs créés pour le déploiement :
- *vertex-ai-deploy* : déployer les modèles de ML via MLFlow dans Cloud Storage puis l'utiliser pour mettre à jour la version du modèle sur VertexAI
- *composer-deploy* : déployer le dossier `dags` dans le bucket du Cloud Composer sur Cloud Storage

Ces déploiements sont déclenchés sur les branches `master` / `production`.

#### Cloud build

Cloud build est utilisé pour le déploiement de l'API sur Cloud Run.

Ces déploiements sont déclenchés sur les branches `master` / `production`.

