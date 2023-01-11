# Orchestration
Repo pour l'orchestration sur Cloud Composer.

## Structure du dossier

Les dags sont dans `orchestration/dags/`.

Les scripts appelés dans les dags sont à mettre dans `orchestration/dags/dependencies/`.

Les tests sont dans le dossier `orchestration/tests/`.

## Déploiement automatique des dags

Lorsque l'on merge sur master les dags sont automatiquement déployés sur le cloud composer grâce à un job circle-ci.

Le job met à jour les fichiers modifiés dans le bucket du cloud composer puis vérifie qu'airflow charge bien les dags. Pour voir quels fichiers ont été modifiés, il faut regarder l'output de l'étape `Deploy to composer` du job `composer-deploy`.

Lors de l'ajout d'un nouveau dag : ajouter un appel au script `./wait_for_dag_deployed.sh` correspondant à ce dag dans le job `composer-deploy`.

## Uploader manuellement les fichiers sur Cloud Composer

```
cd orchestration/dags

gcloud composer environments storage dags import \
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source FILE_TO_UPLOAD
```

Le chemin de référence est dags, donc pour envoyer les dependencies il faut envoyer `--source dags/dependencies`.


- ENVIRONMENT_NAME : data-composer-\<env>
- LOCATION: europe-west1

## Lancement des DAGs

1. Aller sur l'instance de GCP Composer
2. Dans l'onglet `ENVIRONMENT CONFIGURATION`, cliquer sur le lien Google storage de la section `Airflow web UI`
3. Sélectionner le DAG et le lancer

## Variables d'environnement
https://cloud.google.com/composer/docs/how-to/managing/environment-variables?hl=fr#adding_and_updating_environment_variables

Pour voir, ajouter ou modifier les variables d'environement, il faut aller dans la console gcp, sur la page de l'instance de composer puis dans l'onglet variables d'environnement.

## Installer des dépendances
https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies?hl=fr#install-package

A partir de la console gcp, dans l'instance de composer, ajouter les dépendances avec leur version.

# Local 

## Installer Airflow localement:

### .env

Dans le fichier `.env`, il est necessaire de mettre à jour les configs

- Set-up some airflow admin user & password:
```sh
_AIRFLOW_WWW_USER_USERNAME=<user>
_AIRFLOW_WWW_USER_PASSWORD=<password>
```

### airflow.cfg

Il est necessaire de récuperer les configs de `airflow/config/airflow.cfg`


### sa.gcpkey.json

Mettre la clef SA composer dans `/airflow/etc/sa.gcpkey.json`

#### 4. Build and run

Build 

```sh
make build
```

(uniquement la première fois) :init airflow, création des tables par défaut.

```sh
docker-compose up airflow-init
```

Lancer les différents conteneurs
```sh
make dev
```

Lancer le Airflow webserver
```sh
> `http://localhost:8080`
```

#### Stop

```sh
make stop
```
