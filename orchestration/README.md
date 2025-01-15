# Orchestration

Repo pour l'orchestration sur Cloud Composer.

## Structure du dossier

Les fichiers sont organisés de la manière suivante :

- Les dags sont dans `dags/jobs` (2 DAGs sont dans `dags/`).
- Les fonctions communes à tous les DAGs (~utils) sont dans `dags/common`.
- Les requêtes SQL ainsi que la config python spécifique d'un DAG sont dans `dags/dependencies/`.
- La config DBT est dans `dags/data_gcp_dbt`.
- Les tests sont dans le dossier `tests/`.

## Configuration, Déploiement et Lancement des DAGs sur Cloud Composer (Airflow sur GCP)

### Déploiement des DAGs

#### Déploiement automatique

Lorsque l'on merge sur master les dags sont automatiquement déployés sur le cloud composer grâce à Github actions( [Voir doc](../README.md#cd)).

Le job met à jour les fichiers modifiés dans le bucket du cloud composer puis vérifie qu'airflow charge bien les dags. Pour voir quels fichiers ont été modifiés, il faut regarder l'output de l'étape `Deploy to composer` du job `composer-deploy`.

#### Déploiement manuel

```bash
cd orchestration/dags

gcloud composer environments storage dags import \
    --environment data-composer-{ENV} \
    --location europe-west1 \
    --source FILE_PATH_TO_UPLOAD
```

où `{ENV}` est `dev`, `stg` ou `prod` et `FILE_PATH_TO_UPLOAD` est le chemin du fichier à envoyer. Le chemin de référence est dags, donc pour envoyer les dependencies il faut envoyer `--source dags/dependencies`.

### Lancement des DAGs

1. Aller sur l'instance de GCP Composer
2. Dans l'onglet `ENVIRONMENT CONFIGURATION`, cliquer sur le lien Google storage de la section `Airflow web UI`
3. Sélectionner le DAG et le lancer

### Variables d'environnement

<https://cloud.google.com/composer/docs/how-to/managing/environment-variables?hl=fr#adding_and_updating_environment_variables>

Pour voir, ajouter ou modifier les variables d'environement, il faut aller dans la console gcp, sur la page de l'instance de composer puis dans l'onglet variables d'environnement.

### Installer des dépendances

<https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies?hl=fr#install-package>

A partir de la console gcp, dans l'instance de composer, ajouter les dépendances avec leur version.

## Configuration, Déploiement et Lancement des DAGs sur le Airflow local

On peut également choisir de lancer les DAGs en local. Cela permet notamment d'itérer plus vite car on n'a pas besoin de déployer les fichiers sur le cloud composer : ceux-ci sont automatiquement scannés par Airflow.

### Installer Airflow localement (via Docker)

On peut qu'avoir une version de Airflow installé en local. Pour pallier ça, il est possible de lancer Airflow dans un conteneur Docker, ce qui permet d'éviter d'avoir des side-effects sur la machine locale.

#### Prérequis : récupération des Credentials GCP et des variables d'environnement

1. Créer une clé en suivant ce [standard](https://www.notion.so/passcultureapp/R-cuperer-une-clef-SA-pour-Airflow-en-Local-ea66a948a6e644628bafd05e8f0c69ef)
2. Renommer la clé en `sa.gcpkey.json` et la mettre dans `/airflow/etc/sa.gcpkey.json`
3. Uniquement si la machine locale est derrière un Proxy Netskope:
    - Récupérer le certificat **bundled** ou **combiné** de la machine locale. Voir [cette page notion](https://www.notion.so/passcultureapp/Proxyfication-des-outils-du-pass-d1f0da09eafb4158904e9197bbe7c1d4?pvs=4#10cad4e0ff98805ba61efcea26075d65) si on ne trouve pas tout de suite le fichier `*_combined.pem`.
    - Mettre le fichier du certificat bundled dans `/airflow/etc/nscacert_combined.pem `
4. Récupérer le fichier **.env** et le mettre dans `orchestration/.env`, puis:
   - Modifier les valeurs de `_AIRFLOW_WWW_USER_USERNAME` et `_AIRFLOW_WWW_USER_PASSWORD` dans le fichier .env pour mettre un username et password arbitraires.
   - Modifier la valeur du `DAG_FOLDER` en mettant le path vers le folder local.
   - Modifier la valeur de `NETWORK_MODE`:
        - `NETWORK_MODE="proxy"`  si vous êtes sous un proxy
        - `NETWORK_MODE="default"`  sinon
    - En fonction de si l'on souhaite lancer airflow en `dev`ou en `stg`, décommenter les variables :
        - En `dev`:
        ```
        ENV_SHORT_NAME=dev
        DATA_GCS_BUCKET_NAME=data-bucket-dev
        ```
        - en `stg`:
        ```
        ENV_SHORT_NAME=stg
        DATA_GCS_BUCKET_NAME=data-bucket-stg
        ```

### Premier lancement (La première fois uniquement)
sur macos installer la lib coreutils `brew install coreutils`

```sh
make build
```
Ou si on ne veut build que le dockerfile
```
docker build -t <nom de l'image docker> <path vers le dockerfile> --build-arg NETWORK_MODE=<proxy ou default>
```

### Lancement de l'app

1. Lancer les différents conteneurs

    ```sh
    make start
    ```

2. Se connecter au Airflow webserver

    ```sh
    > `http://localhost:8080`
    ```

#### Stop

Pour éteindre les conteneurs :

```sh
make stop
```

#### Changer les variables d'environnement

Pour changer les variables d'environnement, il faut modifier le fichier `orchestration/.env` et relancer le build des conteneurs :

```sh
make build_with_cache
```

#### Troubleshooting

- Pour voir les logs dans les conteneurs :

    ```sh
    make show_airflow_logs
    ```

- Pour supprimer les conteneurs (et les données dans la DB) :

    ```sh
    docker-compose down
    ```
