# DAGS
Repo des DAGS pour Cloud Composer

## Structure du dossier

Les dags sont dans `dags/`.

Les autres scripts sont à mettre dans `dags/dependencies/`.

## Uploader les fichiers sur Cloud Composer

```
gcloud composer environments storage dags import \
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source FILE_TO_UPLOAD
```

Le chemin de référence est dags, donc pour envoyer les dependencies il faut envoyer `--source dags/dependencies`.


- ENVIRONMENT_NAME : data-composer
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
