# data-gcp
Repo pour Cloud Composer

## Structure du dossier 

Les dags sont dans `dags/`.

Les scripts sont à mettre dans `dags/dependencies/`.

## Envoyer des fichiers sur Cloud Composer:

```
gcloud composer environments storage dags import \
    --environment ENVIRONMENT_NAME \
    --location LOCATION \
    --source FILE_TO_UPLOAD
```

Le chemin de référence est dags, donc pour envoyer les dependencies il faut envoyer `--source dags/dependencies`.

ENVIRONMENT_NAME : data-composer

LOCATION: europe-west1
