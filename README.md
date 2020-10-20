# data-gcp
Repo pour la team data sur GCP



## installer des dépendances : 
https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies?hl=fr#install-package

A partir de la console gcp, dans l'instance de composer, ajouter les dépendances avec leur version.


gcloud composer environments storage dags import \
    --environment data-composer \
    --location europe-west1 \
    --source dags