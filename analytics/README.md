# Analytics
Repo contenant le code source pour la partie analytics du projet. Notamment les requêtes pour BigQuery.


## Installation

**Prérequis** :
- pyenv
- poetry
- accès aux comptes de services GCP

**1. Configure le projet**

```
git clone git@github.com:pass-culture/data-gcp.git
cd data-gcp
pyenv install 3.7.7  # si nécessaire
pyenv local 3.7.7
poetry install
```

**2. Configure le compte de service GCP**

Crée une clé sur un compte de service existant (ou nouveau) et télécharge la en JSON.

(ou copie le fichier présent dans le 1password ("compte de service [Bigquery][test]") pour des droits par défaut sur BigQuery)

Copie le fichier `.env` en `.env.local`.

Dans `.env.local`, renseigne le chemin vers ta clé (.JSON) dans la variable `GOOGLE_APPLICATION_CREDENTIALS`.

**3. Ajoute analytics au PYTHONPATH**
```
export PYTHONPATH=$PYTHONPATH:"path/to/analytics"
```

Vous pouvez maintenant lancer les différents scripts !