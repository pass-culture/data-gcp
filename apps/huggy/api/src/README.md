Test d'implémentation de l'API de reco sous fastAPI en gérant la base de données avec la logique ORM

Le test a été effectué : 
- avec une base de données en local.
- sur la route d'offres similaires 
    - instantiation de l'utilisateur
    - instantiation de l'offre
    - renvoie 2 offres hardcodées de test
    - sauvegarde de ces 2 recommendations dans la table past_similar_offers

TODO : 
# TODO : Intégrer l'intelligence des modèles de ranking/retrieval
# TODO : Intégrer les logs
# TODO : Générer proprement avec sqlalchemy/orm la condition des offres recommendables avec la fonction __get_conditions(). 
# TODO : Adapter les paramètres de connexion à la db pour être capable de se connecter à la cloud sql de dev/stg/prod.

Arborescence du dossier : 
```
+-- src
| +-- core
|   +-- model_engine - orchestration de la pipeline de scoring 
|   +-- model_selection - selection du modèle selon le paramètre model_endpoint
|   +-- scorer - récupérer la base d'offres recommendables et les scorer 
|   +-- utils
|
| +-- crud - fonctions de lecture/écritude de la base de données 
|
| +-- models - modèle des tables de la base de données - 1 fichier par table
|
| +-- schemas - définiton des propriétés des objets
|
| +-- tests
| 
| +-- utils
|
+-- main.py 

```