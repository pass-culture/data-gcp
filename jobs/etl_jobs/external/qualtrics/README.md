# qualtrics cloud function

La cloud function qualtrics permet de déclencher les deux tâches ci-dessous. Elle sont déclenchées dans le dag `orchestration/dags/jobs/import/import_qualtrics.py`.

1. **L'import des utilisateurs opt-out dans BigQuery**

Pour déclencher cet import, mettre dans le json d'entrée le paramètre :

```yaml
{"task": "import_opt_out_users"}
```
Le résultat est importé dans la table `raw_{env}.qualtrics_opt_out_users`.

2. **L'import des réponses aux enquêtes dans BigQuery**

Pour déclencher cet import, mettre dans le json d'entrée le paramètre :

```yaml
{"task": "import_ir_survey_answers"}
```

Ce code permet uniquement l'import des réponses aux enquêtes d'indicateurs de réputation (pro et jeunes). Pour implémenter l'import d'autres enquêtes Qualtrics, il faudra modifier le code en ajoutant les identifiants des enquêtes dans le dictionnaire `ir_surveys_mapping`. Les identifiants des enquêtes sont disponibles dans l'outil Qualtrics.

Les résultats sont importés dans les tables `raw_{env}.qualtrics_answers_ir_survey_jeunes` et `raw_{env}.qualtrics_answers_ir_survey_pro`. Un traitement de ces tables est fait pour acheminer les données dans la zone analytics (cf `orchestration/dags/dependencies/qualtrics/sql/analytics/`).