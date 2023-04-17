# ETL jobs

## Fonctionnement

Certains jobs sont éxéxutés via des cloud functions. D'autres sont exécutés dans des VM. 

### Pour l'infrastructure des cloud functions:

Les functions sont déployées une première fois via terraform et le repo `infra-data` pour tous les paramètres liés à l'infra de la fonction.

Pour modifier ces paramètres il faut **apply** le nouveau code terraform, puis **redéployer** le code de la function comme décrit ci-dessous. (Le code terraform va chercher et déployer une archive du code dans GCS qui n'est pas forcément à jour.)

### Pour déployer une nouvelle version du code d'une cloud function:

```
cd jobs/etl_jobs/{external|internal}/{qpi|addresses|...}

gcloud functions deploy FUNCTION_NAME --region "europe-west1"  --entry-point run --source .
```
Avec: 
- FUNCTION_NAME : `qpi_import_<env>` ou `addresses_import_<env>`
