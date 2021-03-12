# Additional Data Sources

## Fonctionnement

Le code dans `function-source` est le code des cloud function `qpi_import_<env>`.

### Pour l'infrastructure des functions:

Les functions sont déployées une première fois via terraform et le repo `infra-data` pour tous les paramètres liés à l'infra de la fonction.

Pour modifier ces paramètres il faut **apply** le nouveau code terraform, puis **redéployer** le code de la function comme décrit ci-dessous. (Le code terraform va chercher et déployer une archive du code dans GCS qui n'est pas forcément à jour.)

### Pour déployer une nouvelle version du code:

```
cd additional-data-source/function-source

gcloud functions deploy FUNCTION_NAME --region "europe-west1"  --entry-point run --source .
```
Avec: 
- FUNCTION_NAME : `qpi_import_<env>`