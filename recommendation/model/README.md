# Model - AI Platform
Repo des modèles de l'algorithme de recommendation

## Uploader manuellement le modèle sur Cloud Storage
En ligne de commande : 

```
gsutil cp <local_path> gs://pass-culture-data/<file_path>
```

Ou directement sur la console GCP : 
https://console.cloud.google.com/storage/browser/pass-culture-data;tab=objects?forceOnBucketsSortingFiltering=false&project=pass-culture-app-projet-test&prefix=&forceOnObjectsSortingFiltering=false

## Faire des prédictions à partir du modèle
Le modèle doit être sur l'AI Platform : 
https://console.cloud.google.com/ai-platform/models?hl=fr&project=pass-culture-app-projet-test

Pour tester le modèle il suffit de sélectionner sa version et d'aller dans l'onglet `TEST ET UTILISATION`

