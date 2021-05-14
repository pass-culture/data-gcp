# Télécharger la version de 2014 simplifié à 100m sur ce lien : https://www.data.gouv.fr/fr/datasets/contours-des-departements-francais-issus-d-openstreetmap/
# puis dézipper
# puis renommer le dossier obtenu 'départements'
# Lancer le proxy pour la cloudsql de l'environnement
# remplacer la commande psql dans la commande suivante avec celle correspondante à l'environnement
# lancer la commande depuis le path contenant le dossier departements
shp2pgsql -I -W "latin1" departements/departements-20140306-100m.shp departements | psql "host=127.0.0.1 sslmode=disable dbname=cloudsql-recommendation-dev user=cloudsql-recommendation-dev"

# Check Paris
# SELECT code_insee FROM departements WHERE ST_CONTAINS(geom, ST_SetSRID(ST_MakePoint(2.3488, 48.8534), 0));
# Check Guyanne
# SELECT code_insee FROM departements WHERE ST_CONTAINS(geom, ST_SetSRID(ST_MakePoint(-53.125782, 3.933889), 0));
# Check Mayotte
# SELECT code_insee FROM departements WHERE ST_CONTAINS(geom, ST_SetSRID(ST_MakePoint(45.165455, -12.824511), 0));
