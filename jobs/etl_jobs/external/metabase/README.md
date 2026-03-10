# Metabase Migration Tool

## Objectif global

Ce job ETL est un **outil de migration automatisée des cartes (questions/dashboards) Metabase** lorsque des tables BigQuery sont renommées ou que leurs colonnes changent de nom. Il permet de mettre à jour en masse les références (tables, schémas, colonnes) dans les cartes Metabase sans avoir à les modifier manuellement une par une.

---

## Architecture des fichiers

| Fichier | Rôle |
|---------|------|
| `main.py` | Point d'entrée CLI (typer), orchestre toute la migration |
| `metabase_api.py` | Client API Metabase (authentification, CRUD cartes/tables) |
| `native.py` | Gestion des cartes **SQL natives** (requêtes écrites en SQL brut) |
| `query.py` | Gestion des cartes **"query builder"** (questions construites via l'UI clics-boutons) |
| `table.py` | Résolution des tables Metabase et mapping des champs (field IDs) |
| `utils.py` | Configuration, accès aux secrets GCP, requêtes BigQuery pour trouver les cartes dépendantes |
| `data/mappings.json` | Dictionnaire de correspondance ancien → nouveau nom de colonne par table |

---

## Flux d'exécution détaillé

### 1. Initialisation (`main.py` + `utils.py`)

```
python main.py --metabase-card-type native|query \
               --legacy-table-name <ancien_nom> \
               --new-table-name <nouveau_nom> \
               --legacy-schema-name <ancien_schema> \
               --new-schema-name <nouveau_schema>
```

- Récupère les **secrets GCP** (host Metabase, client OAuth2, mot de passe API) via `SecretManager`
- L'environnement (`dev`/`stg`/`prod`) est déterminé par la variable `ENV_SHORT_NAME`

### 2. Identification des cartes impactées (`utils.py` → `get_dependant_cards`)

- Exécute une requête BigQuery sur la table `int_metabase_<env>.card_dependency` pour trouver toutes les cartes Metabase qui référencent la table legacy
- Filtre les cartes archivées (`card_name NOT LIKE '%archive%'`)
- Enrichit avec les données d'activité (vues, utilisateurs) depuis `int_metabase_<env>.activity`
- Sépare les résultats en deux listes : **cartes natives** (SQL) et **cartes query** (UI builder)

### 3. Connexion à l'API Metabase (`metabase_api.py`)

- S'authentifie en deux étapes :
  1. Obtient un **OpenID token** via Google OAuth2
  2. Crée une **session Metabase** via `/api/session` avec username/password + bearer token
- Expose des méthodes : `get_cards`, `put_card`, `get_table`, `get_table_metadata`

### 4. Mapping des champs (`table.py`)

- `MetabaseTable` : récupère l'ID d'une table Metabase à partir de son nom et schéma, puis ses métadonnées (field IDs)
- `get_mapped_fields` : crée un dictionnaire `{ancien_field_id: nouveau_field_id}` en croisant les champs des deux tables via les noms de colonnes (avec le mapping JSON si les noms changent)

### 5a. Migration des cartes natives (`native.py`)

Pour chaque carte SQL :
1. **`replace_schema_name`** : remplace `ancien_schema.ancienne_table` → `nouveau_schema.nouvelle_table` dans la requête SQL
2. **`replace_table_name`** : remplace toutes les occurrences restantes de l'ancien nom de table
3. **`replace_column_names`** : renomme les colonnes dans le SQL, **en préservant les lignes contenant `[[ ]]`** (filtres optionnels Metabase, pour éviter de casser la syntaxe)
4. **`update_filters`** : met à jour les `template-tags` (filtres de dimension) avec les nouveaux field IDs
5. **`update_query`** : envoie la carte modifiée via `PUT /api/card/{id}`

### 5b. Migration des cartes query (`query.py`)

Pour chaque carte UI builder :
1. **`update_dataset_query`** : sérialise toute la carte en JSON string, puis fait un **remplacement regex** de tous les anciens field IDs vers les nouveaux, et met à jour le `source-table`
2. **`update_table_id`** : met à jour l'ID de table racine de la carte
3. **`update_card`** : envoie la carte modifiée via `PUT /api/card/{id}`

### 6. Logging (`main.py`)

- Chaque migration (succès ou échec) est logguée dans un DataFrame
- Les logs sont envoyés vers BigQuery dans `int_metabase_<env>.migration_log` (mode `append`)

---

## Le fichier `mappings.json`

Contient les correspondances **ancien nom de colonne → nouveau nom** pour ~15 tables métier de passculture :
- `enriched_user_data`, `enriched_venue_data`, `enriched_offerer_data`, `enriched_collective_offer_data`, `enriched_institution_data`, etc.

Par exemple, `nb_consult_help` → `total_consulted_help`, `booking_cnt` → `total_individual_bookings`. Cela reflète une **campagne de renommage** pour rendre les noms de colonnes plus explicites et cohérents.

---

## En résumé

Ce code automatise la migration des dashboards Metabase de **pass Culture** lors de refactorings BigQuery (renommage de tables/schémas/colonnes). Il identifie automatiquement les cartes impactées, met à jour les requêtes SQL et les définitions de questions UI, puis logge les résultats. C'est un outil critique pour éviter de casser les dashboards métier lors d'évolutions du data warehouse.