WITH offer_tags AS (
    SELECT tag_name, offer_id
FROM `passculture-data-prod.analytics_prod.global_offer_criterion`
WHERE tag_name IN (
  'programme_anneedelamer25',
  'local_anneedelamer25',
  'numerique_anneedelamer25',
  'livres_anneedelamer25',
  'bergers_film',
  'moonlepanda_film',
  'localart_ecologie25',
  'numeriqueart_ecologie25',
  'biensphysiques_bobdylan25',
  'numerique_bobdylan25',
  'local_tousalopera25',
  'numerique_jea25',
  'livres_jea25',
  'local_jea25',
  'localbresil_ameriquelatine25',
  'numeriquebresil_ameriquelatine25',
  'livresbresil_ameriquelatine25',
  'localmusique_ameriquelatine25',
  'numeriquemusique_ameriquelatine25',
  'albumsmusique_ameriquelatine25',
  'numeriquecuisine_ameriquelatine25',
  'livrescuisine_ameriquelatine25',
  'livresrevolution_ameriquelatine25',
  'numeriquerevolution_ameriquelatine25'
)
),
test_items AS (
    SELECT ot.tag_name, go.item_id, ot.offer_id, go.offer_name, go.offer_description
    FROM offer_tags ot
    JOIN `passculture-data-prod.analytics_prod.global_offer` go USING (offer_id)
),
import_embeddings AS (
    SELECT ie.item_id, ie.semantic_content_embedding
    FROM `passculture-data-prod.ml_preproc_prod.item_embedding_extraction` ie
    INNER JOIN test_items ti ON ti.item_id = ie.item_id
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY ie.item_id ORDER BY ie.extraction_date DESC
        ) = 1
)
SELECT
    * EXCEPT(semantic_content_embedding),
    ARRAY(
        SELECT CAST(e AS FLOAT64)
        FROM
            UNNEST(
                SPLIT(SUBSTR(semantic_content_embedding, 2, LENGTH(semantic_content_embedding) - 2))
            ) e
    ) AS embedding
FROM import_embeddings