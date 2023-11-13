SELECT * ,CONCAT("ARTS-",cluster) as cluster_name FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_item_clusters_30_ARTS`
UNION ALL
SELECT * ,CONCAT("MANGA-",cluster) as cluster_name FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_item_clusters_30_MANGA`
UNION ALL
SELECT * ,CONCAT("LIVRES-",cluster) as cluster_name FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_item_clusters_150_LIVRES`
UNION ALL
SELECT * ,CONCAT("MUSIQUE-",cluster) as cluster_name FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_item_clusters_150_MUSIQUE`
UNION ALL
SELECT * ,CONCAT("CINEMA-",cluster) as cluster_name FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_item_clusters_150_CINEMA`
UNION ALL
SELECT * ,CONCAT("SORTIES-",cluster) as cluster_name FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_item_clusters_30_SORTIES`
