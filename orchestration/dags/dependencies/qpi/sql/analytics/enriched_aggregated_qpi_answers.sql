with qpi_v1 as (
  with unpivot_qpi as (
    SELECT * 
    FROM `{{ bigquery_analytics_dataset }}.enriched_qpi_answers_v1` 
    UNPIVOT(answers FOR categories IN (cinema,audiovisuel,jeux_videos,livre,musees_patrimoine,musique,pratique_artistique,spectacle_vivant,instrument) ) 
    )
    SELECT 
        uqpi.user_id
        , subcat.category_id as category_id
        ,'none' as subcategory_id 
    from unpivot_qpi uqpi
    join `{{ bigquery_analytics_dataset }}.subcategories` subcat 
    ON lower(subcat.category_id)=uqpi.categories
    where answers 
    and user_id is not null
    and uqpi.answers 
    ),
qpi_v2 as (
  with unpivot_qpi as(
    SELECT
        * 
    FROM `{{ bigquery_analytics_dataset }}.enriched_qpi_answers_v2` 
    UNPIVOT(answers FOR categories IN (cinema,audiovisuel,jeux_videos,livre,musees_patrimoine,musique,pratique_artistique,spectacle_vivant,instrument) ) 
)
    select 
        uqpi.user_id
        , subcat.category_id as category_id
        ,'none' as subcategory_id from unpivot_qpi uqpi
    join `{{ bigquery_analytics_dataset }}.subcategories` subcat 
    ON lower(subcat.category_id)=uqpi.categories
    where answers 
    and user_id is not null
    and uqpi.answers 
),
qpi_v3 as(
  with unpivot_qpi as(
    SELECT
        * 
    FROM `{{ bigquery_analytics_dataset }}.enriched_qpi_answers_v3` 
    UNPIVOT(answers FOR subcategory_id IN (	
    CARTE_CINE_MULTISEANCES	
    ,CARTE_MUSEE	
    ,CINE_PLEIN_AIR	
    ,CINE_VENTE_DISTANCE	
    ,CONCERT	
    ,CONCOURS	
    ,CONFERENCE	
    ,DECOUVERTE_METIERS	
    ,ESCAPE_GAME	
    ,EVENEMENT_CINE	
    ,EVENEMENT_JEU	
    ,EVENEMENT_MUSIQUE	
    ,EVENEMENT_PATRIMOINE	
    ,FESTIVAL_CINE	
    ,FESTIVAL_LIVRE	
    ,FESTIVAL_MUSIQUE	
    ,FESTIVAL_SPECTACLE	
    ,JEU_EN_LIGNE	
    ,JEU_SUPPORT_PHYSIQUE	
    ,LIVESTREAM_EVENEMENT	
    ,LIVESTREAM_MUSIQUE	
    ,LIVRE_AUDIO_PHYSIQUE	
    ,LIVRE_NUMERIQUE	
    ,LIVRE_PAPIER	
    ,LOCATION_INSTRUMENT	
    ,MATERIEL_ART_CREATIF	
    ,MUSEE_VENTE_DISTANCE	
    ,OEUVRE_ART	
    ,`PARTITION`
    ,PODCAST	
    ,RENCONTRE_JEU	
    ,RENCONTRE	
    ,SALON	
    ,SEANCE_CINE	
    ,SEANCE_ESSAI_PRATIQUE_ART	
    ,SPECTACLE_ENREGISTRE	
    ,SPECTACLE_REPRESENTATION	
    ,SUPPORT_PHYSIQUE_FILM	
    ,SUPPORT_PHYSIQUE_MUSIQUE	
    ,TELECHARGEMENT_LIVRE_AUDIO	
    ,TELECHARGEMENT_MUSIQUE	
    ,VISITE_GUIDEE	
    ,VISITE_VIRTUELLE	
    ,VISITE	
    ,VOD) ) 
    )
    select 
        uqpi.user_id
        , subcat.category_id as category_id
        , uqpi.subcategory_id from unpivot_qpi uqpi
    join `{{ bigquery_analytics_dataset }}.subcategories` subcat 
    ON subcat.id=uqpi.subcategory_id
    where uqpi.answers
),
qpi_v4 as (
  SELECT 
    user_id
    , subcat.category_id
    , subcategories as subcategory_id
FROM `{{ bigquery_analytics_dataset }}.enriched_qpi_answers_v4` uqpi
join `{{ bigquery_analytics_dataset }}.subcategories` subcat
ON subcat.id=uqpi.subcategories
)
select * from qpi_v1
UNION ALL
select * from qpi_v2
UNION ALL
select * from qpi_v3
UNION ALL
select * from qpi_v4