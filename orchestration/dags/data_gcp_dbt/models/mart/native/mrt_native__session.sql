-- todo : ajouter l'incremental une fois le modèle construit

with consultation_by_sessions as (
SELECT 
    unique_session_id,
    SUM(item_score) AS item_score,
    SUM(subcategory_score) AS subcategory_score,
    SUM(category_score) AS category_score,
from {{ ref('mrt_native__consultation')}}
group by unique_session_id
)

SELECT 
    unique_session_id,
    item_score,
    subcategory_score,
    category_score,
FROM consultation_by_sessions
-- ici : potentiellement ajouter un modèle int_firebase__native_session qui commence à aggréger les données
-- at session_level, joindre la sub query et le modèle ensemble + ajouter les jointures avec les tables métiers globales