{{ config(materialized="view") }}

select
    eod.item_id,
    max(
        case
            -- LVL1
            when eim.titelive_gtl_id like "07%"
            then true  -- "Religion & Esotérisme"
            when eim.titelive_gtl_id like "08%"
            then true  -- "Entreprise, économie & droit"
            when eim.titelive_gtl_id like "09%"
            then true  -- "Sciences humaines & sociales"
            when eim.titelive_gtl_id like "10%"
            then true  -- "Sciences & Techniques"
            when eim.titelive_gtl_id like "11%"
            then true  -- "Scolaire"
            when eim.titelive_gtl_id like "12%"
            then true  -- "Parascolaire"
            when eim.titelive_gtl_id like "13%"
            then true  -- "Dictionnaires / Encyclopédies / Documentation"
            -- LVL2
            when eim.titelive_gtl_id like "0202%"
            then true  -- "Eveil / Petite enfance (- de 3 ans)",
            when eim.titelive_gtl_id like "0203%"
            then true  -- "Livres illustrés / Enfance (+ de 3 ans)",
            when eim.titelive_gtl_id like "0403%"
            then true  -- "Arts de la table / Gastronomie"
            when eim.titelive_gtl_id like "0406%"
            then true  -- "Vie quotidienne & Bien-être",
            -- LVL3
            when eim.titelive_gtl_id like "030201%"
            then true  -- "Public averti (+ 18 ans)"
            when eim.titelive_gtl_id like "030403%"
            then true  -- "Kodomo"
            when eim.titelive_gtl_id like "040505%"
            then true  -- "Jeux"
            -- LVL4
            when eim.titelive_gtl_id like "01020503%"
            then true  -- "Thriller érotique"
            when eim.titelive_gtl_id like "01020607%"
            then true  -- "Romance Erotique"
            when eim.titelive_gtl_id like "01020608%"
            then true  -- "Dark Romance"
            when eim.titelive_gtl_id like "01030306%"
            then true  -- "Roman libertin"
            when eim.titelive_gtl_id like "03020112%"
            then true  -- "Public averti (érotique, hyper violence…)"
            when eim.titelive_gtl_id like "04010206%"
            then true  -- "Enseignement universitaire"

            else false
        end
    ) as restrained,
    max(
        case
            -- LVL1
            when eim.titelive_gtl_id like "09%"
            then true  -- "Sciences humaines & sociales"
            when eim.titelive_gtl_id like "11%"
            then true  -- "Scolaire"
            when eim.titelive_gtl_id like "12%"
            then true  -- "Parascolaire"
            -- LVL3
            when eim.titelive_gtl_id like "010210%"
            then true  -- "Erotisme"
            when eim.titelive_gtl_id like "061102%"
            then true  -- "Nu / Charme / Erotisme"
            -- LVL4
            when eim.titelive_gtl_id like "01030306%"
            then true  -- "Roman libertin"
            when eim.titelive_gtl_id like "03020112%"
            then true  -- "Public averti (érotique, hyper violence…)"
            when eim.titelive_gtl_id like "04010206%"
            then true  -- "Enseignement universitaire"
            else false
        end
    ) as blocked
from {{ ref("mrt_global__offer") }} eod
inner join {{ ref("ml_input__item_metadata") }} eim on eim.item_id = eod.item_id
where eod.offer_type_domain = "BOOK"
group by 1
