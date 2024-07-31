{{
    config(
        materialized = "view"
    )
}}

select
    eod.item_id,
    MAX(
        case
            -- LVL1
            when eim.titelive_gtl_id like "07%" then TRUE -- "Religion & Esotérisme"
            when eim.titelive_gtl_id like "08%" then TRUE -- "Entreprise, économie & droit"
            when eim.titelive_gtl_id like "09%" then TRUE -- "Sciences humaines & sociales"
            when eim.titelive_gtl_id like "10%" then TRUE -- "Sciences & Techniques"
            when eim.titelive_gtl_id like "11%" then TRUE -- "Scolaire"
            when eim.titelive_gtl_id like "12%" then TRUE -- "Parascolaire"
            when eim.titelive_gtl_id like "13%" then TRUE -- "Dictionnaires / Encyclopédies / Documentation"
            -- LVL2
            when eim.titelive_gtl_id like "0202%" then TRUE -- "Eveil / Petite enfance (- de 3 ans)",
            when eim.titelive_gtl_id like "0203%" then TRUE -- "Livres illustrés / Enfance (+ de 3 ans)",
            when eim.titelive_gtl_id like "0403%" then TRUE -- "Arts de la table / Gastronomie"
            when eim.titelive_gtl_id like "0406%" then TRUE -- "Vie quotidienne & Bien-être",
            -- LVL3
            when eim.titelive_gtl_id like "030201%" then TRUE -- "Public averti (+ 18 ans)"
            when eim.titelive_gtl_id like "030403%" then TRUE -- "Kodomo"
            when eim.titelive_gtl_id like "040505%" then TRUE -- "Jeux"
            -- LVL4
            when eim.titelive_gtl_id like "01020503%" then TRUE -- "Thriller érotique"
            when eim.titelive_gtl_id like "01020607%" then TRUE -- "Romance Erotique"
            when eim.titelive_gtl_id like "01020608%" then TRUE -- "Dark Romance"
            when eim.titelive_gtl_id like "01030306%" then TRUE -- "Roman libertin"
            when eim.titelive_gtl_id like "03020112%" then TRUE -- "Public averti (érotique, hyper violence…)"
            when eim.titelive_gtl_id like "04010206%" then TRUE -- "Enseignement universitaire"

            else FALSE
        end
    )
        as restrained,
    MAX(
        case
            -- LVL1
            when eim.titelive_gtl_id like "09%" then TRUE -- "Sciences humaines & sociales"
            when eim.titelive_gtl_id like "11%" then TRUE -- "Scolaire"
            when eim.titelive_gtl_id like "12%" then TRUE -- "Parascolaire"
            -- LVL3
            when eim.titelive_gtl_id like "010210%" then TRUE -- "Erotisme"
            when eim.titelive_gtl_id like "061102%" then TRUE -- "Nu / Charme / Erotisme"
            -- LVL4
            when eim.titelive_gtl_id like "01030306%" then TRUE -- "Roman libertin"
            when eim.titelive_gtl_id like "03020112%" then TRUE -- "Public averti (érotique, hyper violence…)"
            when eim.titelive_gtl_id like "04010206%" then TRUE -- "Enseignement universitaire"
            else FALSE
        end
    ) as blocked
from {{ ref('mrt_global__offer') }} eod
    inner join {{ ref('item_metadata') }} eim on eim.item_id = eod.item_id
where eod.offer_type_domain = "BOOK"
group by 1
