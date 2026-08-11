{{
    config(
        cluster_by=["offer_id"],
        tags="weekly",
        labels={"schedule": "weekly"},
    )
}}

select
    global_offer.offer_id,
    global_offer.offer_category_id,
    global_offer.offer_subcategory_id,
    global_offer.venue_type_label,
    global_offer.is_synchronised,
    global_offer.offerer_id,
    global_offer.venue_id,
    global_offer.offer_creation_date,
    global_offer.is_active,
    global_offer.partner_id,
    offer_quality.completion_score,
    offer_quality.absence_image,
    offer_quality.absence_video,
    offer_quality.absence_description,
    offer_quality.livre_cd_vinyle_artiste_manquant,
    offer_quality.livre_cd_vinyle_gtl_manquant,
    offer_quality.livre_cd_vinyle_createur_manquant

from {{ ref("mrt_global__offer") }} as global_offer
inner join
    {{ ref("ml_metadata__offer_quality") }} as offer_quality
    on global_offer.offer_id = offer_quality.offer_id
