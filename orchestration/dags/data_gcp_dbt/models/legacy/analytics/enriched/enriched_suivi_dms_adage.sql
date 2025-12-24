with
    siren_reference_adage as (
        select siren, max(siren_synchro_adage) as siren_synchro_adage
        from {{ ref("adage") }}
        group by 1
    )

select
    dms_pro.procedure_id,
    dms_pro.demandeur_entreprise_siren,
    dms_pro.demandeur_siret,
    dms_pro.demandeur_entreprise_siretsiegesocial
    as demandeur_entreprise_siret_siege_social,
    dms_pro.demandeur_entreprise_raisonsociale as demandeur_entreprise_raison_sociale,
    dms_pro.application_number,
    dms_pro.application_status,
    dms_pro.application_submitted_at,
    dms_pro.passed_in_instruction_at,
    dms_pro.processed_at,
    dms_pro.instructors,
    offerer.offerer_id,
    offerer.offerer_creation_date,
    offerer.offerer_validation_date,
    venue.venue_id,
    venue.venue_name,
    venue.venue_creation_date,
    venue.venue_is_permanent,
    adage.id as adage_id,
    adage.datemodification as adage_date_modification,
    case
        when venue.venue_is_permanent
        then concat("venue-", venue.venue_id)
        else concat("offerer-", offerer.offerer_id)
    end as partner_id,
    case
        when demandeur_siret in (select siret from {{ ref("adage") }})
        then true
        else false
    end as siret_ref_adage,
    case
        when
            demandeur_siret
            in (select siret from {{ ref("adage") }} where siret_synchro_adage = true)
        then true
        else false
    end as siret_synchro_adage,
    coalesce(
        demandeur_entreprise_siren in (select siren from siren_reference_adage), false
    ) as siren_ref_adage,
    coalesce(
        demandeur_entreprise_siren
        in (select siren from siren_reference_adage where siren_synchro_adage),
        false
    ) as siren_synchro_adage

from {{ ref("dms_pro") }}
left join
    {{ ref("int_raw__offerer") }} as offerer
    on dms_pro.demandeur_entreprise_siren = offerer.offerer_siren
    and offerer.offerer_siren <> "nan"
left join
    {{ ref("int_global__venue") }} as venue
    on offerer.offerer_id = venue.offerer_id
    and venue_name <> "Offre numérique"
left join
    {{ source("analytics", "adage") }} as adage on dms_pro.demandeur_siret = adage.siret
where
    dms_pro.application_status = "accepte"
    and dms_pro.procedure_id in ("57081", "57189", "61589", "65028", "80264")
