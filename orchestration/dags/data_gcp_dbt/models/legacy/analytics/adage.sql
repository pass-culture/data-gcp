with
    adage_agreg_synchro as (
        select siret
        from {{ source("raw", "adage") }}
        where synchropass = "1.0"
        qualify row_number() over (partition by siret order by update_date desc) = 1
    )

select
    id,
    venueid,
    siret,
    statutid,
    labelid,
    typeid,
    communeid,
    libelle,
    adresse,
    siteweb,
    latitude,
    longitude,
    actif,
    synchropass,
    datemodification,
    statutlibelle,
    labellibelle,
    typeicone,
    typelibelle,
    communelibelle,
    communedepartement,
    academielibelle,
    regionlibelle,
    domaines,
    domaineids,
    regionid,
    academieid,
    left(siret, 9) as siren,
    coalesce(
        siret in (select siret from adage_agreg_synchro), false
    ) as siret_synchro_adage,
    coalesce(
        left(siret, 9) in (select left(siret, 9) from adage_agreg_synchro), false
    ) as siren_synchro_adage
from {{ source("raw", "adage") }}
qualify row_number() over (partition by siret order by update_date desc) = 1
