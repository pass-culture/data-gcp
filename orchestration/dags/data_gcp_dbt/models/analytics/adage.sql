with adage_agreg_synchro as (
    select siret
    from {{ source('raw', 'adage') }}
    where synchropass = "1.0"
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
    case when siret in (select siret from adage_agreg_synchro) then TRUE else FALSE end as siret_synchro_adage,
    case when left(siret, 9) in (select left(siret, 9) from adage_agreg_synchro) then TRUE else FALSE end as siren_synchro_adage
from {{ source('raw','adage') }}
