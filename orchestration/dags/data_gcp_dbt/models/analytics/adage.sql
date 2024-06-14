WITH adage_agreg_synchro AS (
SELECT siret
FROM {{ source('raw', 'adage')}}
where synchroPass = "1.0"
)

SELECT
    id,
    venueId,
    siret,
    statutId,
    labelId,
    typeId,
    communeId,
    libelle,
    adresse,
    siteWeb,
    latitude,
    longitude,
    actif,
    synchroPass,
    dateModification,
    statutLibelle,
    labelLibelle,
    typeIcone,
    typeLibelle,
    communeLibelle,
    communeDepartement,
    academieLibelle,
    regionLibelle,
    domaines,
    domaineIds,
    regionId,
    academieId,
    left(siret, 9) AS siren,
    CASE WHEN siret in (select siret from adage_agreg_synchro) THEN TRUE ELSE FALSE END AS siret_synchro_adage,
    CASE WHEN left(siret, 9) in (select left(siret, 9) from adage_agreg_synchro) THEN TRUE ELSE FALSE END AS siren_synchro_adage,
FROM {{ source('raw','adage') }}
