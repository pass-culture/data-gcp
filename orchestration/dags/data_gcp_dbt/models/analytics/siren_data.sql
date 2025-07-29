select * except (rnk)
from
    (
        select
            siren,
            unite_legale,
            derniertraitement,
            dateCreationUniteLegale,
            identifiantAssociationUniteLegale,
            trancheEffectifsUniteLegale,
            anneeEffectifsUniteLegale,
            dateDernierTraitementUniteLegale,
            categorieEntreprise,
            etatAdministratifUniteLegale,
            nomUniteLegale,
            denominationUniteLegale,
            categorieJuridiqueUniteLegale,
            activitePrincipaleUniteLegale,
            changementCategorieJuridiqueUniteLegale,
            nomenclatureActivitePrincipaleUniteLegale,
            economieSocialeSolidaireUniteLegale,
            caractereEmployeurUniteLegale,
            update_date,
            , row_number() over (partition by siren order by update_date desc) as rnk
        from {{ source("clean", "siren_data") }}
    ) inn
where rnk = 1
