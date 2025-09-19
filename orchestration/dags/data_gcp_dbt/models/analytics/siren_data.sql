select inn.* except (inn.rnk)
from
    (
        select
            siren,
            unite_legale,
            derniertraitement,
            datecreationunitelegale,
            identifiantassociationunitelegale,
            trancheeffectifsunitelegale,
            anneeeffectifsunitelegale,
            datederniertraitementunitelegale,
            categorieentreprise,
            etatadministratifunitelegale,
            nomunitelegale,
            denominationunitelegale,
            categoriejuridiqueunitelegale,
            activiteprincipaleunitelegale,
            changementcategoriejuridiqueunitelegale,
            nomenclatureactiviteprincipaleunitelegale,
            economiesocialesolidaireunitelegale,
            caractereemployeurunitelegale,
            update_date,
            row_number() over (partition by siren order by update_date desc) as rnk
        from {{ source("clean", "siren_data") }}
    ) inn
where rnk = 1
