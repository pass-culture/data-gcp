En zone raw, les tables qpi_answers_v1, qpi_answers_v2, qpi_answers_v3 sont les données brutes des anciennes versions du questionnaire. Elles ne sont plus utilisées dans notre traitement. 

Dans la zone clean, ces tables ont été copiées sous le mêmes noms de tables en supprimant le champ culturalsurvey_id et en ajoutant le champ catch_up_user_id.

Une autre étape de nettoyage de ces 3 tables a été faite et cachée pour être iso à la structure du questionnaire dans sa derniere version (v4) => Il s'agit des tables analytics: enriched_qpi_answers_v1, enriched_qpi_answers_v2, enriched_qpi_answers_v3. 
Comme ces tables ne sont pas vouées à être utilisée dans analytics, elles sont déplacées dans la zone clean => qpi_answers_v1_clean, qpi_answers_v2_clean et qpi_answers_v3_clean

# TODO : supprimer les tables enriched_qpi_answers_v1, enriched_qpi_answers_v2 et enriched_qpi_answers_v3 de la zone analytics.
# TODO : supprimer la table enriched_qpi_answers_aggregated_versions qui n'est plus mise à jour depuis novembre 2022.
# TODO : supprimer la table enriched_aggregated_qpi_answers qui est renommée.

- Les tables sont partitionnées.

- Renommage de la table enriched_aggregated_qpi_answers en enriched_qpi_answers