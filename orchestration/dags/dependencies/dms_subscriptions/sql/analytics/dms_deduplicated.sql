SELECT
    {% if params.target == 'pro' %}
    * EXCEPT(numero_identifiant_lieu, erreur_traitement_pass_culture),
    CASE WHEN numero_identifiant_lieu LIKE 'PRO-%' THEN TRIM(numero_identifiant_lieu, 'PRO-')
        ELSE numero_identifiant_lieu END AS numero_identifiant_lieu
    , TRIM(erreur_traitement_pass_culture) as erreur_traitement_pass_culture
    {% else %}
    *
    {% endif %}
FROM `{{ bigquery_clean_dataset }}.dms_{{ params.target }}`
QUALIFY ROW_NUMBER() OVER (PARTITION BY application_number ORDER BY update_date DESC, last_update_at DESC) = 1
