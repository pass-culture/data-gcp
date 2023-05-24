SELECT
    {% if params.target == 'pro' %}
    * EXCEPT(numero_identifiant_lieu),
    CASE WHEN numero_identifiant_lieu LIKE 'PRO-%' THEN TRIM(numero_identifiant_lieu, 'PRO-')
        ELSE numero_identifiant_lieu END AS numero_identifiant_lieu
    {% else %}
    *
    {% endif %}
FROM `{{ bigquery_clean_dataset }}.dms_{{ params.target }}`
QUALIFY ROW_NUMBER() OVER (PARTITION BY application_number ORDER BY update_date DESC) = 1
