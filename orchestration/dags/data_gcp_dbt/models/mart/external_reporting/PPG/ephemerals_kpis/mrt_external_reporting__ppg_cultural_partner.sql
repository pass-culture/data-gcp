{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "partner_region_name"},
    {"name": "DEP", "value_expr": "partner_department_name"},
] %}

-- Définition des types de partenaires culturels avec leurs critères
{% set partner_types = [
  {
    'name': 'cinemas',
    'label': 'Cinémas',
    'condition': "partner_type IN ('Cinéma - Salle de projections', 'Cinéma itinérant') OR venue_tag_name IN (\"Cinéma d'art et d'essai\")"
  },
  {
    'name': 'cinemas_art_et_essai',
    'label': 'Cinémas Art et Essai',
    'condition': "venue_tag_name IN (\"Cinéma d'art et d'essai\")"
  },
  {
    'name': 'librairies',
    'label': 'Librairies',
    'condition': "partner_type IN ('Librairie', 'Magasin de grande distribution')"
  },
  {
    'name': 'spectacle_vivant',
    'label': 'Spectacle Vivant',
    'condition': "partner_type IN ('Spectacle vivant')"
  },
  {
    'name': 'spectacle_vivant_labels',
    'label': 'Spectacle Vivant avec Labels',
    'condition': "partner_type IN ('Spectacle vivant') AND venue_tag_name IN ('CCN','CDCN','CDN','CNAREP','SCIN','Scène nationale','Théâtre lyrique','Théâtres nationaux')"
  },
  {
    'name': 'musique_live',
    'label': 'Musique Live',
    'condition': "partner_type IN ('Musique - Salle de concerts', 'Festival')"
  },
  {
    'name': 'musee',
    'label': 'Musées',
    'condition': "partner_type IN ('Musée')"
  },
  {
    'name': 'musee_labels',
    'label': 'Musées avec Labels',
    'condition': "partner_type IN ('Musée') AND venue_tag_name IN ('MdF')"
  }
] %}

WITH all_activated_partners_and_days AS (
  -- Pour chaque partner_id, une ligne par jour depuis la 1ère offre publiée
  SELECT
    gcp.partner_id,
    gcp.first_individual_offer_creation_date,
    gcp.first_collective_offer_creation_date,
    DATE_ADD(DATE('2022-01-01'), INTERVAL offset DAY) AS partition_day
  FROM {{ ref('mrt_global__cultural_partner') }} AS gcp
  CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), '2022-01-01', DAY))) AS offset
  WHERE
    gcp.first_individual_offer_creation_date IS NOT null
    AND DATE_ADD(DATE('2022-01-01'), INTERVAL offset DAY) >= gcp.first_individual_offer_creation_date
    AND DATE_ADD(DATE('2022-01-01'), INTERVAL offset DAY) < CURRENT_DATE()
),

all_days_with_bookability AS (
  SELECT
    apd.partner_id,
    apd.first_individual_offer_creation_date,
    apd.first_collective_offer_creation_date,
    apd.partition_day,
    COALESCE(bph.individual_bookable_offers, 0) AS total_indiv_bookable_offers,
    COALESCE(bph.collective_bookable_offers, 0) AS total_collective_bookable_offers
  FROM all_activated_partners_and_days AS apd
  LEFT JOIN {{ ref('bookable_partner_history') }} AS bph
    ON apd.partner_id = bph.partner_id
    AND apd.partition_day = bph.partition_date
),

bookable_dates AS (
  SELECT
    partner_id,
    first_individual_offer_creation_date,
    partition_day,
    DATE_DIFF(
      partition_day,
      COALESCE(
        MAX(CASE WHEN total_indiv_bookable_offers != 0 THEN partition_day END)
        OVER (PARTITION BY partner_id ORDER BY partition_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        first_individual_offer_creation_date
      ),
      DAY
    ) AS days_since_last_indiv_bookable_date,
    DATE_DIFF(
      partition_day,
      COALESCE(
        MAX(CASE WHEN total_collective_bookable_offers != 0 THEN partition_day END)
        OVER (PARTITION BY partner_id ORDER BY partition_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        first_collective_offer_creation_date
      ),
      DAY
    ) AS days_since_last_collective_bookable_date
  FROM all_days_with_bookability
),

partner_details AS (
  SELECT
    bd.partner_id,
    bd.partition_day,
    bd.first_individual_offer_creation_date,
    bd.days_since_last_indiv_bookable_date,
    bd.days_since_last_collective_bookable_date,
    gcp.partner_region_name,
    gcp.partner_department_name,
    gcp.partner_type,
    gcp.offerer_id,
    gvt.venue_tag_name
  FROM bookable_dates AS bd
  INNER JOIN {{ ref('mrt_global__cultural_partner') }} AS gcp
    ON bd.partner_id = gcp.partner_id
  LEFT JOIN {{ ref('mrt_global__venue_tag') }} AS gvt
    ON gcp.partner_id = gvt.partner_id
  INNER JOIN {{ ref('mrt_global__offerer') }} AS gof
    ON gcp.offerer_id = gof.offerer_id
)

{% for dim in dimensions %}
    {% if not loop.first %}
        UNION ALL
    {% endif %}
    SELECT
        DATE_TRUNC(DATE(partition_day), MONTH) AS execution_date,
        DATE("{{ ds() }}") AS update_date,
        '{{ dim.name }}' AS dimension_name,
        {{ dim.value_expr }} AS dimension_value,
        'nombre_total_de_partenaire_actif' AS kpi_name,
        COUNT(DISTINCT CASE
            WHEN days_since_last_indiv_bookable_date <= 365
            THEN partner_id
        END) AS numerator,
        1 AS denominator,
        COUNT(DISTINCT CASE
            WHEN days_since_last_indiv_bookable_date <= 365
            THEN partner_id
        END) AS kpi
    FROM partner_details
    GROUP BY execution_date, update_date, dimension_name, dimension_value, kpi_name
    UNION ALL
    {% for partner_type in partner_types %}
    {% if not loop.first %}
        UNION ALL
    {% endif %}
    SELECT
        DATE_TRUNC(DATE(partition_day), MONTH) AS execution_date,
        DATE("{{ ds() }}") AS update_date,
        '{{ dim.name }}' AS dimension_name,
        {{ dim.value_expr }} AS dimension_value,
        "nombre_de_partenaire_actif_{{ partner_type.name }}" AS kpi_name,
        COUNT(DISTINCT CASE
            WHEN days_since_last_indiv_bookable_date <= 365
            AND {{ partner_type.condition }}
            THEN partner_id
        END) AS numerator,
        1 AS denominator,
        COUNT(DISTINCT CASE
            WHEN days_since_last_indiv_bookable_date <= 365
            AND {{ partner_type.condition }}
            THEN partner_id
        END) AS kpi
    FROM partner_details
    GROUP BY execution_date, update_date, dimension_name, dimension_value, kpi_name
    {% endfor %}
{% endfor %}
