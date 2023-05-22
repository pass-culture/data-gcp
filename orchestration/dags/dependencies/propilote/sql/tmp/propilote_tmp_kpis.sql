WITH all_months AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month
),
region_dpt AS (
    SELECT 
        num_dep as user_department_code,
        region_name as user_region_name
    FROM `{{ bigquery_analytics_dataset }}.region_department`

),
all_dimension_group AS (
    {% if params.group_type == 'all' %}
        SELECT 'all' as all_dim
    {% else %}    
    SELECT
        DISTINCT {{ params.group_type_name }}
    FROM
       region_dpt
    {% endif %}
),
school_year AS (

    SELECT 
        MAX(adage_id) AS annee_en_cours
    
    FROM 
        `{{ bigquery_clean_dataset }}.applicative_database_educational_year`
),
all_dimension_month AS (
    SELECT
        month,
        {% if params.group_type == 'all' %}
            all_dim,
        {% else %}
            {{ params.group_type_name }},
        {% endif %}
    FROM
        all_months
        CROSS JOIN all_dimension_group
    WHERE
    {% if params.group_type == 'region' %}
        {{ params.group_type_name }} IS NOT NULL 
    {% elif params.group_type == 'department' %}
        {{ params.group_type_name }} NOT IN ('00', '007', '99') 
    {% else %}
        1 = 1
    {% endif %}
),

nb_registrations_agg AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        COUNT(
            DISTINCT CASE
                WHEN type = 'GRANT_18' THEN eud.user_id
                ELSE NULL
            END
        ) AS nb_inscrits_18,
        COUNT(
            CASE
                WHEN (
                    DATE_DIFF(
                        DATE_TRUNC(DATE("{{ ds }}"), MONTH),
                        DATE(user_deposit_creation_date),
                        DAY
                    ) <= 365
                    AND type = 'GRANT_18'
                ) THEN 1
                ELSE NULL
            END
        ) AS inscrits_18_last_12,
        COUNT(
            DISTINCT CASE
                WHEN type = 'GRANT_15_17' THEN eud.user_id
                ELSE NULL
            END
        ) AS total_registered_15_17
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_user_data` eud
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_deposit` applicative_database_deposit ON eud.user_id = applicative_database_deposit.userId
        AND DATE(applicative_database_deposit.datecreated) <= DATE_TRUNC(DATE("{{ ds }}"), MONTH)
    GROUP BY
        1,
        2
),
nb_reservations AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        eud.{{ params.group_type_name }},
    {% endif %}
        COUNT(DISTINCT booking_id) AS nb_reservations_total,
        COUNT(
            DISTINCT CASE
                WHEN digital_goods IS FALSE THEN booking_id
                ELSE NULL
            END
        ) AS dont_physique,
        COUNT(
            DISTINCT CASE
                WHEN digital_goods IS TRUE THEN booking_id
                ELSE NULL
            END
        ) AS dont_numerique
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_deposit` applicative_database_deposit ON ebd.user_id = applicative_database_deposit.userId
        JOIN  `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON ebd.user_id = eud.user_id
    WHERE
        booking_status IN ('USED', 'REIMBURSED')
        AND type = 'GRANT_18'
        AND applicative_database_deposit.datecreated < booking_creation_date
        AND DATE(booking_creation_date) <= DATE_TRUNC(DATE("{{ ds }}"), MONTH)
    GROUP BY
        1,
        2
),
intensity_18 AS (
    SELECT
        ebd.user_id,
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        eud.{{ params.group_type_name }},
    {% endif %}
        COUNT(DISTINCT booking_id) AS nb_reservations
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_deposit` applicative_database_deposit ON ebd.user_id = applicative_database_deposit.userId
        JOIN  `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON ebd.user_id = eud.user_id

    WHERE
        DATE_DIFF(
            DATE_TRUNC(DATE("{{ ds }}"), MONTH),
            DATE(booking_creation_date),
            DAY
        ) <= 365
        AND applicative_database_deposit.datecreated < booking_creation_date
        AND booking_status IN ('USED', 'REIMBURSED')
        AND type = 'GRANT_18'
    GROUP BY
        1,
        2,
        3
),
agg_intensity_18 AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        ROUND(SAFE_DIVIDE(COUNT(
            CASE
                WHEN nb_reservations >= 3 THEN 1
                ELSE NULL
            END
        ), COUNT(*)) * 100 )  AS part_3_resas
    FROM
        intensity_18
    GROUP BY
        1,
        2
),
intensity_15_17 AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        eud.{{ params.group_type_name }},
    {% endif %}
        COUNT(
            DISTINCT ebd.user_id
        ) AS nb_15_17_actifs
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_deposit` apd ON ebd.user_id = apd.userId
        JOIN  `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON ebd.user_id = eud.user_id

    WHERE
        DATE(booking_creation_date) < DATE_TRUNC(DATE("{{ ds }}"), MONTH)
        AND apd.datecreated < booking_creation_date
        AND booking_status IN ('USED', 'REIMBURSED')
        AND type = 'GRANT_15_17'
    GROUP BY
        1,
        2
),
agg_intensity_15_17 AS (
    SELECT
        intensity_15_17.month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        intensity_15_17.{{ params.group_type_name }},
    {% endif %}
    ROUND(SAFE_DIVIDE(nb_15_17_actifs, total_registered_15_17)* 100) AS part_15_17_actifs
    FROM intensity_15_17
    LEFT JOIN nb_registrations_agg on nb_registrations_agg.month = intensity_15_17.month
    {% if params.group_type != 'all' %}
        AND nb_registrations_agg.{{ params.group_type_name }} = intensity_15_17.{{ params.group_type_name }}
    {% endif %}

),

mean_spent_beneficiary_18 AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        user_id AS id_user
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_user_data`
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_deposit` applicative_database_deposit ON `{{ bigquery_analytics_dataset }}.enriched_user_data`.user_id = applicative_database_deposit.userId
    WHERE
        DATE_DIFF(
            DATE_TRUNC(DATE("{{ ds }}"), MONTH),
            DATE(applicative_database_deposit.datecreated),
            DAY
        ) >= 365
        AND type = 'GRANT_18'
    GROUP BY
        1,
        2,
        3
),
agg_mean_spent_beneficiary_18 AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        u.{{ params.group_type_name }},
    {% endif %}
        
        u.user_id,
        SUM(booking_intermediary_amount) AS montant_depense_12mois
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_booking_data` b
        JOIN mean_spent_beneficiary_18 ON mean_spent_beneficiary_18.id_user = b.user_id
        JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` u ON b.user_id = u.user_id
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_deposit` applicative_database_deposit ON u.user_id = applicative_database_deposit.userId
    WHERE
        DATE_DIFF(
            DATE(booking_creation_date),
            DATE(applicative_database_deposit.datecreated),
            DAY
        ) <= 365
        AND type = 'GRANT_18'
        AND booking_status IN ('USED', 'REIMBURSED')
    GROUP BY
        1,
        2,
        3
),
final_agg_mean_spent_beneficiary_18 AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        AVG(montant_depense_12mois) AS montant_moyen_12mois
    FROM
        agg_mean_spent_beneficiary_18
    GROUP BY
        1,
        2
),
nb_actives_15_17 AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        eud.{{ params.group_type_name }},
    {% endif %}
        ebd.user_id,
        SUM(booking_intermediary_amount) AS spent_amount_per_beneficiary
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd
        JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON ebd.user_id = eud.user_id
    WHERE
        booking_is_used
        AND DATE_TRUNC(ebd.booking_used_date, MONTH) = DATE_TRUNC(DATE("{{ ds }}"), MONTH)
        AND ebd.user_id IN (
            SELECT
                DISTINCT userId
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_deposit` deposit
                JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON deposit.userId = eud.user_id
            WHERE
                deposit.type = 'GRANT_15_17'
                AND eud.user_current_deposit_type = 'GRANT_15_17'
        )
    GROUP BY
        1,
        2,
        3
),
montant_moyen AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        AVG(spent_amount_per_beneficiary) AS average_spent_amount
    FROM
        nb_actives_15_17
    GROUP BY
        1,
        2
),


collective_students AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
        date,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        SAFE_DIVIDE(
            SUM(involved_students),
            SUM(total_involved_students)
        ) * 100 AS involved_collective_students
    FROM
        `{{ bigquery_analytics_dataset }}.adage_involved_student` ais
        JOIN region_dpt ON ais.department_code = region_dpt.user_department_code
    WHERE
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) = DATE_TRUNC(date, MONTH)
    GROUP BY
        1,
        2,
        3
    QUALIFY ROW_NUMBER() OVER (ORDER BY DATE DESC) = 1
),

conso_collective AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
        {% if params.group_type == 'region' %}
            school_region_name as {{ params.group_type_name }},
        {% elif params.group_type == 'department'  %} 
            school_department_code as {{ params.group_type_name }},
        {% else %}
            'all' as all_dim,
        {% endif %}
        
        SUM(booking_amount) AS collective_average_spent_amount
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_collective_booking_data` ecbd
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_year` adey ON adey.adage_id = ecbd.educational_year_id
        AND DATE_TRUNC(DATE("{{ ds }}"), MONTH) BETWEEN educational_year_beginning_date
        AND educational_year_expiration_date
    WHERE
        collective_booking_status IN ('USED', 'REIMBURSED')
        AND collective_booking_used_date <DATE_TRUNC(DATE("{{ ds }}"), MONTH)
    GROUP BY
        1,
        2
),
eple_infos AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
        {% if params.group_type == 'region' %}
            venue_region_name as {{ params.group_type_name }},
        {% elif params.group_type == 'department'  %} 
            venue_department_code as {{ params.group_type_name }},
        {% else %}
            'all' as all_dim,
        {% endif %}
        
        COUNT(DISTINCT institution_id) AS nb_total_eple,
        COUNT(
            DISTINCT CASE
                WHEN collective_booking_status IN ('USED', 'REIMBURSED') AND educational_year_id IN (SELECT annee_en_cours FROM school_year) THEN institution_id
                ELSE NULL
            END
        ) AS nb_eple_actifs
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_institution_data` enriched_institution_data
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_booking_data` enriched_collective_booking_data ON enriched_collective_booking_data.educational_institution_id = enriched_institution_data.institution_id
    WHERE
        DATE(first_deposit_creation_date) < DATE_TRUNC(DATE("{{ ds }}"), MONTH)
        AND DATE(collective_booking_creation_date) < DATE_TRUNC(DATE("{{ ds }}"), MONTH)
        AND institution_current_deposit_amount > 0 
        AND institution_current_deposit_amount IS NOT NULL
    GROUP BY
        1,
        2
),
activation_eple AS (
    SELECT
        DATE_TRUNC(DATE("{{ ds }}"), MONTH) as month,
    {% if params.group_type == 'all' %}
        'all' as all_dim,
    {% else %}
        {{ params.group_type_name }},
    {% endif %}
        ROUND(SAFE_DIVIDE(nb_eple_actifs, nb_total_eple) * 100) AS percentage_active_eple
    FROM
        eple_infos
),
join_all_kpis AS (
    {% for kpi_details in params.kpis_list %}
    SELECT
        "{{ kpi_details.effect }}" as effect,
        {{ kpi_details.question }} as question_number,
        "{{ params.group_type }}" as group_type,
    {% if params.group_type == 'all' %}
        'all' as dimension,
    {% else %}
        all_dimension_month.{{ params.group_type_name }} as dimension,
    {% endif %}
        
        FORMAT_DATE("%m-%Y", all_dimension_month.month) AS month,
        {{ kpi_details.kpi }} AS kpi
    FROM
        all_dimension_month
        LEFT JOIN {{ kpi_details.table_name }} ON all_dimension_month.month = {{ kpi_details.table_name }}.month
        AND {{ kpi_details.table_name }}.{{ params.group_type_name }} = all_dimension_month.{{ params.group_type_name }} 
        
        {% if not loop.last %}
        UNION ALL 
        {% endif %} 
    {% endfor %}
)
SELECT
    DATE_TRUNC(DATE("{{ ds }}"), MONTH) as calculation_month,
    *
FROM
    join_all_kpis