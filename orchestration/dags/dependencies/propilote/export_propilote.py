SQL_ANALYTICS_PATH = f"dependencies/propilote/sql/analytics"
SQL_TMP_PATH = f"dependencies/propilote/sql/tmp"

kpis_list = [
    {
        "question": 1,
        "effect": "Nombre d'inscrits total du pass Culture (+18 ans)",
        "kpi": "nb_inscrits_18",
        "table_name": "nb_registrations_agg",
    },
    {
        "question": 2,
        "effect": "Nombre d'inscrits du pass Culture sur les 12 derniers mois (+18 ans)",
        "kpi": "inscrits_18_last_12",
        "table_name": "nb_registrations_agg",
    },
    {
        "question": 3,
        "effect": "Nombre de réservations de produits culturels (+18 ans)",
        "kpi": "nb_reservations_total",
        "table_name": "nb_reservations",
    },
    {
        "question": 4,
        "effect": "Nombre de réservations de produits culturels : biens et services",
        "kpi": "dont_physique",
        "table_name": "nb_reservations",
    },
    {
        "question": 5,
        "effect": "Nombre de réservations de produits culturels : produits numériques",
        "kpi": "dont_numerique",
        "table_name": "nb_reservations",
    },
    {
        "question": 6,
        "effect": "Consommation moyenne du crédit pass Culture sur les 12 premiers mois d’utilisation (+18 ans)",
        "kpi": "montant_moyen_12mois",
        "table_name": "final_agg_mean_spent_beneficiary_18",
    },
    {
        "question": 7,
        "effect": "Part d’utilisateurs ayant effectué 3 réservations ou plus sur 12 mois / nombre de comptes ayant réservé (+18 ans)",
        "kpi": "part_3_resas",
        "table_name": "agg_intensity_18",
    },
    {
        "question": 8,
        "effect": "Part des jeunes ayant fait au moins une réservation individuelle (15-17 ans)",
        "kpi": "part_15_17_actifs",
        "table_name": "agg_intensity_15_17",
    },
    {
        "question": 9,
        "effect": "Consommation moyenne des crédits individuels par jeune ayant activé son pass Culture (15-17 ans)",
        "kpi": "average_spent_amount",
        "table_name": "montant_moyen",
    },
    {
        "question": 10,
        "effect": "Part d'élèves ayant participé à au moins une offre collective collège ou lycée (15-17 ans)",
        "kpi": "involved_collective_students",
        "table_name": "collective_students",
    },
    {
        "question": 11,
        "effect": "Consommation moyenne des crédits de la part collective par classe et par année scolaire",
        "kpi": "collective_average_spent_amount",
        "table_name": "conso_collective",
    },
    {
        "question": 12,
        "effect": "Proportion d’établissements ayant engagé au moins 1 action collective",
        "kpi": "percentage_active_eple",
        "table_name": "activation_eple",
    },
    {
        "question": 13,
        "effect": "Nombre total d'utilisateurs de 15 à 17 ans du pass Culture",
        "kpi": "total_registered_15_17",
        "table_name": "nb_registrations_agg",
    },
]

tmp_tables = {
    "propilote_tmp_kpis_region": {
        "sql": f"{SQL_TMP_PATH}/propilote_tmp_kpis.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_kpis_region",
        "params": {
            "group_type": "region",
            "group_type_name": "user_region_name",
            "kpis_list": kpis_list,
        },
    },
    "propilote_tmp_kpis_department": {
        "sql": f"{SQL_TMP_PATH}/propilote_tmp_kpis.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_kpis_department",
        "params": {
            "group_type": "department",
            "group_type_name": "user_department_code",
            "kpis_list": kpis_list,
        },
    },
    "propilote_tmp_kpis_all": {
        "sql": f"{SQL_TMP_PATH}/propilote_tmp_kpis.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_kpis_all",
        "params": {
            "group_type": "all",
            "group_type_name": "all_dim",
            "kpis_list": kpis_list,
        },
    },
}

analytics_tables = {
    "propilote_kpis": {
        "sql": f"{SQL_ANALYTICS_PATH}/propilote_kpis.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "propilote_kpis${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "calculation_month"},
        "cluster_fields": ["calculation_month"],
        "depends": [
            "propilote_tmp_kpis_region",
            "propilote_tmp_kpis_department",
            "propilote_tmp_kpis_all",
        ],
    },
}

propilote_tables = dict(tmp_tables, **analytics_tables)
