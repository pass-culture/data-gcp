SQL_ANALYTICS_PATH = f"dependencies/propilote/sql/analytics"
SQL_TMP_PATH = f"dependencies/propilote/sql/tmp"

kpis_list = [
    {
        "question": 1,
        "effect": "Taux de couverture",
        "kpi": "taux_couverture",
        "table_name": "propilote_tmp_couverture",
    },
    {
        "question": 2,
        "effect": "Taux d'utilisation à 3 mois",
        "kpi": "taux_utilisation",
        "table_name": "propilote_tmp_utilisation",
    },
    {
        "question": 3,
        "effect": "Montant dépensé moyen après 24 mois",
        "kpi": "montant_depense_24_mois",
        "table_name": "propilote_tmp_montant_24_mois",
    },
    {
        "question": 4,
        "effect": "Part des élèves ayant participé à une offre EAC",
        "kpi": "taux_participation_eac_jeunes",
        "table_name": "propilote_tmp_taux_participation_eac_jeunes",
    },
    {
        "question": 5,
        "effect": "Taux d'activation des structures",
        "kpi": "taux_activation_structure",
        "table_name": "propilote_tmp_taux_activation_structure",
    },
    {
        "question": 6,
        "effect": "Part des établissements actifs",
        "kpi": "taux_participation_eac_ecoles",
        "table_name": "propilote_tmp_taux_participation_eac_ecoles",
    },
    {
        "question": 7,
        "effect": "Score de diversification médian",
        "kpi": "diversification_median",
        "table_name": "propilote_tmp_diversification",
    },
]

tmp_tables_detailed = {
    # Taux de couverture
    "propilote_taux_couverture_region": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_couverture_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_taux_couverture_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_couverture_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "department_code",
        },
    },
    "propilote_taux_couverture_all": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_couverture_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Taux d'utilisation à X mois (3 mois par défaut)
    "propilote_taux_utilisation_region": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_utilisation_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "user_region_name",
            "months_threshold": 90,
        },
    },
    "propilote_taux_utilisation_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_utilisation_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "user_department_code",
            "months_threshold": 90,
        },
    },
    "propilote_taux_utilisation_all": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_utilisation_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
            "months_threshold": 90,
        },
    },
    # Montant moyen dépensé par bénéficiaire après 24 mois
    "propilote_montant_24_mois_region": {
        "sql": f"{SQL_TMP_PATH}/montant_24_mois/propilote_tmp_montant_24_mois.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_montant_24_mois_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "user_region_name",
        },
    },
    "propilote_montant_24_mois_departement": {
        "sql": f"{SQL_TMP_PATH}/montant_24_mois/propilote_tmp_montant_24_mois.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_montant_24_mois_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "user_department_code",
        },
    },
    "propilote_montant_24_mois_all": {
        "sql": f"{SQL_TMP_PATH}/montant_24_mois/propilote_tmp_montant_24_mois.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_montant_24_mois_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Part des élèves ayant participé à une offre EAC
    "propilote_taux_participation_eac_jeunes_region": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_participation_eac_jeunes_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_taux_participation_eac_jeunes_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_participation_eac_jeunes_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "department_code",
        },
    },
    "propilote_taux_participation_eac_jeunes_all": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_participation_eac_jeunes_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Taux d'activation des structures
    "propilote_taux_activation_structure_region": {
        "sql": f"{SQL_TMP_PATH}/taux_activation_structure/propilote_tmp_activation_structure.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_activation_structure_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "venue_region_name",
        },
    },
    "propilote_taux_activation_structure_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_activation_structure/propilote_tmp_activation_structure.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_activation_structure_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "venue_department_code",
        },
    },
    "propilote_taux_activation_structure_all": {
        "sql": f"{SQL_TMP_PATH}/taux_activation_structure/propilote_tmp_activation_structure.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_activation_structure_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Part des établissements actifs
    "propilote_taux_participation_eac_ecoles_region": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_participation_eac_ecoles_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "institution_region_name",
        },
    },
    "propilote_taux_participation_eac_ecoles_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_participation_eac_ecoles_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "institution_departement_code",
        },
    },
    "propilote_taux_participation_eac_ecoles_all": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_taux_participation_eac_ecoles_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Diversification
    "propilote_diversification_region": {
        "sql": f"{SQL_TMP_PATH}/diversification/propilote_tmp_diversification.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_diversification_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "user_region_name",
        },
    },
    "propilote_diversification_departement": {
        "sql": f"{SQL_TMP_PATH}/diversification/propilote_tmp_diversification.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_diversification_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "user_department_code",
        },
    },
    "propilote_diversification_all": {
        "sql": f"{SQL_TMP_PATH}/diversification/propilote_tmp_diversification.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "propilote_tmp_diversification_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
}

analytics_tables = {
    "propilote_kpis": {
        "sql": f"{SQL_ANALYTICS_PATH}/propilote_kpis_v2.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "propilote_kpis_v2",
        "time_partitioning": {"field": "mois"},
        "cluster_fields": ["mois"],
        "depends": list(tmp_tables_detailed.keys()),
        "params": {
            "kpis_list": kpis_list,
        },
    },
}

propilote_tables = dict(tmp_tables_detailed, **analytics_tables)
