SQL_ANALYTICS_PATH = "dependencies/propilote/sql/analytics"
SQL_TMP_PATH = "dependencies/propilote/sql/tmp"

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
        "effect": "Taux de rétention des partenaires",
        "kpi": "taux_retention_partenaires",
        "table_name": "propilote_tmp_taux_retention_partenaires",
    },
    {
        "question": 6,
        "effect": "Part des établissements actifs",
        "kpi": "taux_participation_eac_ecoles",
        "table_name": "propilote_tmp_taux_participation_eac_ecoles",
    },
    {
        "question": 7,
        "effect": "Score de diversité médian",
        "kpi": "diversity_median",
        "table_name": "propilote_tmp_diversity",
    },
    {
        "question": 8,
        "effect": "Nombre de bénéficiaires",
        "kpi": "beneficiaires",
        "table_name": "propilote_tmp_beneficiaires",
    },
]

tmp_tables_detailed = {
    # Taux de couverture
    "propilote_taux_couverture_region": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_couverture_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "population_region_name",
        },
    },
    "propilote_taux_couverture_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_couverture_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "population_department_code",
        },
    },
    "propilote_taux_couverture_academie": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_couverture_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "population_academy_name",
        },
    },
    "propilote_taux_couverture_all": {
        "sql": f"{SQL_TMP_PATH}/taux_couverture/propilote_tmp_couverture.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_couverture_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Beneficiaires
    "propilote_beneficiaires_region": {
        "sql": f"{SQL_TMP_PATH}/beneficiaires/propilote_tmp_beneficiaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_beneficiaires_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_beneficiaires_departement": {
        "sql": f"{SQL_TMP_PATH}/beneficiaires/propilote_tmp_beneficiaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_beneficiaires_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "num_dep",
        },
    },
    "propilote_beneficiaires_academie": {
        "sql": f"{SQL_TMP_PATH}/beneficiaires/propilote_tmp_beneficiaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_beneficiaires_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
        },
    },
    "propilote_beneficiaires_all": {
        "sql": f"{SQL_TMP_PATH}/beneficiaires/propilote_tmp_beneficiaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_beneficiaires_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Taux d'utilisation à X mois (3 mois par défaut)
    "propilote_taux_utilisation_region": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_utilisation_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "user_region_name",
            "months_threshold": 90,
        },
    },
    "propilote_taux_utilisation_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_utilisation_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "user_department_code",
            "months_threshold": 90,
        },
    },
    "propilote_taux_utilisation_academie": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_utilisation_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
            "months_threshold": 90,
        },
    },
    "propilote_taux_utilisation_all": {
        "sql": f"{SQL_TMP_PATH}/taux_utilisation/propilote_tmp_utilisation.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_utilisation_all",
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
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_montant_24_mois_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_montant_24_mois_departement": {
        "sql": f"{SQL_TMP_PATH}/montant_24_mois/propilote_tmp_montant_24_mois.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_montant_24_mois_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "num_dep",
        },
    },
    "propilote_montant_24_mois_academie": {
        "sql": f"{SQL_TMP_PATH}/montant_24_mois/propilote_tmp_montant_24_mois.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_montant_24_mois_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
        },
    },
    "propilote_montant_24_mois_all": {
        "sql": f"{SQL_TMP_PATH}/montant_24_mois/propilote_tmp_montant_24_mois.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_montant_24_mois_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Part des élèves ayant participé à une offre EAC
    "propilote_taux_participation_eac_jeunes_region": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_jeunes_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_taux_participation_eac_jeunes_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_jeunes_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "department_code",
        },
    },
    "propilote_taux_participation_eac_jeunes_academie": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_jeunes_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
        },
    },
    "propilote_taux_participation_eac_jeunes_all": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_jeunes/propilote_tmp_participation_eac_jeunes.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_jeunes_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Taux de rétention des partenaires
    "propilote_taux_retention_partenaires_region": {
        "sql": f"{SQL_TMP_PATH}/taux_retention_partenaires/propilote_tmp_taux_retention_partenaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_retention_partenaires_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_taux_retention_partenaires_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_retention_partenaires/propilote_tmp_taux_retention_partenaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_retention_partenaires_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "num_dep",
        },
    },
    "propilote_taux_retention_partenaires_academie": {
        "sql": f"{SQL_TMP_PATH}/taux_retention_partenaires/propilote_tmp_taux_retention_partenaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_retention_partenaires_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
        },
    },
    "propilote_taux_retention_partenaires_all": {
        "sql": f"{SQL_TMP_PATH}/taux_retention_partenaires/propilote_tmp_taux_retention_partenaires.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_retention_partenaires_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Part des établissements actifs
    "propilote_taux_participation_eac_ecoles_region": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_ecoles_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_taux_participation_eac_ecoles_departement": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_ecoles_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "department_code",
        },
    },
    "propilote_taux_participation_eac_ecoles_academie": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_ecoles_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
        },
    },
    "propilote_taux_participation_eac_ecoles_all": {
        "sql": f"{SQL_TMP_PATH}/taux_participation_eac_ecoles/propilote_tmp_participation_eac_ecoles.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_taux_participation_eac_ecoles_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
    # Diversité
    "propilote_diversity_region": {
        "sql": f"{SQL_TMP_PATH}/diversity/propilote_tmp_diversity.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_diversity_region",
        "params": {
            "group_type": "REG",
            "group_type_name": "region_name",
        },
    },
    "propilote_diversity_departement": {
        "sql": f"{SQL_TMP_PATH}/diversity/propilote_tmp_diversity.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_diversity_departement",
        "params": {
            "group_type": "DEPT",
            "group_type_name": "num_dep",
        },
    },
    "propilote_diversity_academie": {
        "sql": f"{SQL_TMP_PATH}/diversity/propilote_tmp_diversity.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_diversity_academie",
        "params": {
            "group_type": "ACAD",
            "group_type_name": "academy_name",
        },
    },
    "propilote_diversity_all": {
        "sql": f"{SQL_TMP_PATH}/diversity/propilote_tmp_diversity.sql",
        "destination_dataset": "{{ bigquery_tmp_dataset }}",
        "destination_table": "{{ yyyymmdd(ds) }}_propilote_tmp_diversity_all",
        "params": {
            "group_type": "NAT",
            "group_type_name": "all_dim",
        },
    },
}

analytics_tables = {
    "propilote_kpis": {
        "sql": f"{SQL_ANALYTICS_PATH}/propilote_kpis.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "propilote_kpis",
        "time_partitioning": {"field": "month"},
        "cluster_fields": ["month"],
        "depends": list(tmp_tables_detailed.keys()),
        "params": {
            "kpis_list": kpis_list,
        },
    },
    "pilote_export": {
        "sql": f"{SQL_ANALYTICS_PATH}/pilote_export.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "pilote_export",
        "depends": ["propilote_kpis"],
    },
}

propilote_tables = dict(tmp_tables_detailed, **analytics_tables)
