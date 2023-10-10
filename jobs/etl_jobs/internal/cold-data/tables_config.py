ref_tables = {
    "macro_rayons": {
        "file_type": "csv",
        "schema": {"index": "INTEGER", "macro_rayon": "STRING", "rayon": "STRING"},
    },
    "eac_cash_in": {
        "file_type": "csv",
        "schema": {
            "ministry": "STRING",
            "date_update": "DATE",
            "cash_in": "FLOAT",
        },
    },
    "titelive_isbn_weight": {
        "file_type": "csv",
        "schema": {
            "EAN13": "STRING",
            "POIDS": "INTEGER",
            "LONGUEUR": "INTEGER",
            "LARGEUR": "INTEGER",
        },
    },
    "institutional_partners": {
        "file_type": "csv",
        "schema": {
            "institutional_offerer_id": "STRING",
            "zendesk_id": "STRING",
            "institutional_offerer_name": "STRING",
            "city": "STRING",
            "region_name": "STRING",
            "institutional_offerer_type": "STRING",
            "related_team_pass": "STRING",
        },
    },
    "festival_increments": {
        "file_type": "csv",
        "schema": {
            "offerer_id": "STRING",
            "festival_cnt": "INTEGER",
            "offerer_tag": "STRING",
        },
    },
    "propilote_zones_ref": {
        "file_type": "csv",
        "schema": {
            "zone_id": "STRING",
            "nom": "STRING",
            "zone_code": "STRING",
            "zone_type": "STRING",
            "zone_parent": "STRING",
        },
    },
    "forbidden_query_recommendation": {
        "file_type": "csv",
        "schema": {
            "subcategory_id": "STRING",
            "query": "STRING",
        },
    },
    "forbidden_offers_recommendation": {
        "file_type": "csv",
        "schema": {
            "product_id": "STRING",
        },
    },
    "departmental_objectives": {
        "file_type": "csv",
        "schema": {
            "objective_name": "STRING",
            "objective_type": "STRING",
            "region_name": "STRING",
            "department_code": "STRING",
            "objective": "INTEGER",
        },
    },
    "agg_partner_cultural_sector": {
        "file_type": "csv",
        "schema": {
            "partner_type": "STRING",
            "cultural_sector": "STRING",
        },
    },
    "pilote_geographic_standards": {
        "file_type": "csv",
        "schema": {
            "zone_id": "STRING",
            "nom": "STRING",
        },
    },
    "priority_local_authorities": {
        "file_type": "csv",
        "schema": {
            "priority_local_authority_name": "STRING",
            "priority_local_authority_region": "STRING",
            "priority_local_authority_department": "STRING",
            "priority_local_authority_city": "STRING",
            "priority_local_authority_internal_contact": "STRING",
            "priority_local_authority_type": "STRING",
            "priority_local_authority_status": "STRING",
            "priority_local_authority_siren": "STRING",
            "priority_offerer_id": "STRING",
            "priority_zendesk_id": "STRING",
        },
    },
}
