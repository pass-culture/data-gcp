from typing import Any, Dict, List

from common.config import ENV_SHORT_NAME

S3_SECRET_NAME = f"dbt_export_s3_config_snum_{ENV_SHORT_NAME}"

# Columns exported to the prod bucket: the validated vidoc KPIs (see
# _dbt_docs/kpis), so that new columns can be added to the exp_vidoc__* models
# without being sent to prod. The dev and staging buckets get all the columns of
# these tables, for the VIDOC team to build the next indicators.
VIDOC_MODELS = {
    "diversity": [
        "deposit_expiration_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "is_in_qpv",
        "macro_density_label",
        "micro_density_label",
        "total_expired_deposit_beneficiaries",
        "total_3plus_category_booked_beneficiaries",
        # lot 2.1: FI2.3, FI2.4, FI3.1, FI3.2
        *[f"total_{n}plus_category_booked_beneficiaries" for n in (1, 2, 4, 5)],
        *[
            f"total_spent_share_{bucket}_beneficiaries"
            for bucket in ["0"] + [f"{low}_{low + 10}" for low in range(0, 100, 10)]
        ],
        "total_deposit_amount_at_expiration",
        "total_actual_amount_spent_at_expiration",
        "total_distinct_venue_booked_at_expiration",
    ],
    "diversity_by_category": [
        "deposit_expiration_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "is_in_qpv",
        "macro_density_label",
        "micro_density_label",
        "offer_category_id",
        "total_category_booked_beneficiaries",
    ],
    "beneficiary": [
        "partition_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "age_at_calculation",
        "is_in_qpv",
        "macro_density_label",
        "micro_density_label",
        "total_beneficiaries",
        "total_actual_beneficiaries",
    ],
    "population_coverage": [
        "partition_month",
        "birth_month",
        "milestone_age",
        "department_code",
        "department_name",
        "region_name",
        "region_code",
        "total_population_last_12_months",
    ],
    "beneficiary_coverage": [
        "partition_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "milestone_age",
        "total_beneficiaries_last_12_months",
    ],
    # lot 2.2: FI3.3 (masked rows < 5 young people, < 20 for the categories)
    "booking_beneficiary": [
        "partition_month",
        "region_name",
        "region_code",
        "department_name",
        "department_code",
        "offer_category_id",
        "is_low_count",
        "total_used_bookings",
        "total_used_amount",
    ],
    # lot 2.2: FI4.2, FI4.3 (active partners only, no cumulative activated)
    "cultural_partner": [
        "partition_month",
        "partner_region_name",
        "partner_region_code",
        "partner_department_name",
        "partner_department_code",
        *[
            f"total_active_partners_{channel}"
            for channel in (
                "global",
                "individual",
                "collective",
                "dual_part",
                "individual_only",
                "collective_only",
            )
        ],
    ],
    "cultural_partner_by_type": [
        "partition_month",
        "partner_region_name",
        "partner_region_code",
        "partner_department_name",
        "partner_department_code",
        "partner_type",
        *[
            f"total_active_partners_{channel}"
            for channel in ("global", "individual", "collective", "dual_part")
        ],
    ],
    "cultural_partner_by_label": [
        "partition_month",
        "partner_region_name",
        "partner_region_code",
        "partner_department_name",
        "partner_department_code",
        "partner_label",
        *[
            f"total_active_partners_{channel}"
            for channel in ("global", "individual", "collective", "dual_part")
        ],
    ],
    # detail of the partners active in the current month (public lists, no
    # amounts, no SIRET); one is_<type> flag per type of get_partner_types
    "cultural_partner_list": [
        "partition_month",
        "venue_id",
        "venue_name",
        "offerer_id",
        "offerer_name",
        "venue_type_label",
        "venue_labels",
        "is_active_individual",
        "is_active_collective",
        "is_active_dual_part",
        "last_individual_bookable_date",
        "last_collective_bookable_date",
        "partner_region_name",
        "partner_region_code",
        "partner_department_name",
        "partner_department_code",
        "partner_epci_code",
        "partner_municipality_code",
        "partner_municipality_label",
        "partner_postal_code",
        "partner_is_in_qpv",
        *[
            f"is_{partner_type}"
            for partner_type in (
                "cinemas",
                "tag_cinemas_art_et_essai",
                "librairies",
                "tag_librairies_lir",
                "spectacle_vivant",
                "tag_spectacle_vivant_labels",
                "musique_live",
                "musee",
                "tag_musee_labels",
                "arts_visuels_plastiques_galeries",
                "autre",
                "bibliotheques_ou_mediatheques",
                "centre_culturel",
                "pratique_art",
                "culture_scientifique",
                "festival",
                "jeux_videos",
                "magasins_arts_creatifs",
                "musique_disquaire",
                "musique_instrument",
                "offre_numerique",
                "patrimoine_tourisme",
            )
        ],
    ],
}


def generate_table_configs(models: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """
    dbt model `exp_vidoc__<name>` is aliased in BigQuery to
    `export_vidoc_{env}.<name>` (see `generate_alias_name` macro), then its
    `columns` (all of them outside prod) are exported to
    `s3://<bucket>/exp_vidoc__<name>/` (bucket resolved from the S3 secret).
    """
    return [
        {
            "dbt_model": f"exp_vidoc__{name}",
            "bigquery_dataset_name": f"export_vidoc_{ENV_SHORT_NAME}",
            "bigquery_table_name": name,
            "columns": columns if ENV_SHORT_NAME == "prod" else ["*"],
            "s3_prefix": f"exp_vidoc__{name}",
        }
        for name, columns in models.items()
    ]


TABLES_CONFIGS = generate_table_configs(VIDOC_MODELS)
