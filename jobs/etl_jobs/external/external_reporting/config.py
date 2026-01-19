import os
from collections import defaultdict
from enum import Enum
from pathlib import Path

from utils.verbose_logger import log_print

######## base configs
GCP_PROJECT = os.environ.get("GCP_PROJECT", "passculture-data-prod")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "prod")
if GCP_PROJECT != "passculture-data-prod" or ENV_SHORT_NAME != "prod":
    log_print.warning(
        f"Using {GCP_PROJECT=} & {ENV_SHORT_NAME=} -> REPORTS DATA WILL BE PARTIAL OR WRONG",
        fg="red",
        bold=True,
    )

BIGQUERY_ANALYTICS_DATASET = f"analytics_{ENV_SHORT_NAME}"
EXPORT_BUCKET = f"de-bigquery-data-export-{ENV_SHORT_NAME}"
REGION_HIERARCHY_TABLE = "region_department"

BASE_TEMPLATE = Path("./templates/export_template.xlsx")
REPORT_BASE_DIR_DEFAULT = Path("./reports")

# Google Drive Configuration
GOOGLE_DRIVE_ROOT_FOLDER_ID_MAPPING = {
    "dev": "1xtAFgDX-0IZmbvYgho82oBWGXIB5Fl45",
    "stg": "1_kbEaW24GZptXC-AU8euZ8iiW3Vp_7Wv",
    "prod": "1k7hbhKXNM2_X50TOyNnwgHAjlhaPRxHF",
}
GOOGLE_DRIVE_ROOT_FOLDER_ID = GOOGLE_DRIVE_ROOT_FOLDER_ID_MAPPING[ENV_SHORT_NAME]

# French month names for folder naming
MONTH_NAMES_FR = {
    1: "Janvier",
    2: "Février",
    3: "Mars",
    4: "Avril",
    5: "Mai",
    6: "Juin",
    7: "Juillet",
    8: "Août",
    9: "Septembre",
    10: "Octobre",
    11: "Novembre",
    12: "Décembre",
}

# data sources
table_prefix = "external_reporting"

SOURCE_TABLES = {
    "individual": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_individual",
    },
    "collective": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_eac",
    },
    "top_offer": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_offer",
    },
    "top_offer_category": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_offer_category",
    },
    "top_offer_label": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_offer_label",
    },
    "top_venue": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_venue",
    },
    "top_labeled_venue": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_labeled_venue",
    },
    "top_ac": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_ac",
    },
    "top_format": {
        "dataset": BIGQUERY_ANALYTICS_DATASET,
        "table": f"{table_prefix}_top_format",
    },
}

STAKEHOLDER_REPORTS = {
    "ministere": ["national_summary"],
    "drac": ["regional_summary", "departemental_detail", "academy_detail"],
}

REPORTS = {
    "national_summary": {
        "sheets": [
            {"definition": "lexique"},
            {
                "definition": "individual_kpis",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "top_offer",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "top_offer_category",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "top_venue",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "top_labeled_venue",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "top_offer_label",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "collective_kpis",
                "filters": {"scope": "collective", "scale": "national"},
            },
            {
                "definition": "top_ac",
                "filters": {"scope": "collective", "scale": "national"},
            },
            {
                "definition": "top_format",
                "filters": {"scope": "collective", "scale": "national"},
            },
        ]
    },
    "region_summary": {
        "sheets": [
            {"definition": "lexique"},
            {
                "definition": "individual_kpis",
                "filters": {"scope": "individual", "scale": "national"},
            },
            {
                "definition": "individual_kpis",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "top_offer",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "top_offer_category",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "top_offer_label",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "top_venue",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "top_labeled_venue",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "collective_kpis",
                "filters": {"scope": "collective", "scale": "national"},
            },
            {
                "definition": "collective_kpis",
                "filters": {"scope": "collective", "scale": "region"},
            },
            {
                "definition": "top_ac",
                "filters": {"scope": "collective", "scale": "region"},
            },
            {
                "definition": "top_format",
                "filters": {"scope": "collective", "scale": "region"},
            },
        ]
    },
    "academy_detail": {
        "sheets": [
            {"definition": "lexique"},
            {
                "definition": "collective_kpis",
                "filters": {"scope": "collective", "scale": "region"},
            },
            {
                "definition": "collective_kpis",
                "filters": {"scope": "collective", "scale": "academie"},
            },
            {
                "definition": "top_ac",
                "filters": {"scope": "collective", "scale": "academie"},
            },
            {
                "definition": "top_format",
                "filters": {"scope": "collective", "scale": "academie"},
            },
        ]
    },
    "department_detail": {
        "sheets": [
            {"definition": "lexique"},
            {
                "definition": "individual_kpis",
                "filters": {"scope": "individual", "scale": "region"},
            },
            {
                "definition": "individual_kpis",
                "filters": {"scope": "individual", "scale": "departement"},
            },
            {
                "definition": "top_offer",
                "filters": {"scope": "individual", "scale": "departement"},
            },
            {
                "definition": "top_offer_category",
                "filters": {"scope": "individual", "scale": "departement"},
            },
            {
                "definition": "top_offer_label",
                "filters": {"scope": "individual", "scale": "departement"},
            },
            {
                "definition": "top_venue",
                "filters": {"scope": "individual", "scale": "departement"},
            },
            {
                "definition": "top_labeled_venue",
                "filters": {"scope": "individual", "scale": "departement"},
            },
        ]
    },
}


class SheetType(Enum):
    KPIS = "kpis"
    TOP = "top"
    LEXIQUE = "lexique"


SHEET_DEFINITIONS = {
    "individual_kpis": {
        "type": SheetType.KPIS,
        "template_tab": "template_individuel",
        "source_table": "individual",
    },
    "collective_kpis": {
        "type": SheetType.KPIS,
        "template_tab": "template_collectif",
        "source_table": "collective",
    },
    "lexique": {
        "type": SheetType.LEXIQUE,
        "template_tab": "Lexique",
        "source_table": None,
    },
    "top_offer": {
        "type": SheetType.TOP,
        "title_suffix": "Part Individuelle",
        "template_tab": "Top offres",
        "source_table": "top_offer",
        "top_n": 50,
        "select_fields": [
            "partition_month",
            "offer_category_id",
            "offer_subcategory_id",
            "offer_name",
            "total_booking_amount",
            "total_booking_quantity",
        ],
        "ranking": {
            "order_by": [
                {"field": "total_booking_amount", "direction": "DESC"},
            ],
        },
    },
    "top_offer_category": {
        "type": SheetType.TOP,
        "title_suffix": "Part Individuelle",
        "template_tab": "Top par catégorie",
        "source_table": "top_offer_category",
        "top_n": 50,
        "select_fields": [
            "partition_month",
            "offer_category_id",
            "offer_subcategory_id",
            "offer_name",
            "total_booking_amount",
            "total_booking_quantity",
        ],
        "ranking": {
            "partition_by": ["offer_category_id"],
            "order_by": [
                {"field": "offer_category_id", "direction": "DESC"},
                {"field": "total_booking_amount", "direction": "DESC"},
            ],
        },
    },
    "top_offer_label": {
        "type": SheetType.TOP,
        "title_suffix": "Part Individuelle",
        "template_tab": "Top offres lieux labellisés",
        "source_table": "top_offer_label",
        "top_n": 50,
        "select_fields": [
            "partition_month",
            "offer_category_id",
            "offer_subcategory_id",
            "offer_name",
            "venue_tag_name",
            "total_booking_amount",
            "total_booking_quantity",
        ],
        "ranking": {
            "partition_by": ["venue_tag_name"],
            "order_by": [
                {"field": "venue_tag_name", "direction": "DESC"},
                {"field": "total_booking_amount", "direction": "DESC"},
            ],
        },
    },
    "top_venue": {
        "type": SheetType.TOP,
        "title_suffix": "Part Individuelle",
        "template_tab": "Top lieux",
        "source_table": "top_venue",
        "top_n": 50,
        "select_fields": ["partition_month", "venue_name", "offerer_name"],
        "ranking": {
            "order_by": [
                {"field": "total_venue_booking_amount_ranked", "direction": "ASC"},
            ],
        },
    },
    "top_labeled_venue": {
        "type": SheetType.TOP,
        "title_suffix": "Part Individuelle",
        "template_tab": "Top lieux labellisés",
        "source_table": "top_labeled_venue",
        "top_n": 50,
        "select_fields": [
            "partition_month",
            "venue_name",
            "offerer_name",
            "venue_tag_name",
        ],
        "ranking": {
            "order_by": [
                {"field": "total_venue_booking_amount_ranked", "direction": "ASC"}
            ],
        },
    },
    "top_ac": {
        "type": SheetType.TOP,
        "title_suffix": "Part Collective",
        "template_tab": "Top acteurs culturels",
        "source_table": "top_ac",
        "top_n": 50,
        "select_fields": ["partition_month", "offerer_name", "total_number_of_tickets"],
        "ranking": {
            "order_by": [{"field": "total_booking_amount", "direction": "DESC"}],
        },
    },
    "top_format": {
        "type": SheetType.TOP,
        "title_suffix": "Part Collective",
        "template_tab": "Top formats",
        "source_table": "top_format",
        "top_n": 5,
        "select_fields": [
            "partition_month",
            "collective_offer_format",
            "total_booking_amount",
            "total_number_of_tickets",
        ],
        "ranking": {
            "order_by": [{"field": "total_booking_amount", "direction": "DESC"}],
        },
    },
}

TOP_TITLE_WIDTH = {
    "top_offer": 7,
    "top_offer_category": 7,
    "top_offer_label": 8,
    "top_venue": 4,
    "top_labeled_venue": 5,
    "top_ac": 4,
    "top_format": 5,
}


def default_title_layout():
    return {
        "title_row_offset": 0,
        "title_col_offset": 0,
        "title_height": 3,
        "title_width": 1,
    }


MAX_COLUMNS = 30  # Maximum number of columns to consider in the template

SHEET_LAYOUT = defaultdict(
    default_title_layout,
    {
        "top": {
            "title_row_offset": 0,
            "title_col_offset": 0,
            "title_height": 3,
            "title_width": TOP_TITLE_WIDTH,
            "freeze_panes": {"row": 4},
        },
        "kpis": {
            "title_row_offset": 0,
            "title_col_offset": 3,
            "title_height": 3,
            "title_width": "dynamic",
            "freeze_panes": {"row": 4},
        },
    },
)


AGG_TYPE_MAPPING = {
    "Par an: somme sur l'année": "sum",
    "Par an: moyenne sur l'année": "wavg",
    "Par an: maximum": "max",
    "Par an: Août": "august",
    "Par an: décembre": "december",
}

# Default aggregation if not found or invalid
DEFAULT_AGG_TYPE = "sum"

# Months to display in KPI sheets (shift relative to the consolidation month)
KPI_MONTHS_SHIFT_DISPLAYED = [-3, -2, -1, -13]
