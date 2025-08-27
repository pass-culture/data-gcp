# Columns that should be converted to numeric types
import ast
import html
import json
from pathlib import Path

import pandas as pd
from loguru import logger

NUMERIC_COLUMNS = [
    "prix",
    "taux_tva",
    "code_tva",
    "dateparutionentier",
    "datecreationentier",
    "dispo",
    "image",
    "image_spe",
    "image_4",
    "nbmag",
    "drm",
    "biblio",
    "extrait",
    "extrait_oeuvre",
    "pages",
    "longueur",
    "largeur",
    "epaisseur",
    "poids",
    "scolaire",
    "livre_etranger",
    "stock",
    "public_averti",
    "precommande",
    "ispromotion",
    "isnew",
    "drmlcp",
    "id_lectorat",
    "livre_lu",
    "grands_caracteres",
    "multilingue",
    "illustre",
    "luxe",
    "relie",
    "broche",
    "NombreMagasins",
    "collector",
    "contributor_id",
    "typeproduit",
    "csv_index",
    "ean",
    "magid",
    "article_index",
    "nombre",
]

# Data extraction field definitions
MAIN_OEUVRE_FIELDS = ["id", "titre", "auteurs", "typeproduit"]

COMPLEX_FIELDS = [
    "auteurs_multi",
    "auteurs_id",
    "auteurs_fonctions",
    "biographies",
    "websites",
    "contributor",
]

ARTICLE_SIMPLE_FIELDS = [
    "nombre",
    "gencod",
    "editeur",
    "distributeur",
    "code_distributeur",
    "diffuseur",
    "prix",
    "taux_tva",
    "code_tva",
    "dateparution",
    "dateparutionentier",
    "datecreation",
    "datecreationentier",
    "datemodification",
    "dispo",
    "libelledispo",
    "resume",
    "collection",
    "collection_no",
    "image",
    "image_spe",
    "image_4",
    "detailUrl",
    "langue",
    "langueiso",
    "langueflag",
    "nbmag",
    "codesupport",
    "libellesupport",
    "drm",
    "biblio",
    "extrait",
    "extrait_oeuvre",
    "pages",
    "longueur",
    "largeur",
    "epaisseur",
    "poids",
    "serie",
    "idserie",
    "scolaire",
    "lectorat",
    "livre_etranger",
    "dewey",
    "racine_editeur",
    "code_clil",
    "type_prix",
    "traducteur",
    "langueorigine",
    "iad",
    "id_code_langue",
    "stock",
    "cmb",
    "public_averti",
    "precommande",
    "ispromotion",
    "isnew",
    "drmlcp",
    "id_lectorat",
    "livre_lu",
    "grands_caracteres",
    "multilingue",
    "illustre",
    "luxe",
    "relie",
    "broche",
    "NombreMagasins",
    "collector",
    "code_editeur",
    "rentree_litteraire_annee",
    "rentree_litteraire_type",
    "premier_roman",
    "commentaire",
    "sommaire",
    "couleur_couverture",
    "couleur_couverture_code",
]

ARTICLE_COMPLEX_FIELDS = [
    "id_catalogue",
    "prixpays",
    "dispopays",
    "imagesUrl",
    "biographies",
    "websites",
    "gtl",
    "bonus_extraits",
    "replace",
    "contributor",
]

# Analysis configuration
ANALYSIS_SAMPLE_COLUMNS = {
    "main": ["typeproduit", "auteurs"],
    "contributors": ["source"],
}


def safe_eval_string(s: str) -> dict | None:
    """
    Safely evaluate a string that should contain a Python dictionary.

    Args:
        s: String representation of a dictionary

    Returns:
        Dictionary if parsing successful, None otherwise
    """
    try:
        # First try to evaluate as Python literal
        return ast.literal_eval(s)
    except (ValueError, SyntaxError):
        try:
            # Try JSON parsing (in case it's JSON format)
            return json.loads(s)
        except json.JSONDecodeError:
            logger.warning(f"Could not parse string: {s[:100]}...")
            return None


def parse_csv_data(csv_path: str) -> pd.DataFrame:
    """
    Parse the CSV file and return the main DataFrame.

    Args:
        csv_path: Path to the CSV file containing oeuvre data

    Returns:
        DataFrame with parsed data

    Raises:
        FileNotFoundError: If the CSV file doesn't exist
        pd.errors.EmptyDataError: If the CSV file is empty
    """
    logger.info(f"Loading CSV from {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows")
    return df


def extract_oeuvre_data(df: pd.DataFrame) -> list[dict]:
    """
    Extract and parse oeuvre data from the DataFrame.

    Args:
        df: Main DataFrame containing oeuvre column

    Returns:
        List of parsed oeuvre dictionaries with metadata

    Note:
        Adds metadata fields prefixed with '_' for CSV index, EAN, and MAGID
    """
    logger.info("Extracting oeuvre data...")
    oeuvre_data = []

    for idx, row in df.iterrows():
        if pd.isna(row["oeuvre"]):
            continue

        parsed_oeuvre = safe_eval_string(row["oeuvre"])
        if parsed_oeuvre:
            # Add metadata from the main row
            parsed_oeuvre.update(
                {"_csv_index": idx, "_ean": row.get("ean"), "_magid": row.get("magid")}
            )
            oeuvre_data.append(parsed_oeuvre)

    logger.info(f"Successfully parsed {len(oeuvre_data)} oeuvre records")
    return oeuvre_data


def create_oeuvre_dataframe(oeuvre_data: list[dict]) -> pd.DataFrame:
    """
    Create main oeuvre DataFrame with basic book information.

    Args:
        oeuvre_data: List of parsed oeuvre dictionaries

    Returns:
        DataFrame with main oeuvre information
    """
    logger.info("Creating main oeuvre DataFrame...")

    main_records = []
    for oeuvre in oeuvre_data:
        # Extract simple fields
        record = {field: oeuvre.get(field) for field in MAIN_OEUVRE_FIELDS}

        # Add metadata fields
        record.update(
            {
                "csv_index": oeuvre.get("_csv_index"),
                "ean": oeuvre.get("_ean"),
                "magid": oeuvre.get("_magid"),
            }
        )

        # Add complex fields as JSON strings
        for field in COMPLEX_FIELDS:
            default_value = [] if field != "auteurs_multi" else {}
            record[field] = json.dumps(oeuvre.get(field, default_value))

        # Add article fields
        first_article = oeuvre.get("article", [{}])[
            0
        ]  # Titelive only returns one article

        if first_article:
            article_record = extract_article_fields(first_article)
            record.update(article_record)

        main_records.append(record)

    df_main = pd.DataFrame(main_records)
    logger.info(f"Created main DataFrame with {len(df_main)} records")
    return df_main


def parse_gtl_field(gtl_dict: dict) -> dict:
    """
    Parse GTL field and extract GTL IDs into separate columns.

    Args:
        gtl_dict: JSON string containing GTL data

    Returns:
        Dictionary with gtl_1_id, gtl_2_id, gtl_3_id, gtl_4_id columns
    """
    # Initialize result with None values
    result = {
        "gtl_1_id": None,
        "gtl_1_label": None,
        "gtl_2_id": None,
        "gtl_2_label": None,
        "gtl_3_id": None,
        "gtl_3_label": None,
        "gtl_4_id": None,
        "gtl_4_label": None,
    }

    # Handle empty cases
    if not gtl_dict:
        return result

    # Extract the "first" part (ignoring other parts as we only get first key)
    first_part = gtl_dict.get("first", {})
    if not first_part:
        return result

    # Extract up to 4 GTL codes
    for i in range(1, 5):  # GTL keys are "1", "2", "3", "4"
        gtl_key = str(i)
        if gtl_key in first_part:
            gtl_data = first_part[gtl_key]
            if isinstance(gtl_data, dict):
                if "code" in gtl_data:
                    result[f"gtl_{i}_id"] = gtl_data["code"]
                if "libelle" in gtl_data:
                    result[f"gtl_{i}_label"] = html.unescape(gtl_data["libelle"])

    return result


def extract_article_fields(article: dict) -> dict:
    """Extract simple and complex fields from an article dictionary."""
    # Extract simple fields
    record = {field: article.get(field) for field in ARTICLE_SIMPLE_FIELDS}

    # Add complex fields as JSON strings
    for field in ARTICLE_COMPLEX_FIELDS:
        default_value = [] if field in ["biographies", "websites", "replace"] else {}
        record[field] = json.dumps(article.get(field, default_value))

    # Parse GTL field and add GTL ID columns
    gtl_parsed = parse_gtl_field(article.get("gtl"))
    record.update(gtl_parsed)
    return record


def extract_contributor_record(
    contrib: dict, oeuvre_id: str, source: str, article_index: int | None = None
) -> dict:
    """Extract contributor information into a standardized record."""
    return {
        "oeuvre_id": oeuvre_id,
        "source": source,
        "article_index": article_index,
        "contributor_id": contrib.get("id"),
        "name": contrib.get("name"),
        "principal": contrib.get("principal"),
        "role": json.dumps(contrib.get("role", {})),
    }


def create_contributors_dataframe(oeuvre_data: list[dict]) -> pd.DataFrame:
    """
    Create contributors DataFrame with contributor information from articles.

    Args:
        oeuvre_data: List of parsed oeuvre dictionaries

    Returns:
        DataFrame with contributor information
    """
    logger.info("Creating contributors DataFrame...")

    contributor_records = []
    for oeuvre in oeuvre_data:
        oeuvre_id = oeuvre.get("id")

        # Get contributors from main oeuvre level
        main_contributors = oeuvre.get("contributor", [])
        if isinstance(main_contributors, list):
            for contrib in main_contributors:
                if isinstance(contrib, dict):
                    record = extract_contributor_record(contrib, oeuvre_id, "oeuvre")
                    contributor_records.append(record)

        # Get contributors from articles
        article = oeuvre.get("article", [{}])[0]
        if isinstance(article, dict):
            article_contributors = article.get("contributor", [])
            if isinstance(article_contributors, list):
                for contrib in article_contributors:
                    if isinstance(contrib, dict):
                        record = extract_contributor_record(
                            contrib, oeuvre_id, "article", 0
                        )
                        contributor_records.append(record)

    df_contributors = pd.DataFrame(contributor_records)
    logger.info(f"Created contributors DataFrame with {len(df_contributors)} records")
    return df_contributors


def save_csv_files(
    df_main: pd.DataFrame,
    df_contributors: pd.DataFrame,
    output_path: Path,
):
    """Save DataFrames as CSV files."""
    df_main.to_csv(output_path / "oeuvre_main.csv", index=False)
    df_contributors.to_csv(output_path / "oeuvre_contributors.csv", index=False)


def save_parquet_files(
    df_main: pd.DataFrame,
    df_contributors: pd.DataFrame,
    output_path: Path,
):
    """Save DataFrames as Parquet files with data cleaning."""
    try:
        # Clean DataFrames for Parquet compatibility
        df_main_clean = clean_dataframe_for_parquet(df_main)
        df_contributors_clean = clean_dataframe_for_parquet(df_contributors)

        df_main_clean.to_parquet(output_path / "oeuvre_main.parquet", index=False)
        df_contributors_clean.to_parquet(
            output_path / "oeuvre_contributors.parquet", index=False
        )
        logger.info("Also saved as Parquet files")
    except Exception as e:
        logger.warning(f"Could not save Parquet files: {e}")


def clean_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame to ensure data types are compatible with Parquet format.

    Args:
        df: DataFrame to clean

    Returns:
        Cleaned DataFrame
    """
    df_cleaned = df.copy()

    # Clean numeric columns
    for col in NUMERIC_COLUMNS:
        if col in df_cleaned.columns:
            # Convert to numeric, coercing errors to NaN
            df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors="coerce")

    # Convert remaining object columns to strings for consistency
    object_columns = df_cleaned.select_dtypes(include=["object"]).columns

    for col in object_columns:
        if col not in NUMERIC_COLUMNS:  # Don't convert numeric columns to string
            # Convert all values to strings, handling NaN values
            df_cleaned[col] = df_cleaned[col].astype(str).replace("nan", None)

    return df_cleaned
