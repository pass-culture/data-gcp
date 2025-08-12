import logging
from datetime import datetime
from typing import Dict, List

import pandas as pd
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery, secretmanager

from .config import (
    BIGQUERY_RAW_DATASET,
    DEPRECATED_METRICS_MAPPING,
    ENV_SHORT_NAME,
    GCP_PROJECT_ID,
    MAX_ERROR_RATE,
)

# Set up logging
logger = logging.getLogger(__name__)


def access_secret_data(
    project_id: str, secret_id: str, version_id: str = "latest", default: str = None
) -> str:
    """
    Access secret data from Google Secret Manager.

    Args:
        project_id: GCP project ID
        secret_id: Secret ID
        version_id: Version ID (default: "latest")
        default: Default value if secret access fails

    Returns:
        Secret value or default
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("UTF-8")
        logger.info(f"Successfully retrieved secret {secret_id}")
        return secret_value
    except DefaultCredentialsError as e:
        logger.warning(f"Default credentials error accessing secret {secret_id}: {e}")
        return default
    except Exception as e:
        logger.error(f"Error accessing secret {secret_id}: {e}")
        return default


def save_multiple_partitions_to_bq(
    df: pd.DataFrame,
    table_name: str,
    start_date: str,
    end_date: str,
    date_column: str,
    schema: List[bigquery.SchemaField] = None,
) -> None:
    """
    Save DataFrame to BigQuery with daily partitioning.

    Args:
        df: DataFrame to save
        table_name: BigQuery table name
        start_date: Start date for partitioning
        end_date: End date for partitioning
        date_column: Column to use for partitioning
        schema: BigQuery schema
    """
    if df.shape[0] > 0:
        logger.info(f"Processing {table_name} with {df.shape[0]} rows")

        df[date_column] = pd.to_datetime(df[date_column])
        _dates = pd.date_range(start_date, end_date)

        logger.info(
            f"Will save {table_name} -> {df.shape[0]} rows across {len(_dates)} partitions"
        )

        for event_date in _dates:
            date_str = event_date.strftime("%Y-%m-%d")
            tmp_df = df[df[date_column].dt.date == pd.to_datetime(date_str).date()]

            if tmp_df.shape[0] > 0:
                tmp_df = tmp_df.copy()
                tmp_df.loc[:, date_column] = tmp_df[date_column].astype(str)
                logger.info(
                    f"Saving {tmp_df.shape[0]} rows to {table_name} for {date_str}"
                )
                df_to_bq(tmp_df, table_name, date_str, date_column, schema)
            else:
                logger.debug(f"No data for {table_name} on {date_str}")
    else:
        logger.warning(f"No data to save for {table_name}")


def df_to_bq(
    df: pd.DataFrame,
    table_name: str,
    event_date: str,
    date_column: str = "export_date",
    schema: List[bigquery.SchemaField] = None,
) -> None:
    """
    Save DataFrame to BigQuery partitioned table.

    Args:
        df: DataFrame to save
        table_name: BigQuery table name
        event_date: Event date for partitioning
        date_column: Date column name
        schema: BigQuery schema
    """
    try:
        date_fmt = datetime.strptime(event_date, "%Y-%m-%d")
        yyyymmdd = date_fmt.strftime("%Y%m%d")

        df = df.copy()
        df.loc[:, date_column] = date_fmt

        bigquery_client = bigquery.Client()
        table_id = f"{GCP_PROJECT_ID}.{BIGQUERY_RAW_DATASET}.{table_name}${yyyymmdd}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
            ],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=date_column,
            ),
            schema=schema,
        )

        logger.info(f"Loading {df.shape[0]} rows to {table_id}")
        job = bigquery_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()
        logger.info(f"Successfully loaded data to {table_id}")

    except Exception as e:
        logger.error(f"Failed to load data to BigQuery table {table_name}: {e}")
        raise


def preprocess_insight_data(insights_data: List[Dict]) -> pd.DataFrame:
    """
    Process raw insights data into a pandas DataFrame with v23.0 compatibility.

    Args:
        insights_data: Raw insights data as returned by API

    Returns:
        Processed insights DataFrame
    """
    logger.info(f"Processing {len(insights_data)} insight records")

    rows = []
    for insight in insights_data:
        data_points = insight.get("data", [])
        row = {}

        for data_point in data_points:
            metric_name = data_point.get("name")
            values = data_point.get("values", [{}])

            if values:
                value = values[0].get("value")
                end_time = values[0].get("end_time")
                row[metric_name] = value
                row["event_date"] = end_time

        if row:
            rows.append(row)

    df = pd.DataFrame(rows)

    if not df.empty:
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
        logger.info(f"Processed insights into DataFrame with {df.shape[0]} rows")
    else:
        logger.warning("No insight data processed")

    return df


def preprocess_insight_posts(posts: List[Dict], connector) -> pd.DataFrame:
    """
    Process posts data and their insights into a pandas DataFrame.

    Args:
        posts: List of posts data
        connector: Instagram connector instance

    Returns:
        Processed posts insights DataFrame

    Raises:
        RuntimeError: If more than MAX_ERROR_RATE of post insight requests fail
    """
    logger.info(f"Processing insights for {len(posts)} posts")

    rows = []
    error_count = 0
    total_posts = len(posts)

    for i, post in enumerate(posts):
        post_id = post.get("id")
        media_type = post.get("media_type")

        logger.debug(f"Processing post {i+1}/{total_posts}: {post_id} ({media_type})")

        post_insights = connector.fetch_post_insights(post_id, media_type)

        if post_insights is None:
            error_count += 1
            logger.warning(f"Failed to get insights for post {post_id}")
            continue

        row = {
            "post_id": post_id,
            "media_type": media_type,
            "caption": post.get("caption"),
            "media_url": post.get("media_url"),
            "permalink": post.get("permalink"),
            "posted_at": post.get("timestamp"),
            "url_id": post.get("permalink", "").rstrip("/").split("/")[-1],
        }

        # Process insights data
        for data_point in post_insights.get("data", []):
            metric_name = data_point.get("name")
            values = data_point.get("values", [{}])
            if values:
                value = values[0].get("value")
                row[metric_name] = value

        rows.append(row)

    # Check error rate
    error_rate = error_count / total_posts if total_posts > 0 else 0
    if error_rate > MAX_ERROR_RATE:
        raise RuntimeError(
            f"More than {MAX_ERROR_RATE*100}% of post insight requests failed "
            f"({error_count}/{total_posts} errors)"
        )

    df = pd.DataFrame(rows)
    logger.info(
        f"Successfully processed insights for {len(df)} posts with {error_count} errors"
    )
    return df


def add_deprecated_metrics(
    df: pd.DataFrame, metric_type: str = "profile"
) -> pd.DataFrame:
    """
    Add deprecated metrics to DataFrame for schema compatibility.

    Args:
        df: DataFrame to modify
        metric_type: Type of metrics ("profile" or "post")

    Returns:
        DataFrame with deprecated metrics added
    """
    df = df.copy()

    if metric_type == "profile":
        deprecated_metrics = DEPRECATED_METRICS_MAPPING["deprecated_profile_metrics"]
        logger.info(f"Adding {len(deprecated_metrics)} deprecated profile metrics")

        for metric in deprecated_metrics:
            df[metric] = 0
            logger.debug(f"Set deprecated metric {metric} = 0")

    elif metric_type == "post":
        deprecated_metrics = DEPRECATED_METRICS_MAPPING["deprecated_post_metrics"]
        logger.info(f"Adding {len(deprecated_metrics)} deprecated post metrics")

        for metric in deprecated_metrics:
            # For video_views, we try to map from views if available, otherwise set to 0
            if metric == "video_views":
                if "views" in df.columns:
                    # For non-carousel posts, map views to video_views for videos
                    df[metric] = df.apply(
                        lambda row: row.get("views", 0)
                        if row.get("media_type") == "VIDEO"
                        else 0,
                        axis=1,
                    )
                    logger.debug("Mapped views to video_views for VIDEO posts")
                else:
                    df[metric] = 0
                    logger.debug("Set video_views = 0 (views not available)")
            else:
                df[metric] = 0

    return df


def handle_metric_replacements(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle metric replacements for v23.0 API compatibility.

    Args:
        df: DataFrame to modify

    Returns:
        DataFrame with metric replacements handled
    """
    df = df.copy()
    replacements = DEPRECATED_METRICS_MAPPING["metric_replacements"]

    for old_metric, new_metric in replacements.items():
        if new_metric in df.columns and old_metric not in df.columns:
            # If we have the new metric but not the old one, create old metric from new
            df[old_metric] = df[new_metric].fillna(0).astype(int)
            logger.info(f"Mapped {new_metric} to {old_metric} for compatibility")
        elif old_metric in df.columns and new_metric not in df.columns:
            # If we have old metric but not new, create new from old
            df[new_metric] = df[old_metric]
            logger.info(f"Mapped {old_metric} to {new_metric}")

    return df


# Get access token from secrets
ACCESS_TOKEN = access_secret_data(
    GCP_PROJECT_ID, f"facebook-access-token-{ENV_SHORT_NAME}", version_id="latest"
)

if ACCESS_TOKEN is None:
    logger.error("Failed to retrieve Facebook access token from secrets")
    raise ValueError("ACCESS_TOKEN is required but not available")
else:
    logger.info("Successfully retrieved Facebook access token")
