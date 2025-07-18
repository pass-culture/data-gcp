import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

import typer
from brevo_python.rest import ApiException

from brevo_newsletters import BrevoNewsletters
from brevo_transactional import BrevoTransactional
from utils import (
    BIGQUERY_RAW_DATASET,
    BIGQUERY_TMP_DATASET,
    ENV_SHORT_NAME,
    GCP_PROJECT,
    access_secret_data,
    campaigns_histo_schema,
    transactional_histo_schema,
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f"brevo_import_{ENV_SHORT_NAME}.log"),
    ],
)

# Constants
NATIVE_API_KEY = access_secret_data(GCP_PROJECT, f"sendinblue-api-key-{ENV_SHORT_NAME}")
PRO_API_KEY = access_secret_data(
    GCP_PROJECT, f"sendinblue-pro-api-key-{ENV_SHORT_NAME}"
)
TRANSACTIONAL_TABLE_NAME = "sendinblue_transactional_detailed"
UPDATE_WINDOW = 31 if ENV_SHORT_NAME == "prod" else 500

today = datetime.now(tz=timezone.utc)
yesterday = date.today() - timedelta(days=1)


def parse_date_string(date_str: str) -> datetime:
    """Parse date string to datetime object with timezone."""
    try:
        # Try different date formats
        for fmt in ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]:
            try:
                parsed_date = datetime.strptime(date_str, fmt)
                # Add timezone info
                return parsed_date.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        # If none worked, raise error
        raise ValueError(f"Unable to parse date: {date_str}")

    except Exception as e:
        logging.error(f"Error parsing date '{date_str}': {e}")
        raise typer.BadParameter(
            f"Invalid date format: {date_str}. Use YYYY-MM-DD format."
        )


def validate_inputs(
    target: str, audience: str, start_date: str, end_date: str
) -> tuple:
    """Validate and parse input parameters."""

    # Validate target
    valid_targets = ["newsletter", "transactional"]
    if target not in valid_targets:
        raise typer.BadParameter(
            f"Invalid target '{target}'. Must be one of: {', '.join(valid_targets)}"
        )

    # Validate audience
    valid_audiences = ["native", "pro"]
    if audience not in valid_audiences:
        raise typer.BadParameter(
            f"Invalid audience '{audience}'. Must be one of: {', '.join(valid_audiences)}"
        )

    # Parse and validate dates
    try:
        parsed_start = parse_date_string(start_date)
        parsed_end = parse_date_string(end_date)

        if parsed_start > parsed_end:
            raise typer.BadParameter("Start date cannot be after end date")

        if parsed_end > today:
            logging.warning(
                f"End date {end_date} is in the future. Using today's date instead."
            )
            parsed_end = today

        # Check date range is reasonable
        date_diff = (parsed_end - parsed_start).days
        if date_diff > 365:
            logging.warning(
                f"Large date range detected: {date_diff} days. This may take a long time."
            )
        elif date_diff < 0:
            raise typer.BadParameter("Invalid date range")

        return parsed_start, parsed_end

    except Exception as e:
        if isinstance(e, typer.BadParameter):
            raise
        logging.error(f"Date validation error: {e}")
        raise typer.BadParameter(f"Date validation failed: {e}")


def check_api_rate_limit(api_key: str, audience: str) -> dict:
    """
    Check the current rate limit status for the API key.
    """
    import brevo_python

    try:
        configuration = brevo_python.Configuration()
        configuration.api_key["api-key"] = api_key
        api_instance = brevo_python.TransactionalEmailsApi(
            brevo_python.ApiClient(configuration)
        )

        # Make a lightweight API call to check status
        api_instance.get_smtp_templates(template_status="true", limit=1, offset=0)

        return {
            "status": "ok",
            "message": f"API accessible for {audience} audience",
            "can_proceed": True,
        }

    except ApiException as e:
        if e.status == 429:
            rate_limit_info = {
                "status": "rate_limited",
                "can_proceed": False,
                "reset_time": None,
                "remaining": None,
                "limit": None,
            }

            if hasattr(e, "http_resp") and hasattr(e.http_resp, "headers"):
                headers = e.http_resp.headers
                reset_time = headers.get("x-sib-ratelimit-reset")
                remaining = headers.get("x-sib-ratelimit-remaining")
                limit = headers.get("x-sib-ratelimit-limit")

                rate_limit_info.update(
                    {
                        "reset_time": int(reset_time) if reset_time else None,
                        "remaining": int(remaining) if remaining else None,
                        "limit": int(limit) if limit else None,
                    }
                )

                reset_minutes = f"{int(reset_time)/60:.1f}" if reset_time else "unknown"
                rate_limit_info["message"] = (
                    f"Rate limit exceeded for {audience} audience. "
                    f"Limit: {limit}, Remaining: {remaining}, "
                    f"Reset in: {reset_time} seconds ({reset_minutes} minutes)"
                )
            else:
                rate_limit_info["message"] = (
                    f"Rate limit exceeded for {audience} audience (no details available)"
                )

            return rate_limit_info
        else:
            return {
                "status": "error",
                "message": f"API error for {audience} audience: {e}",
                "can_proceed": False,
            }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Unexpected error for {audience} audience: {e}",
            "can_proceed": False,
        }


def wait_for_rate_limit_reset(reset_time: int, audience: str):
    """
    Wait for the rate limit to reset with progress updates.
    """
    if reset_time <= 0:
        return

    wait_time = reset_time + 10  # Add 10 second buffer
    logging.info(
        f"Rate limit exceeded for {audience}. Waiting {wait_time} seconds ({wait_time/60:.1f} minutes)..."
    )

    # Show progress every 30 seconds
    while wait_time > 0:
        if wait_time % 30 == 0 or wait_time <= 10:
            logging.info(f"  {wait_time} seconds remaining...")
        time.sleep(1)
        wait_time -= 1

    logging.info(f"Rate limit should be reset for {audience}. Proceeding...")


def run(
    target: str = typer.Option(
        ...,
        help="Target type: 'newsletter' or 'transactional'",
    ),
    audience: str = typer.Option(
        ...,
        help="Audience type: 'native' or 'pro'",
    ),
    start_date: str = typer.Option(
        ..., help="Start date for import (YYYY-MM-DD format)"
    ),
    end_date: str = typer.Option(..., help="End date for import (YYYY-MM-DD format)"),
    force: bool = typer.Option(
        False, help="Force execution even if rate limited (will wait for reset)"
    ),
    check_only: bool = typer.Option(
        False, help="Only check rate limit status, don't run import"
    ),
):
    """
    Import Brevo email statistics to BigQuery with rate limit handling.

    Examples:
        python main.py --target newsletter --audience native --start-date 2024-01-01 --end-date 2024-01-31
        python main.py --target transactional --audience pro --start-date 2024-07-01 --end-date 2024-07-18
        python main.py --target transactional --audience pro --start-date 2024-07-17 --end-date 2024-07-17 --check-only
        python main.py --target transactional --audience pro --start-date 2024-07-17 --end-date 2024-07-17 --force
    """

    # Log startup info
    script_start = datetime.now()
    logging.info("=" * 60)
    logging.info("Starting Brevo import script")
    logging.info(f"Environment: {ENV_SHORT_NAME}")
    logging.info(f"Target: {target}")
    logging.info(f"Audience: {audience}")
    logging.info(f"Date range: {start_date} to {end_date}")
    logging.info("=" * 60)

    try:
        # Validate inputs
        parsed_start, parsed_end = validate_inputs(
            target, audience, start_date, end_date
        )
        logging.info(f"Parsed date range: {parsed_start.date()} to {parsed_end.date()}")

        # Set up API key and table name based on audience
        if audience == "native":
            API_KEY = NATIVE_API_KEY
            NEWSLETTERS_TABLE_NAME = "sendinblue_newsletters"
            logging.info("Using native API key and table")
        elif audience == "pro":
            API_KEY = PRO_API_KEY
            NEWSLETTERS_TABLE_NAME = "sendinblue_pro_newsletters"
            logging.info("Using pro API key and table")

        # Verify API key exists
        if not API_KEY:
            raise ValueError(f"Failed to retrieve API key for {audience} audience")

        # Check rate limit status
        logging.info(f"Checking rate limit status for {audience} audience...")
        rate_limit_status = check_api_rate_limit(API_KEY, audience)

        logging.info(f"Rate limit status: {rate_limit_status['message']}")

        if check_only:
            logging.info("Check-only mode. Exiting.")
            return "check_complete"

        if not rate_limit_status["can_proceed"]:
            if rate_limit_status["status"] == "rate_limited":
                if force and rate_limit_status["reset_time"]:
                    wait_for_rate_limit_reset(rate_limit_status["reset_time"], audience)
                else:
                    logging.error("Rate limit exceeded. Options:")
                    logging.error("  1. Wait and retry later")
                    logging.error("  2. Use --force to wait for reset automatically")
                    if rate_limit_status["reset_time"]:
                        logging.error(
                            f"  3. Wait {rate_limit_status['reset_time']} seconds ({rate_limit_status['reset_time']/60:.1f} minutes)"
                        )
                    raise typer.Exit(1)
            else:
                logging.error(f"Cannot proceed: {rate_limit_status['message']}")
                raise typer.Exit(1)

        # Proceed with import
        if target == "newsletter":
            return process_newsletters(
                API_KEY, NEWSLETTERS_TABLE_NAME, audience, parsed_start, parsed_end
            )
        else:
            return process_transactional(API_KEY, audience, parsed_start, parsed_end)

    except typer.BadParameter as e:
        logging.error(f"Parameter error: {e}")
        raise
    except Exception as e:
        script_duration = datetime.now() - script_start
        logging.error(f"Script failed after {script_duration}: {e}")
        logging.error("Full error details:", exc_info=True)
        raise typer.Exit(1)
    finally:
        script_duration = datetime.now() - script_start
        logging.info(f"Script completed in {script_duration}")
        logging.info("=" * 60)


def process_newsletters(
    api_key: str,
    table_name: str,
    audience: str,
    start_date: datetime,
    end_date: datetime,
) -> str:
    """Process newsletter campaigns with rate limit handling."""

    process_start = datetime.now()
    logging.info("Starting newsletter processing...")

    try:
        # Use the UPDATE_WINDOW logic as in original for newsletters
        newsletter_start = today - timedelta(days=UPDATE_WINDOW)
        newsletter_end = today

        logging.info(
            f"Newsletter date range (using UPDATE_WINDOW): {newsletter_start.date()} to {newsletter_end.date()}"
        )

        brevo_newsletters = BrevoNewsletters(
            gcp_project=GCP_PROJECT,
            raw_dataset=BIGQUERY_RAW_DATASET,
            api_key=api_key,
            destination_table_name=table_name,
            start_date=newsletter_start,
            end_date=newsletter_end,
        )

        logging.info("Creating newsletter API instance...")
        brevo_newsletters.create_instance_email_campaigns_api()

        logging.info("Fetching campaign data...")
        df = brevo_newsletters.get_data()

        if not df.empty:
            df["campaign_target"] = audience
            logging.info(f"Adding audience tag '{audience}' to {len(df)} campaigns")

            logging.info("Saving to BigQuery...")
            brevo_newsletters.save_to_historical(df, campaigns_histo_schema)

            process_duration = datetime.now() - process_start
            logging.info(
                f"Newsletter processing completed successfully in {process_duration}"
            )
            logging.info(f"Final dataset: {len(df)} campaigns")
        else:
            logging.warning("No newsletter data found for the specified period")
            process_duration = datetime.now() - process_start
            logging.info(
                f"Newsletter processing completed (no data) in {process_duration}"
            )

        return "success"

    except Exception as e:
        process_duration = datetime.now() - process_start
        logging.error(f"Newsletter processing failed after {process_duration}: {e}")
        logging.error("Full error details:", exc_info=True)
        raise


def process_transactional(
    api_key: str, audience: str, start_date: datetime, end_date: datetime
) -> str:
    """Process transactional email events with rate limit handling."""

    process_start = datetime.now()
    logging.info("Starting transactional email processing...")

    try:
        # Convert datetime back to string format for transactional API
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        logging.info(f"Transactional date range: {start_date_str} to {end_date_str}")

        brevo_transactional = BrevoTransactional(
            gcp_project=GCP_PROJECT,
            tmp_dataset=BIGQUERY_TMP_DATASET,
            api_key=api_key,
            destination_table_name=TRANSACTIONAL_TABLE_NAME,
            start_date=start_date_str,
            end_date=end_date_str,
        )

        logging.info("Creating transactional API instance...")
        brevo_transactional.create_instance_transactional_email_api()

        event_types = ["delivered", "opened", "unsubscribed"]
        all_events = []

        for i, event_type in enumerate(event_types):
            event_start = datetime.now()
            logging.info(
                f"Processing event type {i+1}/{len(event_types)}: {event_type}"
            )

            events = brevo_transactional.get_events(event_type)
            all_events.extend(events)

            event_duration = datetime.now() - event_start
            logging.info(
                f"Completed {event_type}: {len(events)} events in {event_duration}"
            )

            # Estimate remaining time
            if i < len(event_types) - 1:
                avg_time_per_event_type = (datetime.now() - process_start) / (i + 1)
                remaining_time = avg_time_per_event_type * (len(event_types) - i - 1)
                logging.info(f"Estimated remaining time: {remaining_time}")

        logging.info(f"Total events collected: {len(all_events)}")

        # Parse and save data
        logging.info("Parsing events to DataFrame...")
        df = brevo_transactional.parse_to_df(all_events)

        if not df.empty:
            df["target"] = audience
            logging.info(f"Processing {len(df)} event records")

            logging.info("Saving to BigQuery...")
            brevo_transactional.save_to_historical(df, transactional_histo_schema)

            process_duration = datetime.now() - process_start
            logging.info(
                f"Transactional processing completed successfully in {process_duration}"
            )
            logging.info(f"Final dataset: {len(df)} rows")
        else:
            logging.warning("No transactional data found for the specified period")
            process_duration = datetime.now() - process_start
            logging.info(
                f"Transactional processing completed (no data) in {process_duration}"
            )

        return "success"

    except Exception as e:
        process_duration = datetime.now() - process_start
        logging.error(f"Transactional processing failed after {process_duration}: {e}")
        logging.error("Full error details:", exc_info=True)
        raise


if __name__ == "__main__":
    typer.run(run)
