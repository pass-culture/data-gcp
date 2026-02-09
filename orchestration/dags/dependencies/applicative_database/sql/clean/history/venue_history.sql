select
    date_add(current_date(), interval -1 day) as partition_date,
    venue_id,
    venue_siret,
    venue_is_permanent,
    venue_type_code,
    venue_activity,
    venue_label_id,
    banner_url,
    venue_description,
    venue_audiodisabilitycompliant,
    venue_mentaldisabilitycompliant,
    venue_motordisabilitycompliant,
    venue_visualdisabilitycompliant,
    venue_withdrawal_details
from `{{ bigquery_raw_dataset }}`.applicative_database_venue
