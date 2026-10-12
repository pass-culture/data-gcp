# Brevo ETL — Data Extraction Summary

**Base URL:** `https://api.brevo.com/v3`

## Two Audiences

| Audience | GCP Secret Manager Secret ID |
|----------|------------------------------|
| native | `sendinblue-api-key-{env}` |
| pro | `sendinblue-pro-api-key-{env}` |

API keys are retrieved at runtime from **Google Cloud Secret Manager** (`projects/{GCP_PROJECT}/secrets/{secret_id}/versions/latest`).

## Endpoints

### 1. Newsletter Campaigns

- **Endpoint:** `GET /v3/emailCampaigns`
- **Params:** `status=sent`, `limit=50`, `offset`, `statistics=globalStats`, optional `startDate`/`endDate`
- **Data extracted per campaign:**

| API field | Destination column |
|-----------|--------------------|
| `id` | `campaign_id` |
| `tag` | `campaign_utm` |
| `name` | `campaign_name` |
| `sentDate` | `campaign_sent_date` |
| `shareLink` | `share_link` |
| `statistics.globalStats.delivered` | `audience_size` |
| `statistics.globalStats.unsubscriptions` | `unsubscriptions` |
| `statistics.globalStats.uniqueViews` | `open_number` |

- **Destination:** `raw_{env}.{brevo_newsletters|brevo_pro_newsletters}_histo` (partitioned by `update_date`)

### 2. SMTP Templates

- **Endpoint:** `GET /v3/smtp/templates`
- **Params:** `templateStatus=true` (active only), `offset`
- **Data extracted:** `id` and `tag` per template (used to query events in the next endpoint)
- **Not stored directly** — used as input for the transactional events pipeline

### 3. Transactional Email Events

- **Endpoint:** `GET /v3/smtp/statistics/events`
- **Params:** `templateId`, `event` (one of `delivered`/`opened`/`unsubscribed`), `startDate`, `endDate`, `offset`
- **Pagination:** batches of 2500 events
- **Data extracted per event:**

| API field | Destination column |
|-----------|--------------------|
| template `id` (from loop) | `template` |
| template `tag` (from loop) | `tag` |
| `email` | `email` |
| `event` | aggregated into `delivered_count`, `opened_count`, `unsubscribed_count` |
| `date` | `event_date` |

- **Aggregation:** performed in `utils.py:transform_events_to_dataframe`. Raw events from the API arrive as individual rows (one per event occurrence). The function groups them by `(tag, template, email, event, event_date)`, counts occurrences, then pivots the `event` column into three count columns: `delivered_count`, `opened_count`, `unsubscribed_count`. Result: one row per `(template, tag, email, event_date)`.
- **Destination:** `tmp_{env}.{YYYYMMDD}_brevo_transactional_detailed_histo` — this is a **staging table**, not consumed by the Python ETL itself but by a downstream Airflow SQL task (see below).

## Airflow DAG Orchestration

**DAG:** `import_brevo_v2` (`orchestration/dags/jobs/import/import_brevo.py`)

The DAG ensures the tmp table is filled before the SQL tasks read from it via Airflow's `>>` dependency operator. The full execution chain:

```
gce_instance_start
  >> fetch_install_code
  >> import_pro_transactional_data_to_tmp      ─┐
  >> import_native_transactional_data_to_tmp   ─┤  Python ETL (fills tmp table)
  >> import_pro_newsletter_data_to_raw         ─┤
  >> import_native_newsletter_data_to_raw      ─┘
  >> gce_instance_stop
  >> end_job
  >> raw_table_tasks       ← SQL reads from tmp table, joins with user/venue emails
  >> end_raw
  >> clean_table_tasks     ← further cleaning
  >> end_clean
  >> analytics_table_tasks ← final analytics tables
```

The `raw_table_tasks` step runs `brevo_transactional.sql` which:
- Reads from `tmp_{env}.{YYYYMMDD}_brevo_transactional_detailed_histo`
- Joins with **user emails** (from applicative DB) to resolve `user_id`
- Joins with **venue booking emails** and **collective offer emails** to resolve `offerer_id`
- Hashes the email into `email_id` via `sha256`
- Writes to `raw_{env}.brevo_transactional` (partitioned by `execution_date`)

## Batch Pagination & Aggregation Safety

The API delivers events in pages of 2500. A natural concern is whether events belonging to the same `(template, tag, email, event_date)` group could be split across pages, causing incorrect counts.

**This is not an issue.** The Python ETL accumulates all pages into a single `all_events` list before any aggregation occurs:
- Sync path (`utils.py:454-508`): the `while True` loop appends every page to `all_events`, then calls `transform_events_to_dataframe` once on the full list.
- Async path (`utils.py:550-573`): all template results are gathered via `asyncio.gather`, extended into `all_events`, then aggregated once.

The pagination is purely a fetch mechanism. The groupby/pivot sees the complete dataset.

## Proposal: Moving Aggregation to SQL

Currently the Python ETL aggregates raw events (groupby + pivot) before writing to the tmp table. An alternative is to write raw unaggregated events to the tmp table and let SQL handle the aggregation.

### Files to modify

1. **`jobs/etl_jobs/jobs/brevo/config.py`**
   - Change `transactional_histo_schema` to match raw event rows:
     ```python
     transactional_histo_schema = {
         "template": "INTEGER",
         "tag": "STRING",
         "email": "STRING",
         "event": "STRING",
         "event_date": "DATE",
         "target": "STRING",
     }
     ```

2. **`jobs/etl_jobs/jobs/brevo/utils.py`**
   - Remove `transform_events_to_dataframe` function
   - In `etl_transactional` and `async_etl_transactional`: replace the transform call with a direct `pd.DataFrame(all_events)` construction, adding the `target` column
   - Remove the numpy import (only used for aggregation fill values)

3. **`orchestration/dags/dependencies/brevo/sql/raw/brevo_transactional.sql`**
   - Add aggregation logic that was previously in Python. The query currently reads pre-aggregated rows; it would need to group and pivot:
     ```sql
     with
         raw_events as (
             select * from `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(today()) }}_brevo_transactional_detailed_histo`
         ),
         aggregated as (
             select
                 template,
                 tag,
                 email,
                 event_date,
                 target,
                 countif(event = 'delivered') as delivered_count,
                 countif(event = 'opened') as opened_count,
                 countif(event = 'unsubscribed') as unsubscribed_count
             from raw_events
             group by template, tag, email, event_date, target
         ),
         -- ... existing user_emails, venue_emails, collective_offer_emails CTEs ...
     select distinct
         template, tag, user_id, to_hex(sha256(email)) as email_id, target,
         coalesce(distinct_venue_emails.offerer_id, co.offerer_id) as offerer_id,
         event_date, delivered_count, opened_count, unsubscribed_count,
         date("{{ ds }}") as execution_date
     from aggregated s
     left join user_emails on s.email = user_emails.user_email
     left join distinct_venue_emails on s.email = distinct_venue_emails.venue_email
     left join collective_offer_emails_offerer as co on s.email = co.collective_offer_email
     ```

### Trade-offs

| Aspect            | Current (Python aggregation)                         | Proposed (SQL aggregation)                              |
| ----------------- | ---------------------------------------------------- | ------------------------------------------------------- |
| Tmp table size    | Smaller (pre-aggregated)                             | Larger (one row per raw event)                          |
| Python complexity | Higher (groupby + pivot logic)                       | Lower (just write raw data)                             |
| SQL complexity    | Lower (reads pre-aggregated)                         | Higher (aggregation + joins)                            |
| Debuggability     | Aggregated data only in tmp                          | Raw events preserved in tmp, easier to inspect          |
| Consistency       | Aggregation logic in Python, separate from SQL joins | All transformation logic in SQL, single source of truth |
