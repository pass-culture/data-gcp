{% test snapshot_volume_anomalies(
    model,
    remove_outliers=false,
    date_column=None,
    check_date=None,
    days_back=30,
    sensitivity=3,
    nr_sensitivity=None,
    dr_sensitivity=None
) %}

    {#
    Arguments:
    - remove_outliers: If true, excludes historical anomalies from the baseline calculation (2-pass).
                       IMPORTANT: Only removes outliers from HISTORICAL data (< target_date).
                       The target day is NEVER filtered out.
    - check_date: Override date.
    - days_back: Window size (default 30).
    - sensitivity: Z-score threshold (default 3) for Updated Row anomalies.
                   Also used for outlier removal from baseline if remove_outliers=true.
    - nr_sensitivity: New Row (Created) threshold. Defaults to 'sensitivity' if not set.
    - dr_sensitivity: Deleted Row (Removed) threshold. Defaults to 'sensitivity' if not set.
#}
    {# 1. Determine Target Date: Priority: check_date > ds (Airflow) > current_date() #}
    {% set target_date = check_date if check_date is not none else var("ds", none) %}

    {# 2. Build SQL Timestamp: If date exists use it, otherwise use SQL current_date() #}
    {% if target_date %} {% set target_ts = "timestamp('" ~ target_date ~ "')" %}
    {% else %} {% set target_ts = "timestamp(current_date())" %}
    {% endif %}

    {# 3. Calculate Boundaries in SQL #}
    {% set lower_bound = (
        "timestamp_sub(" ~ target_ts ~ ", interval " ~ days_back ~ " day)"
    ) %}
    {% set upper_bound = "timestamp_add(" ~ target_ts ~ ", interval 1 day)" %}

    {# 4. Anomaly specific sensitivities (for detection, not for outlier removal) #}
    {% set nr_sensitivity = (
        nr_sensitivity if nr_sensitivity is not none else sensitivity
    ) %}
    {% set dr_sensitivity = (
        dr_sensitivity if dr_sensitivity is not none else sensitivity
    ) %}

    with
        raw_window_data as (
            select dbt_valid_from, dbt_valid_to
            from {{ model }}
            where
                -- 1. PARTITION PRUNING: Only scan Active (NULL) or recently closed
                -- partitions
                (dbt_valid_to >= {{ lower_bound }} or dbt_valid_to is null)
                -- 2. LOGICAL FILTER: Row must overlap with the window
                and (
                    dbt_valid_from >= {{ lower_bound }}
                    or dbt_valid_to >= {{ lower_bound }}
                )
        ),

        daily_events as (
            -- 1. Count rows CREATED (New versions)
            select
                date(dbt_valid_from) as metric_date,
                count(*) as added_count,
                0 as removed_count
            from raw_window_data
            where
                dbt_valid_from >= {{ lower_bound }}
                and dbt_valid_from < {{ upper_bound }}
            group by 1

            union all

            -- 2. Count rows DELETED/UPDATED (Closed versions)
            select
                date(dbt_valid_to) as metric_date,
                0 as added_count,
                count(*) as removed_count
            from raw_window_data
            where dbt_valid_to >= {{ lower_bound }} and dbt_valid_to < {{ upper_bound }}
            group by 1
        ),

        test_set as (
            select
                metric_date,
                sum(added_count) as rows_added,
                sum(removed_count) as rows_removed
            from daily_events
            where metric_date is not null
            group by 1
        ),

        -- PASS 1: Initial stats from ALL historical data (strictly LESS THAN target
        -- date)
        initial_stats as (
            select
                avg(rows_added) as avg_added,
                coalesce(stddev(rows_added), 0) as std_added,
                avg(rows_removed) as avg_removed,
                coalesce(stddev(rows_removed), 0) as std_removed
            from test_set
            where metric_date < date({{ target_ts }})
        ),

        -- OPTIONAL PASS: Clean the history by removing outliers from BASELINE ONLY
        -- CRITICAL: The target day (metric_date = target_ts) is NEVER in this CTE
        -- because of the WHERE clause: metric_date < date({{ target_ts }})
        training_set as (
            select test_set.metric_date, test_set.rows_added, test_set.rows_removed
            from test_set
            cross join initial_stats stats
            where
                test_set.metric_date < date({{ target_ts }})  -- Only historical baseline data
                {% if remove_outliers %}

                    -- Remove historical outliers using the general sensitivity
                    -- threshold
                    -- This creates a cleaner baseline for comparison
                    and abs(test_set.rows_added - stats.avg_added)
                    <= ({{ sensitivity }} * greatest(stats.std_added, 1))
                    and abs(test_set.rows_removed - stats.avg_removed)
                    <= ({{ sensitivity }} * greatest(stats.std_removed, 1))

                {% endif %}
        ),

        -- PASS 2: Final Baseline Stats (from cleaned history if remove_outliers=true)
        final_baseline_stats as (
            select
                avg(rows_added) as avg_added,
                coalesce(stddev(rows_added), 0) as std_added,
                avg(rows_removed) as avg_removed,
                coalesce(stddev(rows_removed), 0) as std_removed
            from training_set
        ),

        -- Compare TARGET DAY against the cleaned baseline
        final_analysis as (
            select
                test_set.metric_date,
                test_set.rows_added,
                test_set.rows_removed,
                stats.avg_added,
                stats.std_added,
                stats.avg_removed,
                stats.std_removed,

                abs(
                    (test_set.rows_added - stats.avg_added)
                    / greatest(stats.std_added, 1)
                ) as z_score_added,
                abs(
                    (test_set.rows_removed - stats.avg_removed)
                    / greatest(stats.std_removed, 1)
                ) as z_score_removed

            from test_set
            cross join final_baseline_stats stats
            where test_set.metric_date = date({{ target_ts }})  -- Only the target day
        )

    select
        *,
        case
            when
                z_score_added > {{ sensitivity }}
                and z_score_removed > {{ sensitivity }}
            then 'Anomaly: High Volume of UPDATES'
            when z_score_added > {{ nr_sensitivity }}
            then 'Anomaly: Spike in CREATED records'
            when z_score_removed > {{ dr_sensitivity }}
            then 'Anomaly: Spike in DELETED records'
            else 'Normal'
        end as anomaly_type
    from final_analysis
    where
        -- Detect anomalies using the specific thresholds
        (z_score_added > {{ sensitivity }} and z_score_removed > {{ sensitivity }})
        or z_score_added > {{ nr_sensitivity }}
        or z_score_removed > {{ dr_sensitivity }}

{% endtest %}
