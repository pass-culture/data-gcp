{% test snapshot_volume_anomalies(
    model,
    unique_key,
    remove_outliers=false,
    check_date=None,
    days_back=30,
    sensitivity=3,
    nr_sensitivity=None,
    dr_sensitivity=None,
    ur_sensitivity=None
) %}

    {#
    Arguments:
    - unique_key: The business key column (e.g., 'id', 'user_id'). REQUIRED.
    - remove_outliers: If true, excludes historical anomalies from the baseline calculation (2-pass).
                       IMPORTANT: Only removes outliers from HISTORICAL data (< target_date).
                       The target day is NEVER filtered out.
    - check_date: Override date.
    - days_back: Window size (default 30).
    - sensitivity: Z-score threshold (default 3) for Updated Row anomalies.
                   Also used for outlier removal from baseline if remove_outliers=true.
    - nr_sensitivity: New Row (INSERT) threshold. Defaults to 'sensitivity' if not set.
    - dr_sensitivity: Deleted Row (DELETE) threshold. Defaults to 'sensitivity' if not set.
    - ur_sensitivity: Updated Row (UPDATE) threshold. Defaults to 'sensitivity' if not set.
    #}
    {# 1. Check age at compile-time: disambiguate new record from updates by checking for records existing before the window #}
    {% set age_check_query %}
        select cast(count(*) > 0 as int64)
        from {{ model }} 
        where dbt_valid_from <= timestamp_sub(
            {% if check_date is not none %} 
                timestamp('{{ check_date }}') 
            {% else %} 
                timestamp(date_sub(date('{{ ds() }}'), interval 1 day)) 
            {% endif %}, 
            interval {{ days_back }} day
        )
    {% endset %}

    {# Ensure we only run the query during the execution phase #}
    {% if execute %}
        {% set results = run_query(age_check_query) %}
        {% set has_enough_history = results.columns[0].values()[0] | int == 1 %}
    {% else %} {% set has_enough_history = true %}
    {% endif %}

    {% if not has_enough_history %}

        {% if execute %}
            {% do log(
                "Snapshot Volume Test: Skipping "
                ~ model
                ~ " - Insufficient history to disambiguate updates (no records with dbt_valid_from <= "
                ~ days_back
                ~ " days ago).",
                info=True,
            ) %}
        {% endif %}

        {# EXIT EARLY: BigQuery returns 0 rows, dbt marks as PASS #}
        select *
        from (select 1 as dummy)
        where 1 = 0
    {% else %}

        {# 2. Determine Target Date: Priority: check_date > ds (Airflow) > current_date() #}
        {% if check_date is not none %}
            {% set target_ts = "timestamp('" ~ check_date ~ "')" %}
        {% else %}
            {# We use your ds() macro and cast to date before subtracting #}
            {% set target_ts = (
                "timestamp(date_sub(date('" ~ ds() ~ "'), interval 1 day))"
            ) %}
        {% endif %}

        {# 3. Calculate Boundaries in SQL using the target_ts defined above #}
        {% set lower_bound = (
            "timestamp_sub(" ~ target_ts ~ ", interval " ~ days_back ~ " day)"
        ) %}
        {% set upper_bound = "timestamp_add(" ~ target_ts ~ ", interval 1 day)" %}

        {# 4. Anomaly specific sensitivities #}
        {% set nr_sensitivity = (
            nr_sensitivity if nr_sensitivity is not none else sensitivity
        ) %}
        {% set dr_sensitivity = (
            dr_sensitivity if dr_sensitivity is not none else sensitivity
        ) %}
        {% set ur_sensitivity = (
            ur_sensitivity if ur_sensitivity is not none else sensitivity
        ) %}

        with
            -- Identify IDs that existed BEFORE the window to disambiguate Updates vs
            -- Inserts
            known_before_window as (
                select distinct {{ unique_key }} as id
                from {{ model }}
                where dbt_valid_to < {{ lower_bound }}
            ),

            raw_window_data as (
                select dbt_valid_from, dbt_valid_to, {{ unique_key }} as id
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

            -- Flatten to daily ID-level events
            daily_id_events as (
                -- IDs that were created on this day
                select
                    date(dbt_valid_from) as metric_date,
                    id,
                    true as was_created,
                    false as was_closed
                from raw_window_data
                where
                    dbt_valid_from >= {{ lower_bound }}
                    and dbt_valid_from < {{ upper_bound }}

                union all

                -- IDs that were closed on this day
                select
                    date(dbt_valid_to) as metric_date,
                    id,
                    false as was_created,
                    true as was_closed
                from raw_window_data
                where
                    dbt_valid_to >= {{ lower_bound }}
                    and dbt_valid_to < {{ upper_bound }}
            ),

            -- Aggregate per ID per day to detect what happened
            daily_id_summary as (
                select
                    e.metric_date,
                    e.id,
                    max(e.was_created) as was_created,
                    max(e.was_closed) as was_closed,
                    -- Check if the ID existed prior to the scanned window
                    max(
                        case when k.id is not null then 1 else 0 end
                    ) as was_previously_known
                from daily_id_events e
                left join known_before_window k on e.id = k.id
                group by metric_date, id
            ),

            -- Count the types of operations
            test_set as (
                select
                    metric_date,
                    -- Row-level counts (for backwards compatibility)
                    sum(case when was_created then 1 else 0 end) as rows_added,
                    sum(case when was_closed then 1 else 0 end) as rows_removed,

                    -- Record-level counts (actual business operations)
                    count(
                        distinct case
                            when
                                was_created and (was_closed or was_previously_known = 1)
                            then id
                        end
                    ) as records_updated,
                    count(
                        distinct case
                            when
                                was_created
                                and not was_closed
                                and was_previously_known = 0
                            then id
                        end
                    ) as records_inserted,
                    count(
                        distinct case when was_closed and not was_created then id end
                    ) as records_deleted
                from daily_id_summary
                group by metric_date
            ),

            -- PASS 1: Initial stats from ALL historical data (strictly LESS THAN
            -- target date)
            initial_stats as (
                select
                    avg(rows_added) as avg_added,
                    coalesce(stddev(rows_added), 0) as std_added,
                    avg(rows_removed) as avg_removed,
                    coalesce(stddev(rows_removed), 0) as std_removed,
                    avg(records_updated) as avg_updated,
                    coalesce(stddev(records_updated), 0) as std_updated,
                    avg(records_inserted) as avg_inserted,
                    coalesce(stddev(records_inserted), 0) as std_inserted,
                    avg(records_deleted) as avg_deleted,
                    coalesce(stddev(records_deleted), 0) as std_deleted
                from test_set
                where metric_date < date({{ target_ts }})
            ),

            -- OPTIONAL PASS: Clean the history by removing outliers from BASELINE ONLY
            training_set as (
                select
                    test_set.metric_date,
                    test_set.rows_added,
                    test_set.rows_removed,
                    test_set.records_updated,
                    test_set.records_inserted,
                    test_set.records_deleted
                from test_set
                cross join initial_stats stats
                where
                    test_set.metric_date < date({{ target_ts }})  -- Only historical baseline data
                    {% if remove_outliers %}
                        -- Remove historical outliers using the general sensitivity
                        -- threshold
                        and abs(test_set.rows_added - stats.avg_added)
                        <= ({{ sensitivity }} * greatest(stats.std_added, 1))
                        and abs(test_set.rows_removed - stats.avg_removed)
                        <= ({{ sensitivity }} * greatest(stats.std_removed, 1))
                    {% endif %}
            ),

            -- PASS 2: Final Baseline Stats (from cleaned history if
            -- remove_outliers=true)
            final_baseline_stats as (
                select
                    avg(rows_added) as avg_added,
                    coalesce(stddev(rows_added), 0) as std_added,
                    avg(rows_removed) as avg_removed,
                    coalesce(stddev(rows_removed), 0) as std_removed,
                    avg(records_updated) as avg_updated,
                    coalesce(stddev(records_updated), 0) as std_updated,
                    avg(records_inserted) as avg_inserted,
                    coalesce(stddev(records_inserted), 0) as std_inserted,
                    avg(records_deleted) as avg_deleted,
                    coalesce(stddev(records_deleted), 0) as std_deleted
                from training_set
            ),

            -- Compare TARGET DAY against the cleaned baseline
            final_analysis as (
                select
                    test_set.*,
                    -- Include ALL baseline metrics for final selection
                    stats.avg_added,
                    stats.std_added,
                    stats.avg_removed,
                    stats.std_removed,
                    stats.avg_updated,
                    stats.std_updated,
                    stats.avg_inserted,
                    stats.std_inserted,
                    stats.avg_deleted,
                    stats.std_deleted,
                    -- Signed z-scores (positive = spike, negative = drop)
                    (test_set.rows_added - stats.avg_added)
                    / greatest(stats.std_added, 1) as z_score_added,
                    (test_set.rows_removed - stats.avg_removed)
                    / greatest(stats.std_removed, 1) as z_score_removed,
                    (test_set.records_updated - stats.avg_updated)
                    / greatest(stats.std_updated, 1) as z_score_updates,
                    (test_set.records_inserted - stats.avg_inserted)
                    / greatest(stats.std_inserted, 1) as z_score_inserts,
                    (test_set.records_deleted - stats.avg_deleted)
                    / greatest(stats.std_deleted, 1) as z_score_deletes
                from test_set
                cross join final_baseline_stats stats
                where test_set.metric_date = date({{ target_ts }})  -- Only the target day
            )

        select
            *,
            case
                -- UPDATE anomalies
                when z_score_updates > {{ ur_sensitivity }}
                then 'Anomaly: Bulk UPDATES spike (same IDs, new versions)'

                when z_score_updates < -{{ ur_sensitivity }}
                then 'Anomaly: UPDATES drop (possible pipeline issue)'

                -- Concurrent INSERT + DELETE
                when
                    z_score_inserts > {{ nr_sensitivity }}
                    and z_score_deletes > {{ dr_sensitivity }}
                then 'Anomaly: Concurrent bulk INSERTS + DELETES (different IDs)'

                when
                    z_score_inserts < -{{ nr_sensitivity }}
                    and z_score_deletes < -{{ dr_sensitivity }}
                then 'Anomaly: Drop in INSERTS and DELETES (possible pipeline issue)'

                -- INSERT anomalies
                when z_score_inserts > {{ nr_sensitivity }}
                then 'Anomaly: Bulk INSERTS spike (new IDs)'

                when z_score_inserts < -{{ nr_sensitivity }}
                then 'Anomaly: INSERTS drop (possible pipeline issue)'

                -- DELETE anomalies
                when z_score_deletes > {{ dr_sensitivity }}
                then 'Anomaly: Bulk DELETES spike (IDs removed)'

                when z_score_deletes < -{{ dr_sensitivity }}
                then 'Anomaly: DELETES drop (possible pipeline issue)'

                -- High churn (legacy)
                when
                    z_score_added > {{ sensitivity }}
                    and z_score_removed > {{ sensitivity }}
                then 'Anomaly: High churn spike (mixed operations)'

                when
                    z_score_added < -{{ sensitivity }}
                    and z_score_removed < -{{ sensitivity }}
                then 'Anomaly: Low churn (possible pipeline issue)'

                else 'Normal'
            end as anomaly_type
        from final_analysis
        where
            -- Detect anomalies using the specific thresholds (both spikes AND drops)
            abs(z_score_updates) > {{ ur_sensitivity }}
            or abs(z_score_inserts) > {{ nr_sensitivity }}
            or abs(z_score_deletes) > {{ dr_sensitivity }}
            or (
                abs(z_score_added) > {{ sensitivity }}
                and abs(z_score_removed) > {{ sensitivity }}
            )

    {% endif %}

{% endtest %}
