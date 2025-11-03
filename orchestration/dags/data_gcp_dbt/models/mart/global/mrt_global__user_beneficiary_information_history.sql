{{ config(**custom_table_config(cluster_by="user_id")) }}

with
    raw_data as (
        -- Action History
        select
            user_id,
            action_date as creation_timestamp,
            action_type,
            json_value(
                action_history_json_data, '$.modified_info.activity.new_info'
            ) as activity,
            json_value(
                action_history_json_data, '$.modified_info.address.new_info'
            ) as address,
            json_value(
                action_history_json_data, '$.modified_info.city.new_info'
            ) as city,
            json_value(
                action_history_json_data, '$.modified_info.postalCode.new_info'
            ) as postal_code
        from {{ source("raw", "applicative_database_action_history") }}
        where action_type = 'INFO_MODIFIED'

        union all

        -- Fraud Check
        select
            user_id,
            datecreated as creation_timestamp,
            type as action_type,
            json_value(result_content, '$.activity') as activity,
            json_value(result_content, '$.address') as address,
            json_value(result_content, '$.city') as city,
            json_value(result_content, '$.postal_code') as postal_code
        from {{ source("raw", "applicative_database_beneficiary_fraud_check") }}
        where type = 'PROFILE_COMPLETION'
    ),

filled_data as (
        select
            user_id,
            creation_timestamp,
            action_type,
            activity,
            address,
            city,
            postal_code,
            -- Forward-fill des valeurs
            last_value(activity ignore nulls) over w as current_activity,
            last_value(address ignore nulls) over w as current_address,
            last_value(city ignore nulls) over w as current_city,
            last_value(postal_code ignore nulls) over w as current_postal_code
        from raw_data
        window w as (partition by user_id order by creation_timestamp rows unbounded preceding)
    ),

filtered_data as (
        select
            user_id,
            creation_timestamp,
            action_type,
            current_activity as activity,
            current_address as address,
            current_city as city,
            current_postal_code as postal_code
        from filled_data
        -- Keep only the most recent row per user_id, action_type, and date
        qualify row_number() over (
            partition by user_id, action_type, date(creation_timestamp)
            order by creation_timestamp desc
        ) = 1
    ),

processed_data as (
        select
            user_id,
            creation_timestamp,
            action_type,
            activity as user_activity,
            address as user_address,
            city as user_city,
            postal_code as user_postal_code,
            row_number() over (partition by user_id order by creation_timestamp)
            - 1 as info_history_rank,

            -- Valeurs précédentes (sur les valeurs déjà forward-fillées)
            lag(activity) over (
                partition by user_id order by creation_timestamp
            ) as user_previous_activity,
            lag(address) over (
                partition by user_id order by creation_timestamp
            ) as user_previous_address,
            lag(city) over (
                partition by user_id order by creation_timestamp
            ) as user_previous_city,
            lag(postal_code) over (
                partition by user_id order by creation_timestamp
            ) as user_previous_postal_code

        from filtered_data
    )

select
    user_id,
    info_history_rank,
    action_type,
    creation_timestamp,
    user_activity,
    user_address,
    user_city,
    user_postal_code,
    user_previous_activity,
    user_previous_address,
    user_previous_city,
    user_previous_postal_code,

    -- Flags de comparaison optimisés
    coalesce(
        user_activity = user_previous_activity
        and user_address = user_previous_address
        and user_city = user_previous_city
        and user_postal_code = user_previous_postal_code,
        false
    ) as has_confirmed,

    coalesce(
        user_activity != user_previous_activity
        or user_address != user_previous_address
        or user_city != user_previous_city
        or user_postal_code != user_previous_postal_code,
        false
    ) as has_modified,

    -- Flags spécifiques par champ
    coalesce(
        user_activity != user_previous_activity, false
    ) as has_modified_activity,
    coalesce(user_address != user_previous_address, false) as has_modified_address,
    coalesce(user_city != user_previous_city, false) as has_modified_city,
    coalesce(
        user_postal_code != user_previous_postal_code, false
    ) as has_modified_postal_code

from processed_data
order by user_id asc, creation_timestamp desc
