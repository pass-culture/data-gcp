{{ config(materialized="ephemeral") }}

{% set json_path_activity = "$.modified_info.activity.new_info" %}
{% set json_path_address = "$.modified_info.address.new_info" %}
{% set json_path_city = "$.modified_info.city.new_info" %}
{% set json_path_postal_code = "$.modified_info.postalCode.new_info" %}

with
    -- CTE: Extract raw data from action_history table
    -- Contains user profile modifications where activity, address, city, or postal
    -- code changed
    raw_action_history as (
        select
            user_id,
            action_date as creation_timestamp,
            action_type,
            json_value(
                action_history_json_data, '{{ json_path_activity }}'
            ) as activity,
            json_value(action_history_json_data, '{{ json_path_address }}') as address,
            json_value(action_history_json_data, '{{ json_path_city }}') as city,
            nullif(
                json_value(action_history_json_data, '{{ json_path_postal_code }}'),
                'None'
            ) as postal_code,
            -- Normalize address using macro for consistent formatting
            {{
                normalize_address(
                    "json_value(action_history_json_data, '"
                    ~ json_path_address
                    ~ "')",
                    "nullif(json_value(action_history_json_data, '"
                    ~ json_path_postal_code
                    ~ "'), 'None')",
                    "json_value(action_history_json_data, '" ~ json_path_city ~ "')",
                )
            }} as normalized_address
        from {{ source("raw", "applicative_database_action_history") }}
        where
            true
            and action_type = 'INFO_MODIFIED'
            and (
                json_value(action_history_json_data, '{{ json_path_activity }}')
                is not null
                or json_value(action_history_json_data, '{{ json_path_address }}')
                is not null
                or json_value(action_history_json_data, '{{ json_path_city }}')
                is not null
                or json_value(action_history_json_data, '{{ json_path_postal_code }}')
                is not null
            )
            and user_id is not null
            {% if is_incremental() %}
                and date(action_date) between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
            {% endif %}
    ),

    -- CTE: Extract raw data from fraud_check table
    -- Contains initial profile completion records (first time user fills profile)
    raw_fraud_check as (
        select
            user_id,
            datecreated as creation_timestamp,
            type as action_type,
            json_value(result_content, '$.activity') as activity,
            json_value(result_content, '$.address') as address,
            json_value(result_content, '$.city') as city,
            -- Handle both naming conventions: postal_code and postalCode
            coalesce(
                json_value(result_content, '$.postal_code'),
                json_value(result_content, '$.postalCode')
            ) as postal_code,
            -- Normalize address using macro for consistent formatting
            {{
                normalize_address(
                    "json_value(result_content, '$.address')",
                    "coalesce(json_value(result_content, '$.postal_code'), json_value(result_content, '$.postalCode'))",
                    "json_value(result_content, '$.city')",
                )
            }}
            as normalized_address
        from {{ source("raw", "applicative_database_beneficiary_fraud_check") }}
        where
            true
            and type = 'PROFILE_COMPLETION'
            and reason != 'Anonymized'
            and user_id is not null
            {% if is_incremental() %}
                and date(datecreated) between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
            {% endif %}
        -- Deduplicate rows with same date and normalized address
        -- (duplicates may be created by normalize_address function)
        qualify
            row_number() over (
                partition by
                    user_id,
                    date(creation_timestamp),
                    lower(activity),
                    {{
                        normalize_address(
                            "json_value(result_content, '$.address')",
                            "coalesce(json_value(result_content, '$.postal_code'), json_value(result_content, '$.postalCode'))",
                            "json_value(result_content, '$.city')",
                        )
                    }},
                    lower(city),
                    postal_code
                order by creation_timestamp desc
            )
            = 1
    ),

    -- CTE: Union both data sources into a single raw dataset
    raw_data as (
        select *
        from raw_action_history
        union all
        select *
        from raw_fraud_check
    ),

    -- CTE: Forward-fill null values within each user's history
    -- This ensures continuity of values when only some fields are modified
    filled_data as (
        select
            user_id,
            creation_timestamp,
            action_type,
            activity,
            normalized_address as address,
            city,
            postal_code,
            -- Forward-fill: carry last non-null value forward
            last_value(activity ignore nulls) over w as current_activity,
            last_value(normalized_address ignore nulls) over w as current_address,
            last_value(city ignore nulls) over w as current_city,
            last_value(postal_code ignore nulls) over w as current_postal_code
        from raw_data
        window
            w as (
                partition by user_id
                order by creation_timestamp
                rows unbounded preceding
            )
    ),

    -- CTE: Deduplicate records keeping only the most recent per user/action_type/date
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
        qualify
            row_number() over (
                partition by user_id, action_type, date(creation_timestamp)
                order by creation_timestamp desc
            )
            = 1
    ),

    -- CTE: Compute previous values and sequential ranking for each user's history
    processed_data as (
        select
            user_id,
            creation_timestamp,
            action_type,
            activity as user_activity,
            address as user_address,
            city as user_city,
            postal_code as user_postal_code,
            -- Sequential rank starting at 0 for each user
            row_number() over (partition by user_id order by creation_timestamp)
            - 1 as info_history_rank,

            -- Previous values for change detection (on forward-filled values)
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
    ),

    -- CTE: Compute change flags using NULL-safe comparisons
    -- Uses IS NOT DISTINCT FROM for proper NULL handling
    user_beneficiary_information_history as (
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

            -- Flag: user confirmed their existing information (all fields unchanged)
            (
                user_activity is not distinct from user_previous_activity
                and user_address is not distinct from user_previous_address
                and user_city is not distinct from user_previous_city
                and user_postal_code is not distinct from user_previous_postal_code
            )
            and info_history_rank > 0 as has_confirmed,

            -- Flag: user modified at least one field
            (
                user_activity is distinct from user_previous_activity
                or user_address is distinct from user_previous_address
                or user_city is distinct from user_previous_city
                or user_postal_code is distinct from user_previous_postal_code
            )
            and info_history_rank > 0 as has_modified,

            -- Field-specific modification flags
            (user_activity is distinct from user_previous_activity)
            and info_history_rank > 0 as has_modified_activity,
            (user_address is distinct from user_previous_address)
            and info_history_rank > 0 as has_modified_address,
            (user_city is distinct from user_previous_city)
            and info_history_rank > 0 as has_modified_city,
            (user_postal_code is distinct from user_previous_postal_code)
            and info_history_rank > 0 as has_modified_postal_code

        from processed_data
        order by user_id asc, creation_timestamp desc
    ),

    -- CTE: Add coordinates from postal code centroid as fallback
    ubih_coordinates_via_postal_code as (
        select
            ubih.*,
            pc.postal_approx_centroid_latitude as user_latitude,
            pc.postal_approx_centroid_longitude as user_longitude
        from user_beneficiary_information_history as ubih
        left join
            {{ ref("int_seed__geo_postal_code") }} as pc
            on ubih.user_postal_code = pc.postal_code
    ),

    -- CTE: Get user's geocoded address for precise coordinates
    user_adresse_clean as (
        select user_id, user_full_address, latitude, longitude
        from {{ source("raw", "user_address") }}
        where result_status = 'ok'
        qualify row_number() over (partition by user_id order by updated_at desc) = 1
    ),

    -- CTE: Add precise coordinates from geocoded address when available
    ubih_coordinates_via_geocoded_address as (
        select
            ubih.*,
            if(
                ua.latitude != '' and ua.latitude is not null,
                safe_cast(ua.latitude as float64),
                null
            ) as user_latitude,
            if(
                ua.longitude != '' and ua.longitude is not null,
                safe_cast(ua.longitude as float64),
                null
            ) as user_longitude
        from user_beneficiary_information_history as ubih
        left join
            user_adresse_clean as ua
            on ubih.user_id = ua.user_id
            and trim(
                concat(
                    replace(replace(ubih.user_address, '\\r', ''), '\\n', ''),
                    ' ',
                    ubih.user_postal_code,
                    ' ',
                    ubih.user_city
                )
            )
            = ua.user_full_address
    ),

    -- CTE: Get user birth date for age calculation
    user_birth_dates as (
        select user_id, user_birth_date
        from {{ ref("int_global__user_beneficiary") }}
        where user_birth_date is not null
    )

-- Final output: prefer geocoded coordinates over postal code centroid
select
    ubih_pc.user_id,
    ubih_pc.info_history_rank as user_information_rank,
    ubih_pc.action_type as user_information_action_type,
    ubih_pc.creation_timestamp as user_information_created_at,
    ubih_pc.user_activity,
    ubih_pc.user_address,
    ubih_pc.user_city,
    ubih_pc.user_postal_code,
    ubih_pc.user_previous_activity,
    ubih_pc.user_previous_address,
    ubih_pc.user_previous_city,
    ubih_pc.user_previous_postal_code,
    ubih_pc.has_confirmed as user_has_confirmed_information,
    ubih_pc.has_modified as user_has_modified_information,
    ubih_pc.has_modified_activity as user_has_modified_activity,
    ubih_pc.has_modified_address as user_has_modified_address,
    ubih_pc.has_modified_city as user_has_modified_city,
    ubih_pc.has_modified_postal_code as user_has_modified_postal_code,
    coalesce(ubih_ga.user_longitude, ubih_pc.user_longitude) as user_longitude,
    coalesce(ubih_ga.user_latitude, ubih_pc.user_latitude) as user_latitude,
    -- User age at information creation (using exact age calculation)
    {{ calculate_exact_age("date(ubih_pc.creation_timestamp)", "ubd.user_birth_date") }}
    as user_age_at_information_creation
from ubih_coordinates_via_postal_code as ubih_pc
left join
    ubih_coordinates_via_geocoded_address as ubih_ga
    on ubih_pc.user_id = ubih_ga.user_id
    and ubih_pc.info_history_rank = ubih_ga.info_history_rank
left join user_birth_dates as ubd on ubih_pc.user_id = ubd.user_id
