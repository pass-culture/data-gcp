{{ config(materialized="ephemeral") }}


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
            ) as postal_code,
            -- Normalize address early in the flow
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(
                                            json_value(
                                                action_history_json_data,
                                                '$.modified_info.address.new_info'
                                            )
                                        ),
                                        r'\s*' || lower(
                                            json_value(
                                                action_history_json_data,
                                                '$.modified_info.postalCode.new_info'
                                            )
                                        )
                                        || r'\s*',
                                        ' '
                                    ),
                                    r'\s*' || lower(
                                        json_value(
                                            action_history_json_data,
                                            '$.modified_info.city.new_info'
                                        )
                                    )
                                    || r'\s*',
                                    ' '
                                ),
                                r',\s*france\s*$',
                                ''
                            ),
                            r'\d{5}',
                            ' '
                        ),
                        r',\s*',
                        ' '
                    ),
                    r'\s+',
                    ' '
                )
            ) as normalized_address
        from {{ source("raw", "applicative_database_action_history") }}
        where
            true
            and action_type = 'INFO_MODIFIED'
            and (
                json_value(
                    action_history_json_data, '$.modified_info.activity.new_info'
                )
                is not null
                or json_value(
                    action_history_json_data, '$.modified_info.address.new_info'
                )
                is not null
                or json_value(action_history_json_data, '$.modified_info.city.new_info')
                is not null
                or json_value(
                    action_history_json_data, '$.modified_info.postalCode.new_info'
                )
                is not null
            )
            and user_id is not null
            {% if is_incremental() %}
                and date(action_date) = date_sub(date("{{ ds() }}"), interval 1 day)
            {% endif %}

        union all

        -- Fraud Check
        select
            user_id,
            datecreated as creation_timestamp,
            type as action_type,
            json_value(result_content, '$.activity') as activity,
            json_value(result_content, '$.address') as address,
            json_value(result_content, '$.city') as city,
            json_value(result_content, '$.postal_code') as postal_code,
            -- Normalize address early in the flow
            trim(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        lower(json_value(result_content, '$.address')),
                                        r'\s*' || lower(
                                            json_value(result_content, '$.postal_code')
                                        )
                                        || r'\s*',
                                        ' '
                                    ),
                                    r'\s*'
                                    || lower(json_value(result_content, '$.city'))
                                    || r'\s*',
                                    ' '
                                ),
                                r',\s*france\s*$',
                                ''
                            ),
                            r'\d{5}',
                            ' '
                        ),
                        r',\s*',
                        ' '
                    ),
                    r'\s+',
                    ' '
                )
            ) as normalized_address
        from {{ source("raw", "applicative_database_beneficiary_fraud_check") }}
        where
            true
            and type = 'PROFILE_COMPLETION'
            and reason != 'Anonymized'
            and user_id is not null
            {% if is_incremental() %}
                and date(datecreated) = date_sub(date("{{ ds() }}"), interval 1 day)
            {% endif %}
        qualify
            row_number() over (
                partition by
                    user_id,
                    lower(activity),
                    trim(
                        regexp_replace(
                            regexp_replace(
                                regexp_replace(
                                    regexp_replace(
                                        regexp_replace(
                                            regexp_replace(
                                                lower(
                                                    json_value(
                                                        result_content, '$.address'
                                                    )
                                                ),
                                                r'\s*' || lower(
                                                    json_value(
                                                        result_content, '$.postal_code'
                                                    )
                                                )
                                                || r'\s*',
                                                ' '
                                            ),
                                            r'\s*' || lower(
                                                json_value(result_content, '$.city')
                                            )
                                            || r'\s*',
                                            ' '
                                        ),
                                        r',\s*france\s*$',
                                        ''
                                    ),
                                    r'\d{5}',
                                    ' '
                                ),
                                r',\s*',
                                ' '
                            ),
                            r'\s+',
                            ' '
                        )
                    ),
                    lower(city),
                    postal_code
                order by creation_timestamp desc
            )
            = 1
    ),

    filled_data as (
        select
            user_id,
            creation_timestamp,
            action_type,
            activity,
            normalized_address as address,
            city,
            postal_code,
            -- Forward-fill des valeurs
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
    ),

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

            -- Flags de comparaison optimisés (using normalized addresses)
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
            coalesce(
                user_address != user_previous_address, false
            ) as has_modified_address,
            coalesce(user_city != user_previous_city, false) as has_modified_city,
            coalesce(
                user_postal_code != user_previous_postal_code, false
            ) as has_modified_postal_code

        from processed_data
        order by user_id asc, creation_timestamp desc
    ),

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

    user_adresse_clean as (
        select user_id, user_full_address, latitude, longitude
        from {{ source("raw", "user_address") }}
        where result_status = 'ok'
        qualify row_number() over (partition by user_id order by updated_at desc) = 1
    ),

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
    )

select
    ubih_pc.user_id,
    ubih_pc.info_history_rank,
    ubih_pc.action_type,
    ubih_pc.creation_timestamp,
    ubih_pc.user_activity,
    ubih_pc.user_address,
    ubih_pc.user_city,
    ubih_pc.user_postal_code,
    ubih_pc.user_previous_activity,
    ubih_pc.user_previous_address,
    ubih_pc.user_previous_city,
    ubih_pc.user_previous_postal_code,
    ubih_pc.has_confirmed,
    ubih_pc.has_modified,
    ubih_pc.has_modified_activity,
    ubih_pc.has_modified_address,
    ubih_pc.has_modified_city,
    ubih_pc.has_modified_postal_code,
    coalesce(ubih_ga.user_longitude, ubih_pc.user_longitude) as user_longitude,
    coalesce(ubih_ga.user_latitude, ubih_pc.user_latitude) as user_latitude
from ubih_coordinates_via_postal_code as ubih_pc
left join
    ubih_coordinates_via_geocoded_address as ubih_ga
    on ubih_pc.user_id = ubih_ga.user_id
    and ubih_pc.info_history_rank = ubih_ga.info_history_rank
