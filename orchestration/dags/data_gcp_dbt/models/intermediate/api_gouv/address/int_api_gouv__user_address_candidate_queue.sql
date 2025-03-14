with
    profile_completion_check as (
        select
            user_id,
            timestamp(datecreated) as user_last_modified_at,
            json_extract(result_content, '$.city') as user_city,
            json_extract(result_content, '$.address') as user_address,
            json_extract(result_content, '$.postal_code') as user_postal_code
        from {{ source("raw", "applicative_database_beneficiary_fraud_check") }}
        where type = 'PROFILE_COMPLETION' and status = 'OK'
    ),

    profile_completion as (
        select
            pcc.user_last_modified_at,
            pcc.user_id,
            trim(
                concat(
                    replace(replace(pcc.user_address, '\\r', ''), '\\n', ''),
                    ' ',
                    pcc.user_postal_code,
                    ' ',
                    pcc.user_city
                )
            ) as user_full_address
        from profile_completion_check as pcc
        where
            -- beneficiary update should have all fields
            coalesce(pcc.user_address, '') <> '' and pcc.user_postal_code is not null
    ),

    applicative_database_user as (
        select
            adu.user_id,
            timestamp(adu.user_creation_date) as user_last_modified_at,
            trim(
                concat(
                    replace(replace(adu.user_address, '\\r', ''), '\\n', ''),
                    ' ',
                    adu.user_postal_code,
                    ' ',
                    adu.user_city
                )
            ) as user_full_address
        from {{ source("raw", "applicative_database_user") }} as adu
        where
            -- beneficiary update should have all fields
            coalesce(adu.user_address, '') <> '' and adu.user_postal_code is not null
    ),

    -- at least a postal code is present in the data
    all_users as (
        select user_id, user_last_modified_at, user_full_address
        from applicative_database_user
        union all
        select user_id, user_last_modified_at, user_full_address
        from profile_completion
    ),

    user_candidates as (
        select user_id, user_last_modified_at, user_full_address
        from all_users
        qualify
            row_number() over (partition by user_id order by user_last_modified_at desc)
            = 1
    ),

    user_location_update as (
        select user_id, timestamp(max(updated_at)) as last_calculation_at
        from {{ source("raw", "user_address") }}
        group by user_id
    )

select
    uc.user_id,
    uc.user_full_address,
    uc.user_last_modified_at as user_address_last_calculation_at,
    u.user_creation_date as user_creation_at

from user_candidates as uc
left join user_location_update as ulu on uc.user_id = ulu.user_id
inner join
    {{ source("raw", "applicative_database_user") }} as u on uc.user_id = u.user_id
where
    -- no location update or location update is older than last user update
    (ulu.user_id is null or ulu.last_calculation_at < uc.user_last_modified_at)
