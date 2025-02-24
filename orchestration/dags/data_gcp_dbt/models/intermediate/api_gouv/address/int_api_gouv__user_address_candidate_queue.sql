with
    last_status_update as (
        select user_id, max(datecreated) as last_modified_at
        from {{ source("raw", "applicative_database_beneficiary_fraud_check") }}
        where type = 'PROFILE_COMPLETION' and status = 'OK'
        group by user_id
    ),

    user_location_update as (
        select user_id, max(updated_at) as last_calculation_at
        from {{ source("raw", "user_address") }}
        group by user_id
    ),

    user_candidates as (
        select
            adu.user_id,
            adu.user_creation_date as user_creation_at,
            greatest(
                adu.user_creation_date, lsu.last_modified_at
            ) as user_last_modified_at,
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
        left join last_status_update as lsu on adu.user_id = lsu.user_id
        where
            coalesce(adu.user_address, '') <> ''
            and adu.user_postal_code is not null
            and adu.user_city is not null
    )

select
    uc.user_id,
    uc.user_full_address,
    uc.user_creation_at,
    ulu.last_calculation_at as user_address_last_calculation_at

from user_candidates as uc
left join user_location_update as ulu on uc.user_id = ulu.user_id
where
    -- no location update or location update is older than last user update
    (ulu.user_id is null or ulu.last_calculation_at < uc.user_last_modified_at)
