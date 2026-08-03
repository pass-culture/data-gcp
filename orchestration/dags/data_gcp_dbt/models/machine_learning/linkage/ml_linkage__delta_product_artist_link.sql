with
    casted_delta_product_artist_link as (
        select distinct
            cast(offer_product_id as string) as offer_product_id,
            cast(artist_id as string) as artist_id,
            cast(artist_type as string) as artist_type,
            cast(action as string) as action,  -- noqa: disable=RF04
            cast(comment as string) as comment
        from {{ source("ml_preproc", "delta_product_artist_link") }}
    ),

    added_delta_product_artist_link as (
        select offer_product_id, artist_id, artist_type, action, comment
        from casted_delta_product_artist_link
        where action = 'add'
    ),

    removed_or_updated_delta_product_artist_link as (
        -- distinct: the applicative link table is joined on its business key, not on
        -- its primary key, so it could otherwise duplicate a delta row
        select distinct
            casted_delta_product_artist_link.offer_product_id,
            casted_delta_product_artist_link.artist_id,
            casted_delta_product_artist_link.artist_type,
            casted_delta_product_artist_link.action,
            casted_delta_product_artist_link.comment
        from casted_delta_product_artist_link
        inner join
            {{ source("raw", "applicative_database_product_artist_link") }}
            as applicative_database_product_artist_link
            on casted_delta_product_artist_link.offer_product_id
            = cast(applicative_database_product_artist_link.offer_product_id as string)
            and casted_delta_product_artist_link.artist_id
            = applicative_database_product_artist_link.artist_id
            and casted_delta_product_artist_link.artist_type
            = applicative_database_product_artist_link.artist_type
        where casted_delta_product_artist_link.action in ('update', 'remove')
    )

select *
from added_delta_product_artist_link
union all
select *
from removed_or_updated_delta_product_artist_link
