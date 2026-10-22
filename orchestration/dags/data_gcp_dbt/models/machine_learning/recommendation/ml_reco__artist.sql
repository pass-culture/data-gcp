{{ config(materialized="table") }}

with
    artist_bookable_search_group_names as (
        select distinct pal.artist_id, o.search_group_name
        from {{ ref("int_applicative__product_artist_link") }} as pal
        inner join
            {{ ref("int_global__offer") }} as o
            on pal.offer_product_id = o.offer_product_id
        where o.offer_is_bookable and o.search_group_name is not null
    )

select
    a.artist_id,
    a.main_search_group_name,
    absg.search_group_name is not null as main_search_group_name_has_bookable_offer
from {{ ref("mrt_global__artist") }} as a
left join
    artist_bookable_search_group_names as absg
    on a.artist_id = absg.artist_id
    and a.main_search_group_name = absg.search_group_name
