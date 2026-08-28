with
    product_artist_link_delta as (
        select offer_product_id, artist_id, artist_type, action
        from {{ ref("ml_linkage__delta_product_artist_link") }}
    ),

    future_product_artist_link as (
        select base.offer_product_id, base.artist_id, base.artist_type
        from {{ ref("int_applicative__product_artist_link") }} as base
        where
            not exists (
                select 1 as found
                from product_artist_link_delta as delta
                where
                    delta.action in ("remove", "update")
                    and delta.offer_product_id = base.offer_product_id
                    and delta.artist_id = base.artist_id
                    and delta.artist_type = base.artist_type
            )
        union all
        select offer_product_id, artist_id, artist_type
        from product_artist_link_delta
        where action in ("add", "update")
    ),

    -- wikidata_id of every artist that can appear in the links (base + delta)
    artist_wikidata as (
        select artist_id, wikidata_id
        from {{ ref("int_applicative__artist") }}
        where wikidata_id is not null
        union distinct
        select artist_id, wikidata_id
        from {{ ref("ml_linkage__delta_artist") }}
        where wikidata_id is not null and action in ("add", "update")
    ),

    -- the single artist_id kept by future_artist for each wikidata_id
    surviving_artist as (
        select wikidata_id, artist_id as surviving_artist_id
        from {{ ref("ml_linkage__future_artist") }}
        where wikidata_id is not null
    ),

    -- map an artist_id deduplicated away in future_artist onto the survivor, so
    -- links keep pointing to an artist_id that still exists (relationships test)
    artist_remap as (
        select
            artist_wikidata.artist_id as duplicate_artist_id,
            surviving_artist.surviving_artist_id
        from artist_wikidata
        inner join surviving_artist using (wikidata_id)
        where artist_wikidata.artist_id != surviving_artist.surviving_artist_id
        -- an artist_id maps to a single survivor even if it ever carried >1 wikidata
        qualify
            row_number() over (
                partition by artist_wikidata.artist_id
                order by surviving_artist.surviving_artist_id
            )
            = 1
    )

-- distinct: a deduplication `add` (or the remap above) can target a link that
-- already exists in the applicative table, which would otherwise appear twice
select distinct
    link.offer_product_id,
    link.artist_type,
    coalesce(remap.surviving_artist_id, link.artist_id) as artist_id
from future_product_artist_link as link
left join artist_remap as remap on link.artist_id = remap.duplicate_artist_id
