select
    om.offer_id,
    om.search_group_name,
    om.author,
    -- fmt: off
    o.theater_movie_id as theater_movieId, -- noqa: CP02
    o.theater_room_id as theater_roomId, -- noqa: CP02
    -- fmt: on
    o.movie_type,
    o.visa as movie_visa,
    o.release_date as movie_release_date,
    o.genres as movie_genres,
    o.countries as movie_countries,
    o.book_editor,
    o.isbn as offer_ean,
    om.gtl_label_level_1,
    om.gtl_label_level_2,
    om.gtl_label_level_3,
    om.gtl_label_level_4
from {{ ref("mrt_global__offer_metadata") }} as om
inner join {{ ref("int_global__offer") }} as o on om.offer_id = o.offer_id
