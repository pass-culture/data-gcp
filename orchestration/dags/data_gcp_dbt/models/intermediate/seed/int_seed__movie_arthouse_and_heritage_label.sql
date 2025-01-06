select
    movie_status as movie_label,
    split(movie_visa, '.')[offset(0)] as movie_visa,
    movie_title,
    movie_distributor,
    cast(movie_release_date as date) as movie_release_date
from {{ source("seed", "gsheet_movie_arthouse_and_heritage_label") }}
qualify row_number() over (partition by movie_visa order by movie_status) = 1
