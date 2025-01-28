select
    movie_status as movie_label,
    movie_title,
    movie_distributor,
    cast(movie_release_date as date) as movie_release_date,
    safe_cast(safe_cast(movie_visa as float64) as string) as movie_visa
from {{ source("raw", "gsheet_movie_arthouse_and_heritage_label") }}
where (movie_visa is not null and movie_visa != 'nan')
qualify row_number() over (partition by movie_visa order by movie_status) = 1
