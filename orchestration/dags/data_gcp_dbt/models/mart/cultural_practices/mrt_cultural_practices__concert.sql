{{ config(tags=["monthly"]) }}

select
    concerts.respondent_id,
    concerts.music_genre,
    concerts.attended_lifetime,
    concerts.attended_last_12m,
    respondent.gender,
    respondent.age_group,
    respondent.income,
    respondent.household_type
from {{ ref("int_cultural_practices__concert_genres") }} as concerts
inner join
    {{ ref("int_cultural_practices__respondent") }} as respondent
    on concerts.respondent_id = respondent.respondent_id
