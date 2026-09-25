{{ config(tags=["monthly"]) }}

select
    genres.respondent_id,
    genres.music_genre,
    genres.listens_to_genre,
    genres.likes_genre,
    genres.dislikes_genre,
    respondent.gender,
    respondent.age_group,
    respondent.income,
    respondent.household_type
from {{ ref("int_cultural_practices__music_genres") }} as genres
inner join
    {{ ref("int_cultural_practices__respondent") }} as respondent
    on genres.respondent_id = respondent.respondent_id
