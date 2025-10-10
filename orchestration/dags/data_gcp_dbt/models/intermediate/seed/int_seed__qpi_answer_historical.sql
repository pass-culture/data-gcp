-- mapping of category_id to question_id
{% set question_mapping = {
    "BIBLIOTHEQUE": "SORTIES",
    "CINEMA": "SORTIES",
    "CONCERT": "SORTIES",
    "CONFERENCE": "SORTIES",
    "COURS": "SORTIES",
    "EVENEMENT_JEU": "SORTIES",
    "FESTIVAL": "SORTIES",
    "MUSEE": "SORTIES",
    "SANS_SORTIE": "SORTIES",
    "SPECTACLE": "SORTIES",
    "FILM_DOMICILE": "ACTIVITES",
    "JEU_VIDEO": "ACTIVITES",
    "JOUE_INSTRUMENT": "ACTIVITES",
    "LIVRE": "ACTIVITES",
    "MATERIEL_ART_CREATIF": "ACTIVITES",
    "PODCAST": "ACTIVITES",
    "PRESSE_EN_LIGNE": "ACTIVITES",
    "SANS_ACTIVITE": "ACTIVITES",
    "FESTIVAL_AUTRE": "FESTIVALS",
    "FESTIVAL_AVANT_PREMIERE": "FESTIVALS",
    "FESTIVAL_CINEMA": "FESTIVALS",
    "FESTIVAL_LIVRE": "FESTIVALS",
    "FESTIVAL_MUSIQUE": "FESTIVALS",
    "FESTIVAL_SPECTACLE": "FESTIVALS",
    "SPECTACLE_AUTRE": "SPECTACLES",
    "SPECTACLE_CIRQUE": "SPECTACLES",
    "SPECTACLE_DANSE": "SPECTACLES",
    "SPECTACLE_HUMOUR": "SPECTACLES",
    "SPECTACLE_OPERA": "SPECTACLES",
    "SPECTACLE_RUE": "SPECTACLES",
    "SPECTACLE_THEATRE": "SPECTACLES",
    "PROJECTION_ACTIVITE_ARTISTIQUE": "PROJECTIONS",
    "PROJECTION_AUTRE": "PROJECTIONS",
    "PROJECTION_CD_VINYLE": "PROJECTIONS",
    "PROJECTION_CINEMA": "PROJECTIONS",
    "PROJECTION_CONCERT": "PROJECTIONS",
    "PROJECTION_CONFERENCE": "PROJECTIONS",
    "PROJECTION_FESTIVAL": "PROJECTIONS",
    "PROJECTION_JEU": "PROJECTIONS",
    "PROJECTION_LIVRE": "PROJECTIONS",
    "PROJECTION_SPECTACLE": "PROJECTIONS",
    "PROJECTION_VISITE": "PROJECTIONS",
} %}

{% set answer_mapping = {"--- IGNORE ---": "--- IGNORE ---"} %}


with
    raw_data as (
        select user_id, submitted_at, category_id, subcategory_id
        from {{ source("raw", "qpi_answers_historical") }}
    )
select
    user_id,
    submitted_at,
    category_id,
    subcategory_id,
    case
        category_id
        {% for old_val, new_val in question_mapping.items() %}
            when '{{ old_val }}' then '{{ new_val }}'
        {% endfor %}
        else null
    end as mapped_question_id,
    case
        subcategory_id
        {% for old_val, new_val in answer_mapping.items() %}
            when '{{ old_val }}' then '{{ new_val }}'
        {% endfor %}
        else null
    end as mapped_answer_id
from raw_data
