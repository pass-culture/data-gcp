with
    source as (
        select
            ident18 as respondent_id,
            g2401 as attended_french_variety,
            g2402 as attended_world_music,
            g2403 as attended_traditional,
            g2404 as attended_international_variety,
            g2405 as attended_rnb,
            g2406 as attended_electronic_techno,
            g2407 as attended_hiphop_rap,
            g2408 as attended_metal_hardrock,
            g2409 as attended_pop_rock,
            g2410 as attended_jazz,
            g2411 as attended_opera,
            g2412 as attended_classical,
            g2501 as attended_last_12m_french_variety,
            g2502 as attended_last_12m_world_music,
            g2503 as attended_last_12m_traditional,
            g2504 as attended_last_12m_international_variety,
            g2505 as attended_last_12m_rnb,
            g2506 as attended_last_12m_electronic_techno,
            g2507 as attended_last_12m_hiphop_rap,
            g2508 as attended_last_12m_metal_hardrock,
            g2509 as attended_last_12m_pop_rock,
            g2510 as attended_last_12m_jazz,
            g2511 as attended_last_12m_opera,
            g2512 as attended_last_12m_classical
        from {{ ref("int_seed__deps_cultural_practices_2018") }}
    ),

    concert_genres as (
        {% set genres = [
            "french_variety",
            "world_music",
            "traditional",
            "international_variety",
            "rnb",
            "electronic_techno",
            "hiphop_rap",
            "metal_hardrock",
            "pop_rock",
            "jazz",
            "opera",
            "classical",
        ] %}
        {% for genre in genres %}
            select
                respondent_id,
                '{{ genre }}' as music_genre,
                attended_{{ genre }} as attended_lifetime,
                attended_last_12m_{{ genre }} as attended_last_12m
            from source
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )

select respondent_id, music_genre, attended_lifetime, attended_last_12m
from concert_genres
