with
    source as (
        select
            ident18 as respondent_id,
            e1001 as listened_french_variety,
            e1002 as listened_world_music,
            e1003 as listened_traditional,
            e1004 as listened_international_variety,
            e1005 as listened_rnb,
            e1006 as listened_electronic_techno,
            e1007 as listened_hiphop_rap,
            e1008 as listened_metal_hardrock,
            e1009 as listened_pop_rock,
            e1010 as listened_jazz,
            e1011 as listened_opera,
            e1012 as listened_classical,
            e1013 as listened_other,
            e1201 as liked_french_variety,
            e1202 as liked_world_music,
            e1203 as liked_traditional,
            e1204 as liked_international_variety,
            e1205 as liked_rnb,
            e1206 as liked_electronic_techno,
            e1207 as liked_hiphop_rap,
            e1208 as liked_metal_hardrock,
            e1209 as liked_pop_rock,
            e1210 as liked_jazz,
            e1211 as liked_opera,
            e1212 as liked_classical,
            e1213 as liked_other,
            e1301 as disliked_french_variety,
            e1302 as disliked_world_music,
            e1303 as disliked_traditional,
            e1304 as disliked_international_variety,
            e1305 as disliked_rnb,
            e1306 as disliked_electronic_techno,
            e1307 as disliked_hiphop_rap,
            e1308 as disliked_metal_hardrock,
            e1309 as disliked_pop_rock,
            e1310 as disliked_jazz,
            e1311 as disliked_opera,
            e1312 as disliked_classical,
            e1313 as disliked_other
        from {{ ref("int_seed__deps_cultural_practices_2018") }}
    ),

    genre_preferences as (
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
            "other",
        ] %}
        {% for genre in genres %}
            select
                respondent_id,
                '{{ genre }}' as music_genre,
                listened_{{ genre }} as listens_to_genre,
                liked_{{ genre }} as likes_genre,
                disliked_{{ genre }} as dislikes_genre
            from source
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    )

select respondent_id, music_genre, listens_to_genre, likes_genre, dislikes_genre
from genre_preferences
