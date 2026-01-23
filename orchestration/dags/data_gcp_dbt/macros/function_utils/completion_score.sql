{% macro completion_score(
    film_titre_format_support,
    absence_image,
    absence_video,
    absence_description,
    livre_cd_vinyle_artiste_manquant,
    livre_cd_vinyle_gtl_manquant,
    livre_cd_vinyle_createur_manquant
) %}

    round(
        1 - (
            (
                cast({{ film_titre_format_support }} as float64) * 1.0
                + cast({{ absence_image }} as float64) * 2.0
                + cast({{ absence_video }} as float64) * 1.0
                + cast({{ absence_description }} as float64) * 1.5
                + cast({{ livre_cd_vinyle_artiste_manquant }} as float64) * 1.0
                + cast({{ livre_cd_vinyle_gtl_manquant }} as float64) * 1.0
                + cast({{ livre_cd_vinyle_createur_manquant }} as float64) * 1.0
            )
            / 8.5
        ),
        1
    )

{% endmacro %}
