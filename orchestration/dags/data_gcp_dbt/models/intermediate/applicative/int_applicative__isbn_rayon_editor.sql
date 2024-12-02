with
    matching_isbn_with_rayon as (
        select isbn, rayon
        from {{ ref("int_applicative__clean_offer") }}
        where
            offer_subcategoryid
            in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
            and rayon is not null
            and isbn is not null
        group by isbn, rayon
        qualify
            row_number() over (partition by isbn order by count(distinct offer_id) desc)
            = 1
    ),

    matching_isbn_with_editor as (
        select isbn, book_editor
        from {{ ref("int_applicative__clean_offer") }}
        where
            offer_subcategoryid
            in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
            and book_editor is not null
            and isbn is not null
        group by isbn, book_editor
        qualify
            row_number() over (partition by isbn order by count(distinct offer_id) desc)
            = 1
    )

select distinct isbn, rayon, book_editor
from matching_isbn_with_rayon
left join matching_isbn_with_editor using (isbn)
