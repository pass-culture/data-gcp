with matching_isbn_with_rayon as (
    select
        isbn,
        rayon,
        ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) as rank_rayon
    from {{ ref('int_applicative__extract_offer') }}
    where offer_subcategoryid in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
        and rayon is not NULL
        and isbn is not NULL
    group by isbn,
        rayon
    qualify ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) = 1
),

matching_isbn_with_editor as (
    select
        isbn,
        book_editor,
        ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) as rank_editor
    from {{ ref('int_applicative__extract_offer') }}
    where offer_subcategoryid in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
        and book_editor is not NULL
        and isbn is not NULL
    group by isbn,
        book_editor
    qualify ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) = 1
)

select distinct
    isbn,
    rayon,
    book_editor
from matching_isbn_with_rayon
    left join matching_isbn_with_editor using (isbn)
