with matching_isbn_with_rayon as (
    select
        isbn,
        rayon,
        ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) as rank_rayon
    from {{ ref('offer_extracted_data') }}
    where
        offer_subcategoryid in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
        and rayon is not NULL
        and isbn is not NULL
    group by 1, 2
    qualify ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) = 1
),

matching_isbn_with_editor as (
    select
        isbn,
        book_editor,
        ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) as rank_editor
    from {{ ref('offer_extracted_data') }}
    where
        offer_subcategoryid in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
        and book_editor is not NULL
        and isbn is not NULL
    group by 1, 2
    qualify ROW_NUMBER() over (partition by isbn order by COUNT(distinct offer_id) desc) = 1
)

select distinct
    isbn,
    rayon,
    book_editor
from matching_isbn_with_rayon
    left join matching_isbn_with_editor using (isbn)
