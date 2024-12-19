with
    clean_references as (
        select
            offer_id,
            offer_subcategoryid,
            rayon,
            book_editor,
            regexp_replace(ean, r'[\\s\\-\\tA-Za-z]', '') as ean,
            regexp_replace(isbn, r'[\\s\\-\\tA-Za-z]', '') as isbn,
            case
                when length(cast(titelive_gtl_id as string)) = 7
                then concat('0', cast(titelive_gtl_id as string))
                else cast(titelive_gtl_id as string)
            end as titelive_gtl_id
        from {{ ref("int_applicative__extract_offer") }}
    ),

    validity_isbn_ean as (
        select
            *,
            case
                when length(isbn) = 10 and regexp_contains(isbn, r'^\d{10}$')
                then 'valid'
                when length(isbn) = 13 and regexp_contains(isbn, r'^\d{13}$')
                then 'valid'
                when isbn is null
                then null
                else 'invalid'
            end as isbn_is_valid,
            case
                when length(ean) = 10 and regexp_contains(ean, r'^\d{10}$')
                then 'valid'
                when length(ean) = 13 and regexp_contains(ean, r'^\d{13}$')
                then 'valid'
                when ean is null
                then null
                else 'invalid'
            end as ean_is_valid
        from clean_references
    ),

    clean_isbn as (
        select
            * except (ean, isbn),
            case when isbn_is_valid = 'valid' then isbn end as isbn,
            case when ean_is_valid = 'valid' then ean end as ean
        from validity_isbn_ean
    ),

    matching_isbn_with_rayon as (
        select isbn, rayon
        from clean_isbn
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
        from clean_isbn
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

select
    clean_isbn.offer_id,
    clean_isbn.ean,
    clean_isbn.titelive_gtl_id,  -- TODO(legacy): isbn is overwritted by ean
    matching_isbn_with_rayon.rayon,
    matching_isbn_with_editor.book_editor,
    if(
        length(clean_isbn.ean) = 13,
        coalesce(clean_isbn.ean, clean_isbn.isbn),
        clean_isbn.isbn
    ) as isbn
from clean_isbn
left join matching_isbn_with_rayon on clean_isbn.isbn = matching_isbn_with_rayon.isbn
left join matching_isbn_with_editor using (isbn)
