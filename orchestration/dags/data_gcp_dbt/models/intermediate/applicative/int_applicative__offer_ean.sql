with
    clean_references as (
        select
            offer_id,
            offer_subcategoryid,
            regexp_replace(ean, r'[\\s\\-\\t]', '') as ean,
            regexp_replace(isbn, r'[\\s\\-\\t]', '') as isbn,
            case
                when length(cast(titelive_gtl_id as string)) = 7
                then concat('0', cast(titelive_gtl_id as string))
                else cast(titelive_gtl_id as string)
            end as titelive_gtl_id,
            rayon,
            book_editor
        from {{ ref("int_applicative__extract_offer") }} o
    ),

    validity_isbn_ean as (
        select
            *,
            case
                when length(isbn) = 10 and regexp_contains(isbn, r'^\d{9}[0-9Xx]$')
                then 'valid'
                when length(isbn) = 13 and regexp_contains(isbn, r'^\d{13}$')
                then 'valid'
                when isbn is null
                then null
                else 'invalid'
            end as isbn_is_valid,
            case
                when length(ean) = 10 and regexp_contains(ean, r'^\d{9}[0-9Xx]$')
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
            case
                when isbn_is_valid = 'valid' or isbn_is_valid is null
                then if(length(ean) = 13, coalesce(ean, isbn), isbn)
                else null
            end as isbn,
            case when ean_is_valid = 'valid' then ean else null end as ean
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
    clean_isbn.isbn,
    clean_isbn.titelive_gtl_id,
    matching_isbn_with_rayon.rayon as rayon,
    matching_isbn_with_editor.book_editor as book_editor
from clean_isbn
left join matching_isbn_with_rayon using (isbn)
left join matching_isbn_with_editor using (isbn)
