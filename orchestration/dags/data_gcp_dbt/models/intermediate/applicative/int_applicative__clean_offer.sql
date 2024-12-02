with
    clean_references as (
        select
            * except (isbn, ean, titelive_gtl_id),
            regexp_replace(ean, r'[\\s\\-\\t]', '') as ean,
            regexp_replace(isbn, r'[\\s\\-\\t]', '') as isbn,
            case
                when length(cast(titelive_gtl_id as string)) = 7
                then concat('0', cast(titelive_gtl_id as string))
                else cast(titelive_gtl_id as string)
            end as titelive_gtl_id
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
    )

select
    * except (ean, isbn),
    case
        when isbn_is_valid = 'valid'
        then if(length(ean) = 13, coalesce(ean, isbn), isbn)
        else null
    end as isbn,
    case when ean_is_valid = 'valid' then ean else null end as ean
from validity_isbn_ean
