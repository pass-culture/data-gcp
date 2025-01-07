with
    -- Step 1: Initial Data Cleaning and Transformation
    base_cleaning as (
        select
            offer_id,
            offer_subcategoryid,
            rayon as category_rayon,
            book_editor as publisher_name,
            regexp_replace(ean, r'[\\s\\-\\tA-Za-z]', '') as cleaned_ean,
            regexp_replace(isbn, r'[\\s\\-\\tA-Za-z]', '') as cleaned_isbn,
            length(regexp_replace(isbn, r'[\\s\\-\\tA-Za-z]', '')) as isbn_length,
            length(regexp_replace(ean, r'[\\s\\-\\tA-Za-z]', '')) as ean_length,
            case
                when length(cast(titelive_gtl_id as string)) = 7
                then concat('0', cast(titelive_gtl_id as string))
                else cast(titelive_gtl_id as string)
            end as normalized_gtl_id
        from {{ ref("int_applicative__extract_offer") }}
    ),

    -- Step 2: Validate ISBN and EAN
    validate_identifiers as (
        select
            offer_id,
            offer_subcategoryid,
            cleaned_ean,
            cleaned_isbn,
            isbn_length,
            ean_length,
            normalized_gtl_id,
            category_rayon,
            publisher_name,
            case
                when isbn_length = 10 and regexp_contains(cleaned_isbn, r'^\d{10}$')
                then 'valid'
                when isbn_length = 13 and regexp_contains(cleaned_isbn, r'^\d{13}$')
                then 'valid'
                when cleaned_isbn is null
                then 'missing'
                else 'invalid_format'
            end as isbn_status,
            case
                when ean_length = 10 and regexp_contains(cleaned_ean, r'^\d{10}$')
                then 'valid'
                when ean_length = 13 and regexp_contains(cleaned_ean, r'^\d{13}$')
                then 'valid'
                when cleaned_ean is null
                then 'missing'
                else 'invalid_format'
            end as ean_status
        from base_cleaning
    ),

    -- Step 3: Filter Valid ISBN and EAN
    filter_valid_identifiers as (
        select
            offer_id,
            offer_subcategoryid,
            normalized_gtl_id,
            category_rayon,
            publisher_name,
            ean_length,
            isbn_length,
            case when isbn_status = 'valid' then cleaned_isbn end as valid_isbn,
            case when ean_status = 'valid' then cleaned_ean end as valid_ean
        from validate_identifiers
    ),

    -- Step 4: Coalesce EAN and ISBN into Primary ISBN
    clean_offer_ean_cte as (
        select
            offer_id,
            offer_subcategoryid,
            category_rayon,
            publisher_name,
            normalized_gtl_id as titelive_gtl_id,
            valid_ean as ean,
            if(ean_length = 13, valid_ean, valid_isbn) as isbn
        from filter_valid_identifiers
    ),

    -- Step 5: Determine the Most Frequent Rayon per ISBN
    determine_rayon_by_isbn as (
        select isbn, category_rayon
        from clean_offer_ean_cte
        where
            offer_subcategoryid
            in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
            and category_rayon is not null
            and isbn is not null
        group by isbn, category_rayon
        qualify
            row_number() over (
                partition by isbn
                order by count(distinct offer_id) desc, max(offer_id) desc
            )
            = 1
    ),

    -- Step 6: Determine the Most Frequent Publisher per ISBN
    determine_editor_by_isbn as (
        select isbn, publisher_name
        from clean_offer_ean_cte
        where
            offer_subcategoryid
            in ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
            and publisher_name is not null
            and isbn is not null
        group by isbn, publisher_name
        qualify
            row_number() over (
                partition by isbn
                order by count(distinct offer_id) desc, max(offer_id) desc
            )
            = 1
    )

-- Final Selection of Processed Data
select
    clean_offer_ean_cte.offer_id,
    clean_offer_ean_cte.ean,
    clean_offer_ean_cte.isbn,
    clean_offer_ean_cte.titelive_gtl_id,
    determine_rayon_by_isbn.category_rayon as rayon,
    determine_editor_by_isbn.publisher_name as book_editor
from clean_offer_ean_cte
left join
    determine_rayon_by_isbn on clean_offer_ean_cte.isbn = determine_rayon_by_isbn.isbn
left join
    determine_editor_by_isbn on clean_offer_ean_cte.isbn = determine_editor_by_isbn.isbn
