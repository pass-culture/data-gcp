{{ create_humanize_id_function() }}
WITH offer_humanized_id AS (
    SELECT
        offer_id,
        humanize_id(offer_id) AS humanized_id,
    FROM
        `{{ gcp_project }}.analytics_{{ env_short_name }}.`.applicative_database_offer
    WHERE
        offer_id is not NULL
),
mediation AS (
    SELECT
        offer_id,
        humanize_id(id) as mediation_humanized_id
    FROM
        (
            SELECT
                id,
                offerId as offer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY offerId
                    ORDER BY
                        dateModifiedAtLastProvider DESC
                ) as rnk
            FROM
                `{{ gcp_project }}.analytics_{{ env_short_name }}`.applicative_database_mediation
            WHERE
                isActive
        ) inn
    WHERE
        rnk = 1
),
base as (
    SELECT
        o.offer_id,
        oii.item_id,
CASE
            when (
                o.offer_name is null
                or o.offer_name = 'NaN'
            ) then "None"
            else safe_cast(o.offer_name as STRING)
        END as offer_name,
CASE
            when (
                o.offer_description is null
                or o.offer_description = 'NaN'
            ) then "None"
            else safe_cast(o.offer_description as STRING)
        END as offer_description,
        o.offer_subcategoryid,
CASE
            WHEN mediation.mediation_humanized_id is not null THEN CONCAT(
                "https://storage.googleapis.com/{{ mediation_url }}-assets-fine-grained/thumbs/mediations/",
                mediation.mediation_humanized_id
            )
            ELSE CONCAT(
                "https://storage.googleapis.com/{{ mediation_url }}-assets-fine-grained/thumbs/products/",
                humanize_id(o.offer_product_id)
            )
        END AS image_url,
    FROM
        `{{ gcp_project }}.raw_{{ env_short_name }}`.applicative_database_offer o
        LEFT JOIN mediation ON o.offer_id = mediation.offer_id
        LEFT JOIN `{{ gcp_project }}.analytics_{{ env_short_name }}`.offer_item_ids oii
        on o.offer_id=oii.offer_id
    where o.offer_id not in (select offer_id from `{{ gcp_project }}.clean_{{ env_short_name }}`.offer_embeddings)
    LIMIT 50000
)
select
    *
from
    base