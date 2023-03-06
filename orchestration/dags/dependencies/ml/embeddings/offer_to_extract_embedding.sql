CREATE TEMPORARY FUNCTION humanize_id(id STRING) RETURNS STRING LANGUAGE js OPTIONS (
    library = "gs://data-bucket-prod/base32-encode/base32.js"
) AS """
     		 // turn int into bytes array
			var byteArray = [];
			var updated_id = id;
			while (updated_id != 0) {
			    var byte = updated_id & 0xff;
			    byteArray.push(byte);
			    updated_id = (updated_id - byte) / 256;
			}
			var reversedByteArray = byteArray.reverse();
			
			// apply base32 encoding
			var raw_b32 = base32Encode(new Uint8Array(reversedByteArray), 'RFC4648', { padding: false });
			
			// replace " O " with " 8 " and " I " with " 9 "
			return raw_b32.replace(/O/g, '8').replace(/I/g, '9');
 """;

WITH offer_humanized_id AS (
    SELECT
        offer_id,
        humanize_id(offer_id) AS humanized_id,
    FROM
        `{gcp_project}.analytics_{env_short_name}.`.applicative_database_offer
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
                `{gcp_project}.analytics_{env_short_name}`.applicative_database_mediation
            WHERE
                isActive
        ) inn
    WHERE
        rnk = 1
),
base as (
    SELECT
        o.offer_validation,
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
                "https://storage.googleapis.com/passculture-metier-prod-production-assets-fine-grained/thumbs/mediations/",
                mediation.mediation_humanized_id
            )
            ELSE CONCAT(
                "https://storage.googleapis.com/passculture-metier-prod-production-assets-fine-grained/thumbs/products/",
                humanize_id(o.offer_product_id)
            )
        END AS image_url,
    FROM
        `{gcp_project}.raw_{env_short_name}`.applicative_database_offer o
        LEFT JOIN `{gcp_project}.raw_{env_short_name}`.applicative_database_stock s on s.offer_id = o.offer_id
        LEFT JOIN `{gcp_project}.analytics_{env_short_name}`.offer_extracted_data oed ON oed.offer_id = o.offer_id
        LEFT JOIN `{gcp_project}.analytics_{env_short_name}`.subcategories subcat ON subcat.id = o.offer_subcategoryid
        LEFT join `{gcp_project}.analytics_{env_short_name}`applicative_database_venue v on v.venue_id = o.venue_id
        LEFT JOIN `{gcp_project}.analytics_{env_short_name}`.macro_rayons AS rayon_ref ON oed.rayon = rayon_ref.rayon
        LEFT JOIN mediation ON o.offer_id = mediation.offer_id
    where
        o.offer_validation <> 'DRAFT'
        and o.offer_last_validation_type = 'MANUAL'
        and (
            (
                o.offer_name is not null
                or o.offer_name <> 'NaN'
            )
        )
)
select
    *
from
    base