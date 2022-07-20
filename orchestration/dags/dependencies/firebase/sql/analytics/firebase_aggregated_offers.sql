WITH events AS (
    SELECT
        event_name,
        CAST(event_params.value.double_value AS STRING) AS double_offer_id,
        event_params.value.string_value AS string_offer_id
    FROM
        `{{ bigquery_clean_dataset }}.firebase_events_*` AS events,
        events.event_params AS event_params
    WHERE
        event_params.key = 'offerId'
),
cleaned_events AS (
    SELECT
        *
    EXCEPT
(double_offer_id, string_offer_id),
        (
            CASE
                WHEN double_offer_id IS NULL THEN string_offer_id
                ELSE double_offer_id
            END
        ) AS offer_id
    FROM
        events
)
SELECT
    offer_id,
    SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
    SUM(CAST(event_name = 'ConsultWholeOffer' AS INT64)) AS consult_whole_offer,
    SUM(
        CAST(event_name = 'ExclusivityBlockClicked' AS INT64)
    ) AS exclusivity_block_clicked,
    SUM(
        CAST(
            event_name = 'ConsultDescriptionDetails' AS INT64
        )
    ) AS consult_description_details,
    SUM(CAST(event_name = 'ClickBookOffer' AS INT64)) AS click_book_offer,
    SUM(
        CAST(event_name = 'ConsultAvailableDates' AS INT64)
    ) AS consult_available_dates,
    SUM(CAST(event_name = 'Share' AS INT64)) AS share,
    SUM(
        CAST(
            event_name = 'ConsultAccessibilityModalities' AS INT64
        )
    ) AS consult_accessibility_modalities,
    SUM(
        CAST(
            event_name = 'ConsultWithdrawalModalities' AS INT64
        )
    ) AS consult_withdrawal_modalities,
    SUM(
        CAST(event_name = 'ConsultLocationItinerary' AS INT64)
    ) AS consult_location_itinerary,
    SUM(
        CAST(event_name = 'HasAddedOfferToFavorites' AS INT64)
    ) AS has_added_offer_to_favorites
from
    cleaned_events
WHERE
    offer_id IS NOT NULL
GROUP BY
    offer_id