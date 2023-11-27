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
        {{ ref('firebase_events_analytics') }}
WHERE
    offer_id IS NOT NULL
GROUP BY
    offer_id