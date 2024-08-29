select
    offer_id,
    SUM(CAST(event_name = 'ConsultOffer' as INT64)) as consult_offer,
    SUM(CAST(event_name = 'ConsultWholeOffer' as INT64)) as consult_whole_offer,
    SUM(
        CAST(event_name = 'ExclusivityBlockClicked' as INT64)
    ) as exclusivity_block_clicked,
    SUM(
        CAST(
            event_name = 'ConsultDescriptionDetails' as INT64
        )
    ) as consult_description_details,
    SUM(CAST(event_name = 'ClickBookOffer' as INT64)) as click_book_offer,
    SUM(
        CAST(event_name = 'ConsultAvailableDates' as INT64)
    ) as consult_available_dates,
    SUM(CAST(event_name = 'Share' as INT64)) as share,
    SUM(
        CAST(
            event_name = 'ConsultAccessibilityModalities' as INT64
        )
    ) as consult_accessibility_modalities,
    SUM(
        CAST(
            event_name = 'ConsultWithdrawalModalities' as INT64
        )
    ) as consult_withdrawal_modalities,
    SUM(
        CAST(event_name = 'ConsultLocationItinerary' as INT64)
    ) as consult_location_itinerary,
    SUM(
        CAST(event_name = 'HasAddedOfferToFavorites' as INT64)
    ) as has_added_offer_to_favorites
from {{ ref('int_firebase__native_event') }}
where
    offer_id is not NULL
group by
    offer_id
