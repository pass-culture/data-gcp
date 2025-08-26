select
    jsonpayload.extra.path as url_path,
    timestamp as log_timestamp,
    resource.labels.namespace_name as environement,
    jsonpayload.message,
    jsonpayload.technical_message_id,
    jsonpayload.extra.choice_datetime,
    jsonpayload.extra.source,
    jsonpayload.extra.method,
    jsonpayload.extra.analyticssource as analytics_source,
    labels.k8s_pod_role,
    jsonpayload.extra.consent.mandatory as cookies_consent_mandatory,
    jsonpayload.extra.consent.accepted as cookies_consent_accepted,
    jsonpayload.extra.consent.refused as cookies_consent_refused,
    jsonpayload.extra.searchtype as search_type,
    jsonpayload.extra.searchsubtype as search_sub_type,
    jsonpayload.extra.searchprotype as search_pro_type,
    cast(jsonpayload.user_id as int64) as user_id,
    jsonpayload.extra.userid as extra_user_id,
    jsonpayload.extra.uai,
    jsonpayload.extra.user_role,
    jsonpayload.extra.from as origin,
    cast(
        jsonpayload.extra.collective_offer_template_id as string
    ) as collective_offer_template_id,
    cast(jsonpayload.extra.queryid as string) as query_id,
    jsonpayload.extra.comment,
    jsonpayload.extra.requested_date,
    cast(jsonpayload.extra.total_students as int) as total_students,
    cast(jsonpayload.extra.total_teachers as int) as total_teachers,
    jsonpayload.extra.header_link_name,
    cast(
        coalesce(jsonpayload.extra.bookingid, jsonpayload.extra.booking_id) as string
    ) as booking_id,
    cast(jsonpayload.extra.resultscount as int) as results_count,
    cast(jsonpayload.extra.resultnumber as int) as results_number,
    jsonpayload.extra.filtervalues.eventaddresstype as address_type_filter,
    cast(jsonpayload.extra.filtervalues.query as string) as text_filter,
    cast(jsonpayload.extra.statuscode as int64) as status_code,
    jsonpayload.extra.deviceid as device_id,
    jsonpayload.extra.sourceip as source_ip,
    jsonpayload.extra.appversion as app_version,
    jsonpayload.extra.duration,
    jsonpayload.extra.platform,
    jsonpayload.extra.suggestiontype as suggestion_type,
    jsonpayload.extra.suggestionvalue as suggestion_value,
    cast(jsonpayload.extra.isfavorite as boolean) as is_favorite,
    cast(jsonpayload.extra.playlistid as string) as playlist_id,
    cast(jsonpayload.extra.domainid as string) as domain_id,
    cast(jsonpayload.extra.index as int) as rank_clicked,
    cast(
        coalesce(
            jsonpayload.extra.changes.quantity.old_value, jsonpayload.extra.old_quantity
        ) as int64
    ) as stock_old_quantity,
    cast(
        coalesce(
            jsonpayload.extra.changes.quantity.new_value,
            jsonpayload.extra.stock_quantity
        ) as int64
    ) as stock_new_quantity,
    coalesce(
        jsonpayload.extra.changes.price.old_value, jsonpayload.extra.old_price
    ) as stock_old_price,
    coalesce(
        jsonpayload.extra.changes.price.new_value, jsonpayload.extra.stock_price
    ) as stock_new_price,
    cast(jsonpayload.extra.stock_dnbookedquantity as int64) as stock_booking_quantity,
    jsonpayload.extra.changes.publicationdatetime.oldvalue
    as publication_date_old_value,
    jsonpayload.extra.changes.publicationdatetime.newvalue
    as publication_date_new_value,
    jsonpayload.extra.changes.bookinglimitdatetime.old_value
    as booking_limit_date_old_value,
    jsonpayload.extra.changes.bookinglimitdatetime.new_value
    as booking_limit_date_new_value,
    jsonpayload.extra.changes.beginningdatetime.old_value
    as stock_beginning_date_old_value,
    jsonpayload.extra.changes.beginningdatetime.new_value
    as stock_beginning_date_new_value,
    jsonpayload.extra.changes.withdrawaldetails.oldvalue
    as offer_withdrawal_details_old_value,
    jsonpayload.extra.changes.withdrawaldetails.newvalue
    as offer_withdrawal_details_new_value,
    jsonpayload.extra.changes.offereraddress.oldvalue as offerer_address_old_value,
    jsonpayload.extra.changes.offereraddress.newvalue as offerer_address_new_value,
    jsonpayload.extra.changes.description.oldvalue as offer_description_old_value,
    jsonpayload.extra.changes.description.newvalue as offer_description_new_value,
    coalesce(
        jsonpayload.extra.changes.pricecategory.old_value, jsonpayload.extra.old_price
    ) as stock_old_price_category,
    coalesce(
        jsonpayload.extra.changes.pricecategory.new_value, jsonpayload.extra.stock_price
    ) as stock_new_price_category,
    jsonpayload.extra.eans as list_of_eans_not_found,
    cast(jsonpayload.extra.offerer_id as int64) as offerer_id,
    jsonpayload.extra.isconvenient as beta_test_new_nav_is_convenient,
    jsonpayload.extra.ispleasant as beta_test_new_nav_is_pleasant,
    jsonpayload.extra.comment as beta_test_new_nav_comment,
    jsonpayload.extra.searchquery as search_query,
    cast(jsonpayload.extra.searchnbresults as int) as search_nb_results,
    cast(jsonpayload.extra.searchrank as int) as card_clicked_rank,
    trace,
    cast(jsonpayload.extra.newlysubscribedto.email as string) as newly_subscribed_email,
    cast(jsonpayload.extra.newlysubscribedto.push as string) as newly_subscribed_push,
    cast(
        jsonpayload.extra.subscriptions.marketing_push as string
    ) as currently_subscribed_marketing_push,
    cast(
        jsonpayload.extra.subscriptions.marketing_email as string
    ) as currently_subscribed_marketing_email,
    cast(
        jsonpayload.extra.newlyunsubscribedfrom.email as string
    ) as newly_unsubscribed_email,
    cast(
        jsonpayload.extra.newlyunsubscribedfrom.push as string
    ) as newly_unsubscribed_push,
    cast(jsonpayload.extra.age as int) as user_age,
    cast(jsonpayload.extra.bookings_count as int) as total_bookings,
    jsonpayload.extra.feedback as user_feedback_message,
    jsonpayload.extra.status as user_status,
    cast(jsonpayload.extra.user_satisfaction as string) as user_satisfaction,
    cast(jsonpayload.extra.user_comment as string) as user_comment,
    cast(jsonpayload.extra.offer_data_api_call_id as string) as suggested_offer_api_id,
    cast(
        jsonpayload.extra.offer_subcategory as string
    ) as suggested_offer_api_subcategory,
    date(timestamp) as partition_date,
    coalesce(
        cast(jsonpayload.extra.stockid as string),
        cast(jsonpayload.extra.stock_id as string)
    ) as stock_id,
    coalesce(
        cast(jsonpayload.extra.offerid as string),
        cast(jsonpayload.extra.offer as string),
        cast(jsonpayload.extra.offer_id as string)
    ) as offer_id,
    array_to_string(
        jsonpayload.extra.filtervalues.departments, ','
    ) as department_filter,
    array_to_string(jsonpayload.extra.filtervalues.academies, ',') as academy_filter,
    jsonpayload.extra.filtervalues.geolocradius as geoloc_radius_filter,
    array_to_string(
        array(
            select cast(value as string)
            from unnest(jsonpayload.extra.filtervalues.venue) as value
        ),
        ','
    ) as venue_filter,
    array_to_string(
        array(
            select cast(value as string)
            from unnest(jsonpayload.extra.filtervalues.domains) as value
        ),
        ','
    ) as artistic_domain_filter,
    array_to_string(
        array(
            select cast(value as string)
            from unnest(jsonpayload.extra.filtervalues.students) as value
        ),
        ','
    ) as student_filter,
    array_to_string(jsonpayload.extra.filtervalues.formats, ',') as format_filter,
    coalesce(
        cast(jsonpayload.extra.venueid as string),
        cast(jsonpayload.extra.venue as string),
        cast(jsonpayload.extra.venue_id as string)
    ) as venue_id,
    coalesce(
        cast(jsonpayload.extra.product as string),
        cast(jsonpayload.extra.product_id as string)
    ) as product_id,
    array_to_string(
        jsonpayload.extra.newlysubscribedto.themes, ' - '
    ) as newly_subscribed_themes,
    array_to_string(
        jsonpayload.extra.subscriptions.subscribed_themes, ' - '
    ) as currently_subscribed_themes,
    array_to_string(
        jsonpayload.extra.newlyunsubscribedfrom.themes, ' - '
    ) as newly_unsubscribed_themes,
    date(
        jsonpayload.extra.firstdepositactivationdate
    ) as user_first_deposit_activation_date,
    array_to_string(
        jsonpayload.extra.offer_subcategories, ','
    ) as suggested_offer_api_subcategories,
    jsonpayload.extra.provider_id,
    jsonpayload.extra.author_id,
    cast(jsonpayload.extra.user_connect_as as boolean) as is_user_connect_as
from {{ source("raw", "stdout") }}
