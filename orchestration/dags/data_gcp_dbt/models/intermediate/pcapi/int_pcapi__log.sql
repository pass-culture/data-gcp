select
    date(timestamp) as partition_date,
    jsonpayload.extra.path as url_path,
    timestamp as log_timestamp,
    resource.labels.namespace_name as environement,
    jsonpayload.message,
    jsonpayload.technical_message_id,
    jsonpayload.extra.source,
    jsonpayload.extra.analyticssource as analytics_source,
    cast(jsonpayload.user_id as int64) as user_id,
    jsonpayload.extra.userid as extra_user_id,
    jsonpayload.extra.uai,
    jsonpayload.extra.user_role,
    jsonpayload.extra.from as origin,
    coalesce(
        cast(jsonpayload.extra.stockid as string),
        cast(jsonpayload.extra.stock_id as string)
    ) as stock_id,
    coalesce(
        cast(jsonpayload.extra.offerid as string),
        cast(jsonpayload.extra.offer as string),
        cast(jsonpayload.extra.offer_id as string)
    ) as offer_id,
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
    jsonpayload.extra.platform,
    array_to_string(
        jsonpayload.extra.filtervalues.departments, ','
    ) as department_filter,
    array_to_string(jsonpayload.extra.filtervalues.academies, ',') as academy_filter,
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
    jsonpayload.extra.suggestiontype as suggestion_type,
    jsonpayload.extra.suggestionvalue as suggestion_value,
    cast(jsonpayload.extra.isfavorite as boolean) as is_favorite,
    cast(jsonpayload.extra.playlistid as string) as playlist_id,
    cast(jsonpayload.extra.domainid as string) as domain_id,
    coalesce(
        cast(jsonpayload.extra.venueid as string),
        cast(jsonpayload.extra.venue as string),
        cast(jsonpayload.extra.venue_id as string)
    ) as venue_id,
    cast(jsonpayload.extra.index as int) as rank_clicked,
    coalesce(
        cast(jsonpayload.extra.product as string),
        cast(jsonpayload.extra.product_id as string)
    ) as product_id,
    cast(jsonpayload.extra.old_quantity as int64) as stock_old_quantity,
    cast(jsonpayload.extra.stock_quantity as int64) as stock_new_quantity,
    jsonpayload.extra.old_price as stock_old_price,
    jsonpayload.extra.stock_price as stock_new_price,
    cast(jsonpayload.extra.stock_dnbookedquantity as int64) as stock_booking_quantity,
    jsonpayload.extra.eans as list_of_eans_not_found,
    cast(jsonpayload.extra.offerer_id as int64) as offerer_id,
    jsonpayload.extra.isconvenient as beta_test_new_nav_is_convenient,
    jsonpayload.extra.ispleasant as beta_test_new_nav_is_pleasant,
    jsonpayload.extra.comment as beta_test_new_nav_comment,
    trace

from {{ source("raw", "stdout") }}
