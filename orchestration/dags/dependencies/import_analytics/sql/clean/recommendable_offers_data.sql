CREATE TEMP FUNCTION offer_has_at_least_one_bookable_stock(var_offer_id STRING) RETURNS INT64 AS (
    (
        SELECT
            COUNT(1),
        FROM
            `{{ bigquery_clean_dataset }}.applicative_database_stock` stock
        WHERE
            stock.offer_id = var_offer_id
            AND stock.stock_is_soft_deleted = FALSE
            AND (
                stock.stock_beginning_date > CURRENT_DATE()
                OR stock.stock_beginning_date IS NULL
            )
            AND (
                stock.stock_booking_limit_date > CURRENT_DATE()
                OR stock.stock_booking_limit_date IS NULL
            )
            AND (
                stock.stock_quantity IS NULL
                OR (
                    SELECT
                        GREATEST(
                            stock.stock_quantity - COALESCE(SUM(booking.booking_quantity), 0),
                            0
                        )
                    FROM
                        `{{ bigquery_clean_dataset }}.applicative_database_booking` booking
                    WHERE
                        booking.stock_id = stock.stock_id
                        AND booking.booking_is_cancelled = FALSE
                ) > 0
            )
    )
);

CREATE TEMP FUNCTION offer_has_at_least_one_active_mediation(var_offer_id STRING) RETURNS INT64 AS (
    (
        SELECT
            COUNT(1),
        FROM
            `{{ bigquery_clean_dataset }}.applicative_database_mediation` mediation
        WHERE
            mediation.offerId = var_offer_id
            AND mediation.isActive
            AND mediation.thumbCount > 0
    )
);

WITH get_recommendable_offers AS(
    SELECT
        DISTINCT (offer.offer_id) AS offer_id,
        offer.offer_product_id AS product_id,
        offer.venue_id AS venue_id,
        offer.offer_subcategoryId AS subcategory_id,
        subcategories.category_id AS category,
        subcategories.search_group_name AS search_group_name,
        offer.offer_name AS name,
        offer.offer_url AS url,
        offer.offer_is_national AS is_national,
        offer.offer_creation_date AS offer_creation_date,
        stock.stock_beginning_date AS stock_beginning_date,
        enriched_offer.last_stock_price AS stock_price,
        (
            CASE
                WHEN booking_numbers.booking_number IS NOT NULL THEN booking_numbers.booking_number
                ELSE 0
            END
        ) AS booking_number,
        (
            CASE
                WHEN offer.offer_subcategoryId IN ('LIVRE_PAPIER', 'SEANCE_CINE') THEN CONCAT('product-', offer.offer_product_id)
                ELSE CONCAT('offer-', offer.offer_id)
            END
        ) AS item_id,
        (
            CASE
                WHEN (
                    offer.offer_product_id NOT IN ('3469240')
                    AND offer.offer_subcategoryId <> 'JEU_EN_LIGNE'
                    AND offer.offer_subcategoryId <> 'JEU_SUPPORT_PHYSIQUE'
                    AND offer.offer_subcategoryId <> 'ABO_JEU_VIDEO'
                    AND offer.offer_subcategoryId <> 'ABO_LUDOTHEQUE'
                    AND (
                        offer.offer_url IS NULL
                        OR enriched_offer.last_stock_price = 0
                        OR subcategories.id = 'LIVRE_NUMERIQUE'
                        OR subcategories.id = 'ABO_LIVRE_NUMERIQUE'
                        OR subcategories.id = 'TELECHARGEMENT_LIVRE_AUDIO'
                        OR subcategories.category_id = 'MEDIA'
                    )
                ) THEN TRUE
                ELSE FALSE
            END
        ) AS is_underage_recommendable,
    FROM
        `{{ bigquery_clean_dataset }}.applicative_database_offer` offer
        JOIN `{{ bigquery_clean_dataset }}.subcategories` subcategories ON offer.offer_subcategoryId = subcategories.id
        JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` enriched_offer ON offer.offer_id = enriched_offer.offer_id
        JOIN (
            SELECT
                *
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
            WHERE
                venue_validation_token IS NULL
        ) venue ON offer.venue_id = venue.venue_id
        JOIN (
            SELECT
                *
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_offerer` offerer
            WHERE
                offerer_validation_token IS NULL
        ) offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_stock` stock ON offer.offer_id = stock.offer_id
        LEFT JOIN (
            SELECT
                COUNT(*) AS booking_number,
                offer.offer_product_id
            FROM
                `{{ bigquery_clean_dataset }}.applicative_database_booking` booking
                LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_stock` stock ON booking.stock_id = stock.stock_id
                LEFT JOIN `{{ bigquery_clean_dataset }}.applicative_database_offer` offer ON stock.offer_id = offer.offer_id
            WHERE
                booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
                AND NOT booking.booking_is_cancelled
            GROUP BY
                offer.offer_product_id
        ) booking_numbers ON booking_numbers.offer_product_id = offer.offer_product_id
    WHERE
        offer.offer_is_active = TRUE
        AND (
            (
                SELECT
                    offer_has_at_least_one_active_mediation(offer.offer_id)
            ) = 1
        )
        AND (
            (
                SELECT
                    offer_has_at_least_one_bookable_stock(offer.offer_id)
            ) = 1
        )
        AND offerer.offerer_is_active = TRUE
        AND offer.offer_validation = 'APPROVED'
        AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND offer.offer_product_id NOT IN (
            '1839851',
            '3091285',
            '3543047',
            '1039337',
            '448269',
            '3546497',
            '3543420',
            '950939',
            '3263530',
            '3049167',
            '1579118',
            '1846991',
            '484671',
            '742624',
            '33068',
            '156561',
            '2590311',
            '234502',
            '43472',
            '3503779',
            '1709760',
            '2188902',
            '3331749',
            '3567080',
            '324437',
            '484514',
            '3186679',
            '3592786',
            '3406408',
            '3553878',
            '3351360',
            '3002681',
            '2590871',
            '2333216',
            '2779812',
            '2163820',
            '1838146',
            '2223410',
            '3134555',
            '676794',
            '3543143',
            '1708272',
            '3592377',
            '3462941',
            '3517691',
            '3547187',
            '3542017',
            '3134534',
            '3419858',
            '3555194',
            '3462640',
            '2531916',
            '2742988',
            '2643450',
            '3543305',
            '3309673',
            '2232476',
            '1290894',
            '3598277',
            '1561925',
            '3604666',
            '3510898',
            '3535855',
            '3323591',
            '3097014',
            '2346389',
            '2140785',
            '2217934',
            '3536288',
            '493212',
            '3543218',
            '3416386',
            '1358062',
            '3541980',
            '3134425',
            '3557358',
            '2874799',
            '3544146',
            '3469240',
            '2830247',
            '3004974',
            '1695066',
            '2043626',
            '2444464',
            '431024',
            '3556998',
            '1129008',
            '3255260',
            '1257705',
            '3504020',
            '1473404',
            '3561731',
            '3613978',
            '1975660',
            '3561555',
            '94996',
            '3643983',
            '3423510',
            '3541933',
            '3287717',
            '2667510',
            '2082516',
            '2634021',
            '2359446',
            '3358392',
            '219380',
            '2760414',
            '3613876',
            '3541968',
            '3550400',
            '3440581',
            '3462933',
            '2119564',
            '3351333',
            '3617484',
            '3423506',
            '3559581',
            '397307',
            '3351372',
            '86574',
            '536916',
            '2677147',
            '3000889',
            '2765147',
            '3612171',
            '1076789',
            '3645938',
            '3313879',
            '3513331',
            '3481162',
            '3346071',
            '3567320',
            '3542090',
            '3567319',
            '2905127',
            '3049366',
            '3574463',
            '2463501',
            '2321839',
            '1351596',
            '2537964',
            '1511221',
            '3372581',
            '3407645',
            '2772753',
            '3557365',
            '3559635',
            '3100248',
            '327591',
            '2903500',
            '2721566',
            '2727888',
            '1407437',
            '3517792',
            '3540692',
            '2095856',
            '1808116',
            '190842',
            '1721393',
            '3589125',
            '3199578',
            '2775295',
            '1491579',
            '3612166'
        )
)
SELECT
    *
FROM
    get_recommendable_offers