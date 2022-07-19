
/* Creating indexes to increase the creation of the materialized view speed. */
CREATE INDEX idx_stock_id                ON public.stock     USING btree (stock_id);
CREATE INDEX idx_stock_offerid           ON public.stock     USING btree ("offer_id");

CREATE INDEX idx_booking_stockid         ON public.booking   USING btree ("stock_id");

CREATE INDEX idx_mediation_offerid       ON public.mediation USING btree ("offerId");

CREATE INDEX idx_offer_id                ON public.offer     USING btree (offer_id);
CREATE INDEX idx_offer_subcategoryid     ON public.offer     USING btree ("offer_subcategoryId");
CREATE INDEX idx_offer_venueid           ON public.offer     USING btree ("venue_id");

CREATE INDEX idx_venue_id                ON public.venue     USING btree (venue_id);
CREATE INDEX idx_venue_managingoffererid ON public.venue     USING btree ("venue_managing_offerer_id");

CREATE INDEX idx_offerer_id              ON public.offerer   USING btree (offerer_id);



/* Function to check if a given offer has bookable stocks (>0 and in the future). */
DROP FUNCTION IF EXISTS offer_has_at_least_one_bookable_stock;
CREATE OR REPLACE FUNCTION offer_has_at_least_one_bookable_stock(var_offer_id varchar)
RETURNS SETOF INTEGER AS
$body$
BEGIN
    RETURN QUERY
    SELECT 1
      FROM public.stock
      WHERE stock."offer_id" = var_offer_id
        AND stock."stock_is_soft_deleted" = FALSE
        AND (
              stock."stock_beginning_date" > NOW()
            OR stock."stock_beginning_date" IS NULL
            )
        AND (
              stock."stock_booking_limit_date" > NOW()
            OR stock."stock_booking_limit_date" IS NULL
            )
        AND (
              stock.stock_quantity IS NULL
            OR (
                SELECT GREATEST(stock.stock_quantity - COALESCE(SUM(booking.booking_quantity), 0), 0)
                FROM public.booking
                WHERE booking."stock_id" = stock.stock_id
                  AND booking."booking_is_cancelled" = FALSE
              ) > 0
        );
END
$body$
LANGUAGE plpgsql;



/* Function to check if a given offer has one active mediation. */
DROP FUNCTION IF EXISTS offer_has_at_least_one_active_mediation;
CREATE OR REPLACE FUNCTION offer_has_at_least_one_active_mediation(var_offer_id varchar)
RETURNS SETOF INTEGER AS
$body$
BEGIN
    RETURN QUERY
    SELECT 1
      FROM public.mediation
    WHERE mediation."offerId" = var_offer_id
    AND mediation."isActive"
    AND mediation."thumbCount" > 0;
END
$body$
LANGUAGE plpgsql;



/* Function to get all recommendable offers ids. */
DROP FUNCTION IF EXISTS get_recommendable_offers CASCADE;
CREATE OR REPLACE FUNCTION get_recommendable_offers()
RETURNS TABLE (offer_id varchar,
                product_id varchar,
                venue_id varchar,
                subcategory_id VARCHAR,
                category VARCHAR,
                search_group_name VARCHAR,
                name VARCHAR,
                url VARCHAR,
                is_national BOOLEAN,
                offer_creation_date TIMESTAMP,
                stock_beginning_date TIMESTAMP,
                stock_price REAL,
                booking_number BIGINT,
                item_id text) AS
$body$
BEGIN
    RETURN QUERY 
    SELECT DISTINCT ON (offer.offer_id)
            offer.offer_id               AS offer_id,
            offer.offer_product_id       AS product_id,
            offer."venue_id"             AS venue_id,
            offer."offer_subcategoryId"  AS subcategory_id,
            subcategories.category_id    AS category,
            subcategories.search_group_name AS search_group_name,
            offer.offer_name             AS name,
            offer.offer_url              AS url,
            offer."offer_is_national"    AS is_national,
            offer."offer_creation_date"  as offer_creation_date,
            stock."stock_beginning_date" as stock_beginning_date,
            enriched_offer.last_stock_price as stock_price,
            (CASE WHEN booking_numbers.booking_number IS NOT NULL THEN booking_numbers.booking_number ELSE 0 END) AS booking_number,
            (CASE WHEN offer."offer_subcategoryId" in ('LIVRE_PAPIER', 'SEANCE_CINE') THEN CONCAT('product-', offer.offer_product_id) ELSE CONCAT('offer-', offer.offer_id) END) AS item_id
      FROM public.offer
      JOIN subcategories ON offer."offer_subcategoryId" = subcategories.id
      JOIN enriched_offer ON offer.offer_id = enriched_offer.offer_id
      JOIN (SELECT * FROM public.venue WHERE venue_validation_token IS NULL) venue ON offer."venue_id" = venue.venue_id
      JOIN (SELECT * FROM public.offerer WHERE offerer_validation_token IS NULL) offerer ON offerer.offerer_id = venue."venue_managing_offerer_id"
      LEFT JOIN public.stock on offer.offer_id = stock.offer_id
      LEFT JOIN (
            SELECT count(*) AS booking_number, offer.offer_product_id
            FROM public.booking
            LEFT JOIN public.stock
            ON booking.stock_id = stock.stock_id
            LEFT JOIN public.offer offer 
            ON stock.offer_id=offer.offer_id 
            WHERE booking.booking_creation_date >= NOW() - INTERVAL '14 days'
            AND NOT booking.booking_is_cancelled
            GROUP BY offer.offer_product_id
      ) booking_numbers
      ON booking_numbers.offer_product_id = offer.offer_product_id
    WHERE offer."offer_is_active" = TRUE
      AND (EXISTS (SELECT * FROM offer_has_at_least_one_active_mediation(offer.offer_id)))
      AND (EXISTS (SELECT * FROM offer_has_at_least_one_bookable_stock(offer.offer_id)))
      AND offerer."offerer_is_active" = TRUE
      AND offer."offer_validation" = 'APPROVED'
      AND offer."offer_subcategoryId" not in ('ACTIVATION_THING', 'ACTIVATION_EVENT')
      AND offer."offer_product_id"    not in ('1839851',
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
    '3612166');
END;
$body$
LANGUAGE plpgsql;



/* Creation of the materialized view. */
DROP MATERIALIZED VIEW IF EXISTS recommendable_offers;
CREATE MATERIALIZED VIEW IF NOT EXISTS recommendable_offers AS
SELECT * FROM get_recommendable_offers()
WITH NO DATA;



/* Populating the materialized view. */
REFRESH MATERIALIZED VIEW recommendable_offers;
/* Takes about 80 secondes with the indexes.*/


/* Check that the view is populated. */
SELECT COUNT(*) FROM recommendable_offers;
/* 09/11/20 : count was 26796. */
