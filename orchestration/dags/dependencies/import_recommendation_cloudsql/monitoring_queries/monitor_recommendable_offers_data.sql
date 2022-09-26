with recommendable_offers as (
    select
        *
    from
        `{{ bigquery_clean_dataset }}.recommendable_offers_data`
),
non_recommenadable_offers as (
    select
        *
    from
        `{{ bigquery_clean_dataset }}.recommendable_offers_data` offer
    Where
        offer.subcategory_id in ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        OR (offer.subcategory_id = 'ACHAT_INSTRUMENT' and REGEXP_CONTAINS(LOWER(offer.name),r'bon d’achat|bons d’achat'))
        OR (offer.subcategory_id = 'MATERIEL_ART_CREATIF'AND REGEXP_CONTAINS(LOWER(offer.name),r'stabilo|surligneurs'))
        OR offer.product_id in (
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
select
    CURRENT_DATE as date,
    count(*) as recommendable_offers_count,
    (
        SELECT
            count(*) <= 0
        from
            non_recommenadable_offers nro
    ) as non_recommenadable_offers_check
from
    recommendable_offers ro