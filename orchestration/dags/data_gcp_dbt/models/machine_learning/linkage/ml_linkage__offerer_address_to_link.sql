with
    offerer_venue_address as (
        select
            oa.offerer_address_id,
            trim(lower(oa.offerer_address_label)) as offerer_address_label, -- always unlabeled
            oa.offerer_address_type,
            
            oa.offerer_id, --- soon deleted
            -- o.offerer_siren, -- impossible join

            oa.address_id,
            oa.address_street,
            oa.address_postal_code,
            oa.address_city,
            oa.address_ban_id,
            oa.address_insee_code,
            cast(oa.address_department_code as string) as address_department_code,
            concat({{ normalize_address("oa.address_street", "cast(oa.address_postal_code as string)", "oa.address_city") }}, ' ', lower(oa.address_postal_code), ' ', lower(oa.address_city)) as normalized_address,
            oa.address_latitude,
            oa.address_longitude,

            oa.venue_id,
            trim(lower(v.venue_name)) as venue_name,
            trim(lower(v.venue_public_name)) as venue_public_name,
            concat({{ normalize_address("v.venue_street", "cast(v.venue_postal_code as string)", "v.venue_city") }}, ' ', lower(v.venue_postal_code), ' ', lower(v.venue_city)) as normalized_venue_address,
            v.venue_siret,
            v.venue_managing_offerer_id,
            o.offerer_siren as venue_managing_offerer_siren

        from {{ ref("int_applicative__offerer_address") }} as oa
        left join
            {{ ref("int_applicative__venue") }} as v on oa.venue_id = v.venue_id
        left join
            {{ ref("int_raw__offerer") }} as o on v.venue_managing_offerer_id = o.offerer_id
        where oa.offerer_address_type = 'VENUE_LOCATION'
    ),
    offerer_offer_address as (
        select
            oa.offerer_address_id,
            trim(lower(oa.offerer_address_label)) as offerer_address_label, -- may or may not be labeled (25k unlabeled, 29k labeled)
            oa.offerer_address_type,
            
            oa.offerer_id, -- soon deleted
            -- o.offerer_siren, -- impossible join

            oa.address_id,
            oa.address_street,
            oa.address_postal_code,
            oa.address_city,
            oa.address_ban_id,
            oa.address_insee_code,
            cast(oa.address_department_code as string) as address_department_code,
            concat({{ normalize_address("oa.address_street", "cast(oa.address_postal_code as string)", "oa.address_city") }}, ' ', lower(oa.address_postal_code), ' ', lower(oa.address_city)) as normalized_address,
            oa.address_latitude,
            oa.address_longitude,

            oa.venue_id,
            trim(lower(v.venue_name)) as venue_name,
            trim(lower(v.venue_public_name)) as venue_public_name,
            concat({{ normalize_address("v.venue_street", "cast(v.venue_postal_code as string)", "v.venue_city") }}, ' ', lower(v.venue_postal_code), ' ', lower(v.venue_city))  as normalized_venue_address,
            v.venue_siret,
            v.venue_managing_offerer_id,
            o.offerer_siren as venue_managing_offerer_siren

        from {{ ref("int_applicative__offerer_address") }} as oa
        left join
            {{ ref("int_applicative__venue") }} as v on oa.venue_id = v.venue_id
        left join
            {{ ref("int_raw__offerer") }} as o on v.venue_managing_offerer_id = o.offerer_id
        where oa.offerer_address_type = 'OFFER_LOCATION'
    )

select * from offerer_venue_address
union all
select * from offerer_offer_address



