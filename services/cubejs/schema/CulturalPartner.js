cube(`CulturalPartner`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_cultural_partner\``,
  description: `Cultural partners of pass Culture. Each row represents a partner venue eligible for cultural partner analysis, with geographic attributes, Adage status, activity indicators, offer and booking counters, and revenue aggregates.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of cultural partner venues.`,
    },
    countActiveLastMonth: {
      type: `count`,
      filters: [{ sql: `${CUBE}.is_active_last_30days = TRUE` }],
      description: `Number of partners with at least one booking in the last 30 days.`,
    },
    countActiveCurrentYear: {
      type: `count`,
      filters: [{ sql: `${CUBE}.is_active_current_year = TRUE` }],
      description: `Number of partners with at least one bookable offer in the current calendar year.`,
    },
    totalCreatedIndividualOffers: {
      sql: `total_created_individual_offers`,
      type: `sum`,
      description: `Total number of individual offers created by partners.`,
    },
    totalCreatedCollectiveOffers: {
      sql: `total_created_collective_offers`,
      type: `sum`,
      description: `Total number of collective offers created by partners.`,
    },
    totalCreatedOffers: {
      sql: `total_created_offers`,
      type: `sum`,
      description: `Total number of individual and collective offers created by partners.`,
    },
    totalNonCancelledIndividualBookings: {
      sql: `total_non_cancelled_individual_bookings`,
      type: `sum`,
      description: `Total number of non-cancelled individual bookings.`,
    },
    totalUsedIndividualBookings: {
      sql: `total_used_individual_bookings`,
      type: `sum`,
      description: `Total number of used individual bookings.`,
    },
    totalNonCancelledCollectiveBookings: {
      sql: `total_non_cancelled_collective_bookings`,
      type: `sum`,
      description: `Total number of non-cancelled collective bookings.`,
    },
    totalUsedCollectiveBookings: {
      sql: `total_used_collective_bookings`,
      type: `sum`,
      description: `Total number of used collective bookings.`,
    },
    totalIndividualRealRevenue: {
      sql: `total_individual_real_revenue`,
      type: `sum`,
      description: `Total actual revenue from individual bookings, in euros.`,
    },
    totalCollectiveRealRevenue: {
      sql: `total_collective_real_revenue`,
      type: `sum`,
      description: `Total actual revenue from collective bookings, in euros.`,
    },
    totalRealRevenue: {
      sql: `total_real_revenue`,
      type: `sum`,
      description: `Total actual revenue from individual and collective bookings, in euros.`,
    },
  },

  dimensions: {
    partnerId: {
      sql: `partner_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique partner venue identifier.`,
    },
    venueId: {
      sql: `venue_id`,
      type: `string`,
      description: `Identifier of the venue represented by the partner row.`,
    },
    offererId: {
      sql: `offerer_id`,
      type: `string`,
      description: `Identifier of the offerer organization owning the venue.`,
    },
    name: {
      sql: `partner_name`,
      type: `string`,
      description: `Name of the cultural partner venue.`,
    },
    academy: {
      sql: `partner_academy_name`,
      type: `string`,
      title: `Academy`,
      description: `Education academy covering the partner venue.`,
    },
    region: {
      sql: `partner_region_name`,
      type: `string`,
      title: `Region`,
      description: `Administrative region where the partner venue is located.`,
    },
    regionCode: {
      sql: `partner_region_code`,
      type: `string`,
      description: `Administrative region code of the partner venue.`,
    },
    departmentCode: {
      sql: `partner_department_code`,
      type: `string`,
      description: `Department code where the partner venue is located.`,
    },
    departmentName: {
      sql: `partner_department_name`,
      type: `string`,
      description: `Department name where the partner venue is located.`,
    },
    epci: {
      sql: `partner_epci`,
      type: `string`,
      title: `EPCI`,
      description: `Intercommunality where the partner venue is located.`,
    },
    epciCode: {
      sql: `partner_epci_code`,
      type: `string`,
      title: `EPCI code`,
      description: `Intercommunality code where the partner venue is located.`,
    },
    city: {
      sql: `partner_city`,
      type: `string`,
      description: `City where the partner venue is located.`,
    },
    cityCode: {
      sql: `partner_city_code`,
      type: `string`,
      description: `INSEE city code where the partner venue is located.`,
    },
    postalCode: {
      sql: `partner_postal_code`,
      type: `string`,
      description: `Postal code of the partner venue.`,
    },
    type: {
      sql: `partner_type`,
      type: `string`,
      title: `Partner type`,
      description: `Cultural type of the partner venue.`,
    },
    typeOrigin: {
      sql: `partner_type_origin`,
      type: `string`,
      description: `Origin of the partner type classification.`,
    },
    culturalSector: {
      sql: `cultural_sector`,
      type: `string`,
      description: `Cultural sector associated with the partner type.`,
    },
    creationDate: {
      sql: `TIMESTAMP(partner_creation_date)`,
      type: `time`,
      description: `Date when the partner venue was registered on pass Culture.`,
    },
    wasRegisteredLastYear: {
      sql: `was_registered_last_year`,
      type: `boolean`,
      description: `Whether the partner venue was already registered at the start of the previous calendar year.`,
    },
    dmsAcceptedAt: {
      sql: `dms_accepted_at`,
      type: `time`,
      description: `Date when the partner's DMS application was accepted.`,
    },
    firstDmsAdageStatus: {
      sql: `first_dms_adage_status`,
      type: `string`,
      description: `First Adage status recorded for the partner's DMS application.`,
    },
    isReferenceAdage: {
      sql: `is_reference_adage`,
      type: `boolean`,
      description: `Whether the partner is an Adage reference partner.`,
    },
    isSynchroAdage: {
      sql: `is_synchro_adage`,
      type: `boolean`,
      description: `Whether the partner is synchronized with Adage.`,
    },
    isActiveLastMonth: {
      sql: `is_active_last_30days`,
      type: `boolean`,
      description: `Whether the partner had at least one booking in the last 30 days.`,
    },
    isActiveCurrentYear: {
      sql: `is_active_current_year`,
      type: `boolean`,
      description: `Whether the partner had at least one bookable offer in the current calendar year.`,
    },
    isIndividualActiveLastMonth: {
      sql: `is_individual_active_last_30days`,
      type: `boolean`,
      description: `Whether the partner had at least one individual booking in the last 30 days.`,
    },
    isIndividualActiveCurrentYear: {
      sql: `is_individual_active_current_year`,
      type: `boolean`,
      description: `Whether the partner had at least one bookable individual offer in the current calendar year.`,
    },
    isCollectiveActiveLastMonth: {
      sql: `is_collective_active_last_30days`,
      type: `boolean`,
      description: `Whether the partner had at least one collective booking in the last 30 days.`,
    },
    isCollectiveActiveCurrentYear: {
      sql: `is_collective_active_current_year`,
      type: `boolean`,
      description: `Whether the partner had at least one bookable collective offer in the current calendar year.`,
    },
    firstOfferCreationDate: {
      sql: `first_offer_creation_date`,
      type: `time`,
      description: `Date when the partner created its first offer.`,
    },
    firstIndividualOfferCreationDate: {
      sql: `first_individual_offer_creation_date`,
      type: `time`,
      description: `Date when the partner created its first individual offer.`,
    },
    firstCollectiveOfferCreationDate: {
      sql: `first_collective_offer_creation_date`,
      type: `time`,
      description: `Date when the partner created its first collective offer.`,
    },
    lastBookableOfferDate: {
      sql: `TIMESTAMP(last_bookable_offer_date)`,
      type: `time`,
      description: `Date of the partner's most recent bookable offer.`,
    },
    firstBookableOfferDate: {
      sql: `first_bookable_offer_date`,
      type: `time`,
      description: `Date of the partner's first bookable offer.`,
    },
    firstIndividualBookableOfferDate: {
      sql: `first_individual_bookable_offer_date`,
      type: `time`,
      description: `Date of the partner's first bookable individual offer.`,
    },
    lastIndividualBookableOfferDate: {
      sql: `last_individual_bookable_offer_date`,
      type: `time`,
      description: `Date of the partner's most recent bookable individual offer.`,
    },
    firstCollectiveBookableOfferDate: {
      sql: `first_collective_bookable_offer_date`,
      type: `time`,
      description: `Date of the partner's first bookable collective offer.`,
    },
    lastCollectiveBookableOfferDate: {
      sql: `last_collective_bookable_offer_date`,
      type: `time`,
      description: `Date of the partner's most recent bookable collective offer.`,
    },
    status: {
      sql: `partner_status`,
      type: `string`,
      title: `Partner status`,
      description: `Whether the partner venue is open to the public (ERP) or not.`,
    },
  },
});
