cube(`Offers`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_offer\``,
  description: `pass Culture cultural offer catalog. Each row is one offer published by a cultural partner. Contains product metadata, venue location, offer type classification (digital/physical/event), and aggregated booking counters. Use this cube to analyze offer adoption, availability, and pricing.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of offers in the catalog, regardless of status.`,
    },
    countBookable: {
      type: `count`,
      filters: [{ sql: `${CUBE}.offer_is_bookable = TRUE` }],
      description: `Number of currently bookable offers (has non-expired, non-sold-out stock).`,
    },
    totalBookings: {
      sql: `total_individual_bookings`,
      type: `sum`,
      description: `Total number of individual bookings across all offers (pre-aggregated counter from source table).`,
    },
    totalFavorites: {
      sql: `total_favorites`,
      type: `sum`,
      description: `Total number of times offers were added to favorites by beneficiaries.`,
    },
    averagePrice: {
      sql: `last_stock_price`,
      type: `avg`,
      description: `Average price across offers, based on the last stock price in euros.`,
    },
  },

  dimensions: {
    offerId: {
      sql: `offer_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique offer identifier.`,
    },
    name: {
      sql: `offer_name`,
      type: `string`,
      description: `Offer name as displayed in the pass Culture app.`,
    },
    categoryId: {
      sql: `offer_category_id`,
      type: `string`,
      title: `Category`,
      description: `Top-level offer category. Values: SPECTACLE, CINEMA, LIVRE, MUSIQUE_LIVE, MUSIQUE_ENREGISTREE, MUSEE, FILM, INSTRUMENT, BEAUX_ARTS, MEDIA, JEU, PRATIQUE_ART, CONFERENCE, CARTE_JEUNES.`,
    },
    subcategoryId: {
      sql: `offer_subcategory_id`,
      type: `string`,
      title: `Subcategory`,
      description: `Detailed offer subcategory within the parent category.`,
    },
    isBookable: {
      sql: `offer_is_bookable`,
      type: `boolean`,
      description: `Whether the offer currently has non-expired, non-sold-out stock and can be booked.`,
    },
    isDuo: {
      sql: `offer_is_duo`,
      type: `boolean`,
      description: `Whether the offer can be booked as a duo (2 seats for 1 booking). A duo booking counts as booking_quantity = 2.`,
    },
    isActive: {
      sql: `is_active`,
      type: `boolean`,
      description: `Whether the offer is currently visible and active in the app.`,
    },
    validation: {
      sql: `offer_validation`,
      type: `string`,
      description: `Offer validation status in the moderation pipeline. Values: DRAFT, PENDING, VALIDATED, REJECTED.`,
    },
    creationDate: {
      sql: `offer_creation_date`,
      type: `time`,
      description: `Date when the offer was created by the cultural partner.`,
    },

    // Type
    isDigital: {
      sql: `digital_goods`,
      type: `boolean`,
      description: `Whether the offer is digital content (streaming, VOD, e-book, online course, etc.).`,
    },
    isPhysical: {
      sql: `physical_goods`,
      type: `boolean`,
      description: `Whether the offer is a physical good (book, vinyl, instrument, etc.).`,
    },
    isEvent: {
      sql: `event`,
      type: `boolean`,
      description: `Whether the offer is an event (concert, cinema screening, theater performance, workshop, etc.).`,
    },

    // Venue
    venueId: {
      sql: `venue_id`,
      type: `string`,
      description: `Identifier of the venue where the offer is provided.`,
    },
    venueName: {
      sql: `venue_name`,
      type: `string`,
      description: `Name of the venue.`,
    },
    venueRegion: {
      sql: `venue_region_name`,
      type: `string`,
      title: `Venue region`,
      description: `Administrative region where the venue is located.`,
    },
    venueDepartment: {
      sql: `venue_department_code`,
      type: `string`,
      description: `Department code where the venue is located.`,
    },
    venueMacroDensity: {
      sql: `venue_macro_density_label`,
      type: `string`,
      description: `Population density classification of the venue's area. Values: dense urban, sparse urban, rural.`,
    },
    venueTypeLabel: {
      sql: `venue_type_label`,
      type: `string`,
      description: `Cultural type of the venue: Museum, Cinema, Bookstore, Performance hall, Library, etc.`,
    },

    // Offerer
    offererName: {
      sql: `offerer_name`,
      type: `string`,
      description: `Name of the offerer organization (association, company, or local authority) that published the offer.`,
    },
  },
});
