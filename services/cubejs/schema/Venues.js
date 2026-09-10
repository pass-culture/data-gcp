cube(`Venues`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_venue\``,
  description: `Cultural partner venues of pass Culture. Each row is one venue (physical or digital location) attached to an offerer organization. Contains location hierarchy (postal code → city → department → region), classification, activity status, and pre-aggregated booking/revenue/offer counters. Use this cube to analyze venue distribution, partner activity, and geographic coverage.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of venues registered on pass Culture.`,
    },
    countActive: {
      type: `count`,
      filters: [{ sql: `${CUBE}.is_active_last_30days = TRUE` }],
      description: `Number of venues that had at least one booking (individual or collective) in the last 30 days.`,
    },
    totalBookings: {
      sql: `total_bookings`,
      type: `sum`,
      description: `Total number of bookings (individual + collective) across all venues.`,
    },
    totalNonCancelledBookings: {
      sql: `total_non_cancelled_bookings`,
      type: `sum`,
      description: `Total non-cancelled bookings (individual + collective) across all venues.`,
    },
    totalRealRevenue: {
      sql: `total_real_revenue`,
      type: `sum`,
      description: `Total actual revenue: amount from used bookings that have been reimbursed to partners, in euros.`,
    },
    totalTheoreticRevenue: {
      sql: `total_theoretic_revenue`,
      type: `sum`,
      description: `Total theoretical revenue: amount of non-cancelled bookings (whether used or not yet), in euros. Higher than real revenue because it includes bookings not yet used.`,
    },
    totalCreatedOffers: {
      sql: `total_created_offers`,
      type: `sum`,
      description: `Total number of offers created by venues (individual + collective).`,
    },
    totalBookableOffers: {
      sql: `total_bookable_offers`,
      type: `sum`,
      description: `Total number of currently bookable offers (non-expired, non-sold-out stock).`,
    },
  },

  dimensions: {
    venueId: {
      sql: `venue_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique venue identifier.`,
    },
    name: {
      sql: `venue_name`,
      type: `string`,
      description: `Internal venue name.`,
    },
    publicName: {
      sql: `venue_public_name`,
      type: `string`,
      description: `Venue name as displayed in the pass Culture app (may differ from internal name).`,
    },
    typeLabel: {
      sql: `venue_type_label`,
      type: `string`,
      title: `Venue type`,
      description: `Cultural type of the venue: Museum, Cinema, Bookstore, Performance hall, Library, Festival, etc.`,
    },
    label: {
      sql: `venue_label`,
      type: `string`,
      title: `Quality label`,
      description: `Ministry of Culture quality label (e.g., CCN, CDN, Scene nationale, Theatre lyrique). Manually attributed by pass Culture teams. NULL if no label.`,
    },

    // Location
    postalCode: {
      sql: `venue_postal_code`,
      type: `string`,
      description: `Postal code of the venue.`,
    },
    city: {
      sql: `venue_city`,
      type: `string`,
      description: `City where the venue is located.`,
    },
    departmentCode: {
      sql: `venue_department_code`,
      type: `string`,
      description: `Department code where the venue is located.`,
    },
    departmentName: {
      sql: `venue_department_name`,
      type: `string`,
      description: `Department name where the venue is located.`,
    },
    region: {
      sql: `venue_region_name`,
      type: `string`,
      title: `Region`,
      description: `Administrative region where the venue is located (18 metropolitan regions + overseas territories).`,
    },
    macroDensity: {
      sql: `venue_macro_density_label`,
      type: `string`,
      title: `Density`,
      description: `Population density classification of the venue's area. Values: dense urban, sparse urban, rural (INSEE classification).`,
    },
    latitude: {
      sql: `venue_latitude`,
      type: `number`,
      description: `Geographic latitude of the venue.`,
    },
    longitude: {
      sql: `venue_longitude`,
      type: `number`,
      description: `Geographic longitude of the venue.`,
    },
    isPermanent: {
      sql: `venue_is_permanent`,
      type: `boolean`,
      description: `Whether the venue is a permanent physical location (as opposed to a temporary or digital-only venue).`,
    },
    inQpv: {
      sql: `venue_in_qpv`,
      type: `boolean`,
      title: `In QPV`,
      description: `Whether the venue is located in a Quartier Prioritaire de la Politique de la Ville (priority neighborhood).`,
    },

    // Status
    isActiveLastMonth: {
      sql: `is_active_last_30days`,
      type: `boolean`,
      description: `Whether the venue had at least one booking in the last 30 days.`,
    },
    isActiveCurrentYear: {
      sql: `is_active_current_year`,
      type: `boolean`,
      description: `Whether the venue had at least one bookable offer in the current calendar year.`,
    },
    creationDate: {
      sql: `venue_creation_date`,
      type: `time`,
      description: `Date when the venue was registered on pass Culture.`,
    },

    // Offerer
    offererName: {
      sql: `offerer_name`,
      type: `string`,
      description: `Name of the offerer organization (association, company, or local authority) that owns this venue. One offerer can have multiple venues.`,
    },
    isEpn: {
      sql: `offerer_is_epn`,
      type: `boolean`,
      title: `National public institution`,
      description: `Whether the offerer is an Etablissement Public National (e.g., Opera de Paris, BnF, Centre Pompidou).`,
    },
  },
});
