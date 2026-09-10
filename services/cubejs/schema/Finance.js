cube(`Finance`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_finance\``,
  description: `Individual booking reimbursement aggregates. Each row summarizes used individual bookings by used date, venue geography, EPN status, and offer category, with revenue, reimbursed amount, and offerer contribution amounts from invoiced finance pricing lines.`,

  measures: {
    totalBookings: {
      sql: `total_bookings`,
      type: `sum`,
      description: `Total number of used individual bookings with invoiced reimbursement data.`,
    },
    totalQuantities: {
      sql: `total_quantities`,
      type: `sum`,
      description: `Total quantities booked. A booking quantity can be 1 or 2.`,
    },
    totalRevenueAmount: {
      sql: `total_revenue_amount`,
      type: `sum`,
      description: `Total booking intermediary amount for used individual bookings, in euros.`,
    },
    totalReimbursedAmount: {
      sql: `amount`,
      type: `sum`,
      filters: [{ sql: `${CUBE}.amount_type = "offerer revenue"` }],
      description: `Total amount reimbursed to offerers from invoiced finance pricing lines, in euros.`,
    },
    totalContributionAmount: {
      sql: `amount`,
      type: `sum`,
      filters: [{ sql: `${CUBE}.amount_type = "offerer contribution"` }],
      description: `Total amount contributed by offerers on their bookings, in euros.`,
    },
    contributionRate: {
      sql: `1.0 * ${totalContributionAmount} / NULLIF(${totalRevenueAmount}, 0) * 100`,
      type: `number`,
      title: `Contribution rate (%)`,
      description: `Share of booking revenue contributed by offerers. Formula: totalContributionAmount / totalRevenueAmount x 100.`,
    },
  },

  dimensions: {
    financeId: {
      sql: `TO_HEX(MD5(CONCAT(CAST(booking_id AS STRING), '|', COALESCE(batchid, ''))))`,
      type: `string`,
      primaryKey: true,
      public: false,
      description: `Technical key for the finance aggregate row grain.`,
    },

    // Booking
    bookingId: {
      sql: `booking_id`,
      type: `string`,
      description: `Unique identifier for the booking.`,
    },
    // Venue
    venueDepartmentCode: {
      sql: `venue_department_code`,
      type: `string`,
      description: `Department code where the venue is located.`,
    },
    venueDepartmentName: {
      sql: `venue_department_name`,
      type: `string`,
      description: `Department name where the venue is located.`,
    },
    venueRegion: {
      sql: `venue_region_name`,
      type: `string`,
      title: `Venue region`,
      description: `Administrative region where the venue is located.`,
    },
    venueEpciCode: {
      sql: `venue_epci_code`,
      type: `string`,
      title: `Venue EPCI code`,
      description: `EPCI code where the venue is located.`,
    },
    venueCityCode: {
      sql: `venue_city_code`,
      type: `string`,
      title: `Venue city code`,
      description: `INSEE city code where the venue is located.`,
    },

    // Offer
    offerCategoryId: {
      sql: `offer_category_id`,
      type: `string`,
      title: `Category`,
      description: `Top-level offer category.`,
    },

    // Offerer
    offererIsEpn: {
      sql: `offerer_is_epn`,
      type: `boolean`,
      title: `National public institution`,
      description: `Whether the offerer is an Etablissement Public National.`,
    },
  },
  joins: {
    Bookings: {
      relationship: `many_to_one`,
      sql: `${CUBE.bookingId} = ${Bookings.bookingId}`
    }
  }
});
