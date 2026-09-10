cube(`Bookings`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_booking\``,
  description: `Individual bookings made by pass Culture beneficiaries. Central cube for analyzing booking activity, revenue, and usage patterns. Each row is one booking transaction linking a user, an offer, a venue, and a deposit. Use booking_intermediary_amount (price × quantity) for revenue calculations.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of bookings, including cancelled ones.`,
    },
    countNonCancelled: {
      type: `count`,
      filters: [{ sql: `${CUBE}.booking_is_cancelled = FALSE` }],
      description: `Number of bookings excluding cancelled ones. Base for revenue and usage rate calculations.`,
    },
    countUsed: {
      type: `count`,
      filters: [{ sql: `${CUBE}.booking_is_used = TRUE` }],
      description: `Number of bookings that were actually used (ticket scanned, item collected, etc.).`,
    },
    totalRevenue: {
      sql: `booking_intermediary_amount`,
      type: `sum`,
      filters: [{ sql: `${CUBE}.booking_is_cancelled = FALSE` }],
      description: `Total theoretical revenue: sum of booking_intermediary_amount for non-cancelled bookings. This is the amount charged to beneficiary credits, NOT the amount reimbursed to partners (reimbursement schedules may differ).`,
    },
    averageAmount: {
      sql: `booking_intermediary_amount`,
      type: `avg`,
      filters: [{ sql: `${CUBE}.booking_is_cancelled = FALSE` }],
      description: `Average booking amount (booking_intermediary_amount) for non-cancelled bookings, in euros.`,
    },
    uniqueUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      description: `Number of distinct beneficiaries who made at least one booking.`,
    },
    usageRate: {
      sql: `1.0 * ${countUsed} / NULLIF(${countNonCancelled}, 0) * 100`,
      type: `number`,
      title: `Usage rate (%)`,
      description: `Percentage of non-cancelled bookings that were actually used. Formula: countUsed / countNonCancelled × 100.`,
    },
  },

  dimensions: {
    bookingId: {
      sql: `booking_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique booking identifier.`,
    },
    creationDate: {
      sql: `booking_creation_date`,
      type: `time`,
      description: `Date when the booking was created (DATE precision). Use this for time-series analysis.`,
    },
    usedDate: {
      sql: `TIMESTAMP(booking_used_date)`,
      type: `time`,
      description: `Date when the booking was used (DATE precision). Use this for time-series analysis.`,
    },
    createdAt: {
      sql: `booking_created_at`,
      type: `time`,
      description: `Exact timestamp when the booking was created (DATETIME precision).`,
    },
    status: {
      sql: `booking_status`,
      type: `string`,
      description: `Current booking status (e.g. CONFIRMED, CANCELLED, USED).`,
    },
    isCancelled: {
      sql: `booking_is_cancelled`,
      type: `boolean`,
      description: `Whether the booking was cancelled. Cancelled bookings are excluded from revenue and usage rate calculations.`,
    },
    isUsed: {
      sql: `booking_is_used`,
      type: `boolean`,
      description: `Whether the booking was actually used (ticket scanned, item collected). Only non-cancelled bookings can be used.`,
    },

    // Offer
    offerId: {
      sql: `offer_id`,
      type: `string`,
      description: `Identifier of the booked offer.`,
    },
    offerName: {
      sql: `offer_name`,
      type: `string`,
      description: `Name of the booked offer as displayed in the app.`,
    },
    offerCategoryId: {
      sql: `offer_category_id`,
      type: `string`,
      title: `Category`,
      description: `Top-level offer category. Values: SPECTACLE, CINEMA, LIVRE, MUSIQUE_LIVE, MUSIQUE_ENREGISTREE, MUSEE, FILM, INSTRUMENT, BEAUX_ARTS, MEDIA, JEU, PRATIQUE_ART, CONFERENCE, CARTE_JEUNES.`,
    },
    offerSubcategoryId: {
      sql: `offer_subcategory_id`,
      type: `string`,
      title: `Subcategory`,
      description: `Detailed offer subcategory within the parent category.`,
    },
    isPhysicalGoods: {
      sql: `physical_goods`,
      type: `boolean`,
      description: `Whether the offer is a physical good (book, vinyl, instrument, etc.).`,
    },
    isDigitalGoods: {
      sql: `digital_goods`,
      type: `boolean`,
      description: `Whether the offer is digital content (streaming, VOD, e-book, etc.).`,
    },
    isEvent: {
      sql: `event`,
      type: `boolean`,
      description: `Whether the offer is an event (concert, cinema screening, theater performance, etc.).`,
    },

    // User
    userId: {
      sql: `user_id`,
      type: `string`,
      description: `Identifier of the beneficiary who made the booking.`,
    },
    userAgeAtBooking: {
      sql: `user_age_at_booking`,
      type: `number`,
      description: `Age of the beneficiary at the time of booking (15-20 typically).`,
    },
    userRegion: {
      sql: `user_region_name`,
      type: `string`,
      title: `User region`,
      description: `Administrative region where the beneficiary resides (18 metropolitan regions + overseas territories).`,
    },
    userDepartment: {
      sql: `user_department_code`,
      type: `string`,
      description: `Department code where the beneficiary resides.`,
    },
    userMacroDensity: {
      sql: `user_macro_density_label`,
      type: `string`,
      title: `User density`,
      description: `Population density classification of the beneficiary's area. Values: dense urban, sparse urban, rural (INSEE classification).`,
    },
    userActivity: {
      sql: `user_activity`,
      type: `string`,
      description: `Self-declared activity of the beneficiary: student, apprentice, high schooler, unemployed, etc.`,
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
    venueTypeLabel: {
      sql: `venue_type_label`,
      type: `string`,
      title: `Venue type`,
      description: `Cultural type of the venue: Museum, Cinema, Bookstore, Performance hall, Library, etc.`,
    },
    venueRegion: {
      sql: `venue_region_name`,
      type: `string`,
      title: `Venue region`,
      description: `Administrative region where the venue is located. Can differ from user region for cross-region bookings.`,
    },
    venueDepartment: {
      sql: `venue_department_code`,
      type: `string`,
      description: `Department code where the venue is located.`,
    },
    venueMacroDensity: {
      sql: `venue_macro_density_label`,
      type: `string`,
      title: `Venue density`,
      description: `Population density classification of the venue's area. Values: dense urban, sparse urban, rural.`,
    },

    // Deposit
    depositType: {
      sql: `deposit_type`,
      type: `string`,
      title: `Credit type`,
      description: `Type of credit used for this booking. Values: GRANT_18 (300 EUR at 18), GRANT_15_17 (20-30 EUR at 15-17), GRANT_17_18 (post-reform credit for 17-18), GRANT_FREE.`,
    },

    // Offerer
    offererId: {
      sql: `offerer_id`,
      type: `string`,
      description: `Identifier of the offerer (legal entity/organization) that owns the venue.`,
    },
    offererName: {
      sql: `offerer_name`,
      type: `string`,
      description: `Name of the offerer organization (association, company, or local authority).`,
    },
  },
  joins: {
    Finance: {
      relationship: `one_to_many`,
      sql: `${CUBE.bookingId} = ${Finance.bookingId}`
    }
  }
});
