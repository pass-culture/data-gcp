cube(`Bookings`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_booking\``,
  description: `Individual bookings made by pass Culture beneficiaries. Central cube for analyzing booking activity, revenue, and usage patterns. Each row is one booking transaction linking a user, an offer, a venue, and a deposit. Use booking_intermediary_amount (price × quantity) for revenue calculations.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of bookings, including cancelled ones.`,
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

    // User
    userId: {
      sql: `user_id`,
      type: `string`,
      description: `Identifier of the beneficiary who made the booking.`,
    },
  },

  joins: {
    Finance: {
      relationship: `one_to_many`,
      sql: `${CUBE.bookingId} = ${Finance.bookingId}`
    }
  }
});
