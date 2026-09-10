cube(`Stocks`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_stock\``,
  description: `Stock availability for pass Culture offers. Each stock represents a specific price/date combination for an offer. Events have stocks with a beginning_date (screening time, concert date); non-event offers (books, digital) have stocks without dates. One offer can have multiple stocks (e.g., different screening times, different price tiers).`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of stocks.`,
    },
    totalQuantity: {
      sql: `stock_quantity`,
      type: `sum`,
      description: `Total initial quantity across all stocks. NULL quantities (unlimited digital goods) are excluded from the sum.`,
    },
    totalAvailable: {
      sql: `total_available_stock`,
      type: `sum`,
      description: `Total remaining available stock (initial quantity minus non-cancelled bookings).`,
    },
    totalBookings: {
      sql: `total_bookings`,
      type: `sum`,
      description: `Total number of bookings (individual + collective) across all stocks.`,
    },
    totalNonCancelledBookings: {
      sql: `total_non_cancelled_bookings`,
      type: `sum`,
      description: `Total non-cancelled bookings across all stocks.`,
    },
    averagePrice: {
      sql: `stock_price`,
      type: `avg`,
      description: `Average stock price in euros. 0 means free.`,
    },
  },

  dimensions: {
    stockId: {
      sql: `stock_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique stock identifier.`,
    },
    beginningDate: {
      sql: `stock_beginning_date`,
      type: `time`,
      description: `Date and time of the event. Only set for event offers (concerts, screenings, shows). Empty for non-event offers.`,
    },
    bookingLimitDate: {
      sql: `stock_booking_limit_date`,
      type: `time`,
      description: `Deadline after which the stock can no longer be booked.`,
    },
    creationDate: {
      sql: `stock_creation_date`,
      type: `time`,
      description: `Date when the stock was created by the cultural partner.`,
    },
    price: {
      sql: `stock_price`,
      type: `number`,
      description: `Price of this stock in euros. 0 if the offer is free.`,
    },
    quantity: {
      sql: `stock_quantity`,
      type: `number`,
      description: `Initial total quantity when stock was created. NULL if unlimited (e.g., digital goods).`,
    },
    features: {
      sql: `stock_features`,
      type: `string`,
      description: `Movie screening features for synchronized cinema offers (e.g., VO, VF, 3D). Can contain multiple values.`,
    },

    // Offer
    offerId: {
      sql: `offer_id`,
      type: `string`,
      description: `Identifier of the offer this stock belongs to.`,
    },
    offerName: {
      sql: `offer_name`,
      type: `string`,
      description: `Name of the offer as displayed in the app.`,
    },
    offerSubcategoryId: {
      sql: `offer_subcategory_id`,
      type: `string`,
      description: `Subcategory of the offer.`,
    },

    // Offerer
    offererId: {
      sql: `offerer_id`,
      type: `string`,
      description: `Identifier of the offerer organization.`,
    },
  },
});
