view(`BookingAnalysis`, {
  description: `Curated view for analyzing booking activity: volume, revenue, usage rates, by category, geography, time, and beneficiary profile. Combines Bookings measures with the most useful dimensions for operational and strategic analysis.`,

  cubes: [
    {
      join_path: Bookings,
      includes: [
        // Measures
        `count`,
        `countNonCancelled`,
        `countUsed`,
        `totalRevenue`,
        `averageAmount`,
        `uniqueUsers`,
        `usageRate`,

        // Time
        `creationDate`,

        // Offer
        `offerCategoryId`,
        `offerSubcategoryId`,
        `offerName`,
        `isPhysicalGoods`,
        `isDigitalGoods`,
        `isEvent`,

        // User
        `userRegion`,
        `userDepartment`,
        `userMacroDensity`,
        `userAgeAtBooking`,
        `userActivity`,

        // Venue
        `venueRegion`,
        `venueDepartment`,
        `venueMacroDensity`,
        `venueTypeLabel`,
        `venueName`,

        // Status
        `isCancelled`,
        `isUsed`,
        `depositType`,
      ],
    },
  ],
});
