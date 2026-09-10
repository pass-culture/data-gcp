cube(`UserBeneficiaries`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_user_beneficiary\``,
  description: `Enriched table of pass Culture beneficiaries. Combines user demographics, deposit history, spending breakdown, and geographic data. Key table for external reporting indicators: coverage, activation, diversity, consumption, and priority audience metrics. Each row is one beneficiary.`,

  measures: {
    totalUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      description: `Total number of distinct beneficiaries (users who received at least one credit).`,
    },
    currentBeneficiaries: {
      sql: `case when user_is_current_beneficiary then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries with active (non-expired, non-exhausted) credit. These users can still make bookings.`,
    },
    activeUsers: {
      sql: `case when user_is_active then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries with an active account (not suspended or deactivated).`,
    },
    priorityPublicUsers: {
      sql: `case when user_is_priority_public then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries belonging to priority audiences: resides in QPV, resides in a rural area, or is not in education. Key equity indicator.`,
    },
    qpvUsers: {
      sql: `case when user_is_in_qpv then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries residing in a Quartier Prioritaire de la Politique de la Ville (QPV). INSEE benchmark: 9.4% of the 15-24 population lives in QPV.`,
    },
    ruralUsers: {
      sql: `case when user_macro_density_label = 'rural' then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries residing in rural areas (INSEE macro density classification). INSEE benchmark: 31% of the 15-18 population lives in rural areas.`,
    },
    usersInEducation: {
      sql: `case when user_is_in_education then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries currently in education (student, high schooler, apprentice). Self-declared status.`,
    },
    unemployedUsers: {
      sql: `case when user_is_unemployed then user_id end`,
      type: `countDistinct`,
      description: `Beneficiaries who declared themselves as unemployed.`,
    },

    // Spending
    totalDeposits: {
      sql: `total_deposit_amount`,
      type: `sum`,
      description: `Total amount of credits granted across all beneficiaries, in euros.`,
    },
    totalSpent: {
      sql: `total_actual_amount_spent`,
      type: `sum`,
      description: `Total actual amount spent by beneficiaries, in euros. Based on used bookings (booking_is_used = TRUE).`,
    },
    totalTheoreticalSpent: {
      sql: `total_theoretical_amount_spent`,
      type: `sum`,
      description: `Total theoretical amount spent (includes non-cancelled bookings not yet used). Higher than actual spent because it includes pending bookings.`,
    },
    totalDigitalSpent: {
      sql: `total_theoretical_digital_goods_amount_spent`,
      type: `sum`,
      description: `Total theoretical amount spent on digital goods (streaming, e-books, VOD, etc.), in euros.`,
    },
    totalPhysicalSpent: {
      sql: `total_theoretical_physical_goods_amount_spent`,
      type: `sum`,
      description: `Total theoretical amount spent on physical goods (books, vinyl, instruments, etc.), in euros.`,
    },
    totalOutingsSpent: {
      sql: `total_theoretical_outings_amount_spent`,
      type: `sum`,
      description: `Total theoretical amount spent on outings/events (cinema, concerts, theater, museums, etc.), in euros.`,
    },
    totalRemainingCredit: {
      sql: `total_theoretical_remaining_credit`,
      type: `sum`,
      description: `Total theoretical remaining credit across all beneficiaries, in euros.`,
    },

    // Rates
    spendRate: {
      sql: `1.0 * ${totalSpent} / NULLIF(${totalDeposits}, 0) * 100`,
      type: `number`,
      description: `Average consumption rate: percentage of granted credit that has been actually spent. Formula: totalSpent / totalDeposits × 100. Key pass Culture KPI.`,
    },
    avgSpendPerUser: {
      sql: `1.0 * ${totalSpent} / NULLIF(${totalUsers}, 0)`,
      type: `number`,
      description: `Average actual amount spent per beneficiary, in euros.`,
    },
    pctQpv: {
      sql: `1.0 * ${qpvUsers} / NULLIF(${totalUsers}, 0) * 100`,
      type: `number`,
      description: `Share of beneficiaries residing in QPV (%). INSEE benchmark for 15-24 age group: 9.4%.`,
    },
    pctRural: {
      sql: `1.0 * ${ruralUsers} / NULLIF(${totalUsers}, 0) * 100`,
      type: `number`,
      description: `Share of beneficiaries residing in rural areas (%). INSEE benchmark for 15-18 age group: 31%.`,
    },
    pctPriorityPublic: {
      sql: `1.0 * ${priorityPublicUsers} / NULLIF(${totalUsers}, 0) * 100`,
      type: `number`,
      description: `Share of beneficiaries belonging to priority audiences (QPV, rural, or not in education), as a percentage.`,
    },

    // Diversity
    avgDiversityScore: {
      sql: `total_diversity_score`,
      type: `avg`,
      description: `Average diversity score: measures the variety of cultural categories booked by beneficiaries. Higher score means more diverse cultural consumption.`,
    },
    totalNonCancelledBookings: {
      sql: `total_non_cancelled_individual_bookings`,
      type: `sum`,
      description: `Total number of non-cancelled individual bookings across all beneficiaries.`,
    },
    avgBookingsPerUser: {
      sql: `1.0 * ${totalNonCancelledBookings} / NULLIF(${totalUsers}, 0)`,
      type: `number`,
      description: `Average number of non-cancelled bookings per beneficiary.`,
    },

    // Time to action
    avgDaysToFirstBooking: {
      sql: `days_between_activation_date_and_first_booking_date`,
      type: `avg`,
      description: `Average number of days between account activation and first booking. Measures onboarding speed.`,
    },
    avgDaysToFirstPaidBooking: {
      sql: `days_between_activation_date_and_first_booking_paid`,
      type: `avg`,
      description: `Average number of days between account activation and first paid booking (excludes free bookings).`,
    },
  },

  dimensions: {
    userId: {
      sql: `user_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique beneficiary identifier.`,
    },

    // Time
    creationDate: {
      sql: `user_creation_date`,
      type: `time`,
      description: `Date when the beneficiary's account was created.`,
    },
    activationDate: {
      sql: `user_activation_date`,
      type: `time`,
      description: `Date when the beneficiary's account was activated (identity verified).`,
    },
    firstDepositDate: {
      sql: `first_deposit_creation_date`,
      type: `time`,
      description: `Date when the beneficiary received their first credit. Use this for cohort analysis of new beneficiaries.`,
    },
    lastDepositExpiration: {
      sql: `last_deposit_expiration_date`,
      type: `time`,
      description: `Expiration date of the beneficiary's most recent deposit. After this date, unspent credit is lost.`,
    },

    // Demographics
    age: {
      sql: `user_age`,
      type: `number`,
      description: `Current age of the beneficiary.`,
    },
    civility: {
      sql: `user_civility`,
      type: `string`,
      description: `Civility of the beneficiary. Values: male, female.`,
    },
    activity: {
      sql: `user_activity`,
      type: `string`,
      description: `Self-declared activity of the beneficiary: student, apprentice, high schooler, unemployed, etc. Updated at each credit grant.`,
    },
    schoolType: {
      sql: `user_school_type`,
      type: `string`,
      description: `Type of school attended by the beneficiary (when applicable).`,
    },

    // Geography
    region: {
      sql: `user_region_name`,
      type: `string`,
      title: `Region`,
      description: `Administrative region where the beneficiary resides (18 metropolitan regions + overseas territories).`,
    },
    department: {
      sql: `user_department_code`,
      type: `string`,
      description: `Department code where the beneficiary resides.`,
    },
    departmentName: {
      sql: `user_department_name`,
      type: `string`,
      description: `Department name where the beneficiary resides.`,
    },
    city: {
      sql: `user_city`,
      type: `string`,
      description: `City where the beneficiary resides.`,
    },
    epci: {
      sql: `user_epci`,
      type: `string`,
      description: `EPCI (inter-municipal cooperation body) where the beneficiary resides.`,
    },
    macroDensity: {
      sql: `user_macro_density_label`,
      type: `string`,
      title: `Density`,
      description: `Population density classification of the beneficiary's area. Values: dense urban, sparse urban, rural (INSEE classification).`,
    },
    densityLevel: {
      sql: `user_density_level`,
      type: `string`,
      description: `Detailed density level from 1 (very urban) to 7 (very rural), based on INSEE classification.`,
    },

    // Status
    isInQpv: {
      sql: `user_is_in_qpv`,
      type: `boolean`,
      title: `In QPV`,
      description: `Whether the beneficiary resides in a Quartier Prioritaire de la Politique de la Ville (priority neighborhood). Located via declared address at credit grant.`,
    },
    isPriorityPublic: {
      sql: `user_is_priority_public`,
      type: `boolean`,
      title: `Priority audience`,
      description: `Whether the beneficiary belongs to a priority audience: resides in QPV, resides in a rural area, or is not in education.`,
    },
    isUnemployed: {
      sql: `user_is_unemployed`,
      type: `boolean`,
      description: `Whether the beneficiary declared themselves as unemployed.`,
    },
    isInEducation: {
      sql: `user_is_in_education`,
      type: `boolean`,
      description: `Whether the beneficiary is currently in education (student, high schooler, apprentice).`,
    },
    isCurrentBeneficiary: {
      sql: `user_is_current_beneficiary`,
      type: `boolean`,
      title: `Current beneficiary`,
      description: `Whether the beneficiary still has available credit (not expired, not fully spent).`,
    },

    // Deposit
    firstDepositType: {
      sql: `first_deposit_type`,
      type: `string`,
      title: `First credit type`,
      description: `Type of the beneficiary's first credit. Values: GRANT_18, GRANT_15_17, GRANT_17_18, GRANT_FREE.`,
    },
    currentDepositType: {
      sql: `current_deposit_type`,
      type: `string`,
      title: `Current credit type`,
      description: `Type of the beneficiary's current (most recent) credit. Values: GRANT_18, GRANT_15_17, GRANT_17_18, GRANT_FREE.`,
    },
    reformCategory: {
      sql: `user_current_deposit_reform_category`,
      type: `string`,
      description: `Reform category of the beneficiary's current deposit, distinguishing pre-reform and post-reform credit rules.`,
    },
  },
});
