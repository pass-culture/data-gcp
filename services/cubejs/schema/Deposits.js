cube(`Deposits`, {
  sql: `SELECT * FROM \`passculture-data-prod.analytics_prod.global_deposit\``,
  description: `Credits (deposits) granted to pass Culture beneficiaries. A beneficiary can receive multiple deposits over time (e.g., GRANT_15_17 at age 15-17, then GRANT_18 at 18). Each row is one deposit with its granted amount, consumption metrics, and the beneficiary's demographics at grant time. Use this cube to analyze credit consumption, diversity of cultural practices, and deposit lifecycle.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of deposits granted.`,
    },
    totalAmount: {
      sql: `deposit_amount`,
      type: `sum`,
      description: `Total amount of credits granted, in euros. GRANT_15_17: 20-30 EUR, GRANT_18: 300 EUR (historically up to 500 EUR), GRANT_17_18: post-reform amount.`,
    },
    totalSpent: {
      sql: `total_actual_amount_spent`,
      type: `sum`,
      description: `Total actual amount spent by beneficiaries from their credits, in euros. Based on used bookings (booking_is_used = TRUE).`,
    },
    averageConsumptionRate: {
      sql: `1.0 * ${totalSpent} / NULLIF(${totalAmount}, 0) * 100`,
      type: `number`,
      title: `Average consumption rate (%)`,
      description: `Average percentage of granted credit that has been spent. Formula: totalSpent / totalAmount × 100. Key pass Culture KPI for measuring credit utilization.`,
    },
    averageDiversityScore: {
      sql: `total_diversity_score`,
      type: `avg`,
      description: `Average diversity score across deposits. Measures the variety of cultural categories booked by beneficiaries (higher = more diverse cultural consumption).`,
    },
    uniqueUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      description: `Number of distinct beneficiaries who received at least one deposit.`,
    },
  },

  dimensions: {
    depositId: {
      sql: `deposit_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique deposit identifier.`,
    },
    depositType: {
      sql: `deposit_type`,
      type: `string`,
      title: `Credit type`,
      description: `Type of credit granted. Values: GRANT_18 (300 EUR at 18), GRANT_15_17 (20-30 EUR at 15-17), GRANT_17_18 (post-reform credit for 17-18), GRANT_FREE.`,
    },
    reformCategory: {
      sql: `deposit_reform_category`,
      type: `string`,
      description: `Reform category distinguishing pre-reform and post-reform credit rules. Values include: 15_17_pre_reform, 18_pre_reform, post_reform, etc.`,
    },
    source: {
      sql: `deposit_source`,
      type: `string`,
      description: `Identity verification method used to create the deposit. Values: educonnect (school login), ubble (video ID check), dms (manual administrative process).`,
    },
    creationDate: {
      sql: `deposit_creation_date`,
      type: `time`,
      description: `Date when the deposit was granted to the beneficiary.`,
    },
    expirationDate: {
      sql: `deposit_expiration_date`,
      type: `time`,
      description: `Date when the deposit expires. Unspent credit is lost after this date.`,
    },

    // User
    userId: {
      sql: `user_id`,
      type: `string`,
      description: `Identifier of the beneficiary who received the deposit.`,
    },
    userAgeAtDeposit: {
      sql: `user_age_at_deposit`,
      type: `number`,
      description: `Age of the beneficiary when the deposit was created (typically 15-18).`,
    },
    userRegion: {
      sql: `user_region_name`,
      type: `string`,
      title: `Region`,
      description: `Administrative region where the beneficiary resides.`,
    },
    userDepartment: {
      sql: `user_department_code`,
      type: `string`,
      description: `Department code where the beneficiary resides.`,
    },
    userMacroDensity: {
      sql: `user_macro_density_label`,
      type: `string`,
      title: `Density`,
      description: `Population density classification of the beneficiary's area. Values: dense urban, sparse urban, rural (INSEE classification).`,
    },
    userIsInQpv: {
      sql: `user_is_in_qpv`,
      type: `boolean`,
      title: `In QPV`,
      description: `Whether the beneficiary resides in a Quartier Prioritaire de la Politique de la Ville (priority neighborhood). INSEE benchmark: 9.4% of the 15-24 population lives in QPV.`,
    },
    userIsPriorityPublic: {
      sql: `user_is_priority_public`,
      type: `boolean`,
      title: `Priority audience`,
      description: `Whether the beneficiary belongs to a priority audience: resides in QPV, resides in a rural area, or is not in education. Key equity indicator for pass Culture.`,
    },
  },
});
