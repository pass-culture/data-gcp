cube(`DailyDeposits`, {
  sql: `SELECT * FROM \`passculture-data-prod.int_global_prod.daily_deposit\` WHERE deposit_active_date > date_sub(current_date(), interval 48 month)`,
  description: `Credits (deposits) granted to pass Culture beneficiaries. A beneficiary can receive multiple deposits over time (e.g., GRANT_15_17 at age 15-17, then GRANT_18 at 18). Each row is one deposit with its granted amount, consumption metrics, and the beneficiary's demographics at grant time. Use this cube to analyze credit consumption, diversity of cultural practices, and deposit lifecycle.`,

  measures: {
    count: {
      type: `count`,
      description: `Total number of deposits granted.`,
    },
    totalBeneficiaries: {
      sql: `user_id`,
      type: `countDistinct`,
      description: `Number of distinct beneficiaries who received at least one deposit.`,
    },
    totalBeneficiariesLastYear: {
      sql: `user_id`,
      type: `countDistinct`,
      filters: [
        { sql: `${CUBE}.deposit_active_date >= date_sub(date_trunc(deposit_active_date, month), interval 12 month)` }
      ],
      description: `Number of distinct beneficiaries who received at least one deposit in the trailing year.`,
    },

  },


  dimensions: {
    depositActiveDate: {
      sql: `TIMESTAMP(deposit_active_date)`,
      type: `time`,
      description: `Date when the deposit became active.`,
    },
    depositId: {
      sql: `deposit_id`,
      type: `string`,
      primaryKey: true,
      description: `Unique deposit identifier.`,
    },

    // User
    userId: {
      sql: `user_id`,
      type: `string`,
      description: `Identifier of the beneficiary who received the deposit.`,
    },
    userAge: {
      sql: `user_age`,
      type: `number`,
      description: `Age of the beneficiary when the deposit was created (typically 15-18).`,
    },
    userDecimalAge: {
      sql: `date_diff(deposit_active_date, user_birth_date, month)/ 12.0`,
      type: `number`,
      description: `Exact age of the beneficiary at the time of the deposit, in decimal years.`,
    },
    userDepartment: {
      sql: `user_department_code`,
      type: `string`,
      description: `Department code where the beneficiary resides.`,
    },
  }
});
