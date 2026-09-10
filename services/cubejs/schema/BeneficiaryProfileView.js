view(`BeneficiaryProfile`, {
  description: `Curated view for beneficiary analytics aligned with pass Culture reporting indicators (FI 1-5). Covers: active beneficiary counts (FI 1), priority audience shares for QPV/rural (FI 2/3), diversity metrics for 3+ category bookings (FI 4), and spending patterns by category (FI 5). Includes demographic, geographic, and deposit dimensions for cohort analysis.`,

  cubes: [
    {
      join_path: UserBeneficiaries,
      includes: [
        // Population counts (FI 1: nombre de jeunes beneficiaires actifs)
        `totalUsers`,
        `currentBeneficiaries`,
        `activeUsers`,

        // Priority audiences (FI 2/3: taux de recours par publics prioritaires)
        `priorityPublicUsers`,
        `qpvUsers`,
        `ruralUsers`,
        `usersInEducation`,
        `unemployedUsers`,
        `pctQpv`,
        `pctRural`,
        `pctPriorityPublic`,

        // Diversity (FI 4: part ayant reserve dans 3+ categories)
        `avgDiversityScore`,
        `totalNonCancelledBookings`,
        `avgBookingsPerUser`,

        // Spending (FI 5: reservations par categorie + consommation)
        `totalDeposits`,
        `totalSpent`,
        `totalDigitalSpent`,
        `totalPhysicalSpent`,
        `totalOutingsSpent`,
        `totalRemainingCredit`,
        `spendRate`,
        `avgSpendPerUser`,

        // Time to action
        `avgDaysToFirstBooking`,

        // Demographics
        `age`,
        `civility`,
        `activity`,

        // Geography
        `region`,
        `department`,
        `departmentName`,
        `city`,
        `macroDensity`,
        `densityLevel`,

        // Status flags
        `isInQpv`,
        `isPriorityPublic`,
        `isUnemployed`,
        `isInEducation`,
        `isCurrentBeneficiary`,

        // Deposit / cohort
        `firstDepositType`,
        `currentDepositType`,
        `reformCategory`,
        `firstDepositDate`,
        `lastDepositExpiration`,
      ],
    },
  ],
});
