**deposit_id**: Unique identifier for the deposit.

**deposit_date**: The date when the deposit was made.

**deposit_amount**: The total amount of the deposit. Amount varies by deposit_type. GRANT_15_17: 20€ in the year the user turns 15, 30€ at 16, 30€ at 17, 30€ in the year they turn 17; GRANT_18: 300€ before the reform, 500€ during the experimental phase; GRANT_17_18: 150€; GRANT_FREE: 0€ for 15–16-year-olds (allows free bookings).

**deposit_source**: Creation source of the deposit. Possible values: educonnect – Deposit initiated via the French national education authentication system; ubble – Deposit created following a remote identity verification process via Ubble. dms – Deposit imported from a Document Management System (internal or third-party document repository).

**deposit_creation_date**: The date when the deposit was created.

**deposit_active_date**: Active date of a deposit.

**deposit_update_date**: The date when the deposit was last updated (only for GRANT_15_17).

**deposit_expiration_date**: The expiration date of the deposit. GRANT_18: 24 months after creation; GRANT_15_17: On the user’s 18th birthday; GRANT_17_18: The day before the user turns 21.

**deposit_type**: Type of the deposit, can be GRANT_18, GRANT_15_17, GRANT_17_18, GRANT_FREE.

**deposit_reform_category**: Categorizes deposits following the reform, allowing distinction between 17 and 18-year-old beneficiaries for the new GRANT_17_18 credit. This field also differentiates between deposits granted before and after the reform. Values are : 15_17_pre_reform, 18_pre_reform, 18_experiment_phase, 17_post_reform, 18_post_reform.

**deposit_rank_asc**: Ascending rank of the deposit in user's history.

**deposit_rank_desc**: Descending rank of the deposit in user's history.

**deposit_seniority**: Total days between deposit_creation_date and today. Used for analytics purposes.

**days_between_user_creation_and_deposit_creation**: Total days between user_creation_date (user activated its account) and deposit_creation_date (user received grant).

**user_first_deposit_creation_date**: First deposit creation date.
