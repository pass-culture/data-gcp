**deposit_id**: Unique identifier for the deposit.

**deposit_date**: The date when the deposit was made.

**deposit_amount**: Amount credited on the deposit, in euros, age recredits included and balance transferred from a previous deposit excluded. Varies by deposit_type: GRANT_15_17 up to €80 (€20 at 15, €30 at 16, €30 at 17); GRANT_18 €300 before the reform, €500 during the experimental phase; GRANT_17_18 €50 at 17 and €150 at 18; GRANT_FREE €0 (free bookings only). Recredits are already included: do not add total_recredit_amount.

**deposit_source**: Creation source of the deposit. Normalised values: 'EDUCONNECT' (Éducation nationale authentication), 'UBBLE' (remote identity check), 'DMS' (application file imported from Démarches Simplifiées); otherwise the raw source label, e.g. 'Transfer deposit to age_17_18 type' (deposit converted to GRANT_17_18), 'complétion du profil' (GRANT_FREE), or free-text labels of manual grants.

**deposit_creation_date**: The date when the deposit was created.

**deposit_active_date**: Active date of a deposit.

**deposit_update_date**: The date when the deposit was last updated (only for GRANT_15_17).

**deposit_expiration_date**: The expiration date of the deposit. GRANT_18: 24 months after creation; GRANT_15_17: On the user’s 18th birthday; GRANT_17_18: The day before the user turns 21.

**deposit_type**: Type of the deposit, can be GRANT_18, GRANT_15_17, GRANT_17_18, GRANT_FREE. Since the March 2025 reform, new adult credits are issued as GRANT_17_18 (GRANT_18 is pre-reform/legacy): to measure adult-credit conversion across the reform, count GRANT_18 OR GRANT_17_18, not GRANT_18 alone.

**deposit_reform_category**: Categorizes deposits relative to the March 2025 reform: pre-reform ('15_17_pre_reform' for GRANT_15_17, '18_pre_reform' and '18_experiment_phase' for GRANT_18 up to or above €300) vs post-reform ('17_post_reform' and '18_post_reform' for GRANT_17_18, '15_16_post_reform' for GRANT_FREE). For GRANT_17_18 it carries only the LATEST stage: a deposit created at 17 and recredited at 18 is '18_post_reform', so '17_post_reform' misses about a third of 17-credit recipients; read RECREDIT_17 / RECREDIT_18 in the recredit table instead.

**deposit_rank_asc**: Ascending rank of the deposit in user's history.

**deposit_rank_desc**: Descending rank of the deposit in user's history.

**deposit_seniority**: Total days between deposit_creation_date and today. Used for analytics purposes.

**days_between_user_creation_and_deposit_creation**: Total days between user_creation_date (user activated its account) and deposit_creation_date (user received grant).

**user_first_deposit_creation_date**: First deposit creation date.

**credit_17_18**: credit_17 / credit_18 (« crédit 17 ans » / « crédit 18 ans »): the money granted to a young person at 17 and at 18, whatever deposit carries it — not the deposit itself, nor the remaining balance. credit_17 = a RECREDIT_17 (50€ on GRANT_17_18 since the March 2025 reform, 30€ on GRANT_15_17 before), or a GRANT_15_17 deposit created at 17. credit_18 = a GRANT_18 deposit (300€, before the reform) or a RECREDIT_18 on GRANT_17_18 (150€, after). Not credits: GRANT_FREE (0€), PREVIOUS_DEPOSIT (balance carried over), BONUS_CREDIT, MANUAL_MODIFICATION.

**first_credit_17_date**: The date the young person received credit_17 (see credit_17 / credit_18): first RECREDIT_17 on any deposit, or creation of a GRANT_15_17 at 17. Comparable across the March 2025 reform. It is an unlock date, not a request date: a request made at 17 and unlocked after 18 falls at 18 (credit_17 and credit_18 the same day) — bound it with `user_age_at_first_credit_17 = 17` to measure take-up at 17. Former GRANT_15_17 beneficiaries (born up to February 2010) receive it automatically on their 17th birthday: for those generations it reflects the former 15-17 scheme, not a sign-up.

**first_credit_18_date**: The date the young person received credit_18 (see credit_17 / credit_18): GRANT_18 creation before the March 2025 reform, first RECREDIT_18 after. The column to compare 18-credit take-up across the reform. It is an unlock date, not a request date: a request made at 18 and unlocked after 19 falls after the 19th birthday — bound it with `user_age_at_first_credit_18 < 19` to compare generations.

**user_age_at_first_credit_17**: The user's exact age on `first_credit_17_date`; null if no credit_17.

**user_age_at_first_credit_18**: The user's exact age on `first_credit_18_date`; null if no credit_18. Use `< 19` to give every generation the same 12-month window after the 18th birthday.

**user_has_credit_before_18**: Whether the user received money before their 18th birthday: a GRANT_15_17 deposit, or credit_17 before 18 (a RECREDIT_17 received at 18 alongside credit_18 does not count). GRANT_FREE (0€) does not count. The population to measure the drop-off between the 17 and 18 credits: `user_has_credit_before_18 and (first_credit_18_date is null or user_age_at_first_credit_18 >= 19)`.
