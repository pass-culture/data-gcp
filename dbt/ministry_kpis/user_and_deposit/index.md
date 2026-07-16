## Scope

Main tables:

- `exp_ministry__user`
- `exp_ministry__deposit`

These tables are the basis for beneficiary stock and credit lifecycle analyses.

## Concepts

- A beneficiary is represented by `user_id` in `exp_ministry__user`.
- Credits are represented by `deposit_id` in `exp_ministry__deposit`, linked through `user_id`.
- A user can have multiple deposits over time (initial credit and recredits).

## Grain and keys

- `exp_ministry__user`: one row per `user_id`.
- `exp_ministry__deposit`: one row per `deposit_id`.
- Main join key: `user_id`.

## Common joins

```
SELECT
    u.user_id,
    u.user_age,
    u.user_is_current_beneficiary,
    d.deposit_id,
    d.deposit_amount,
    d.deposit_creation_date,
    d.deposit_expiration_date,
    d.deposit_type
FROM exp_ministry__user AS u
LEFT JOIN exp_ministry__deposit AS d
    ON d.user_id = u.user_id;
```

## Example indicators (pseudo-SQL)

### Ever-credited beneficiaries

```
SELECT
    DATE_TRUNC(d.deposit_creation_date, MONTH) AS cohort_month,
    COUNT(DISTINCT d.user_id) AS total_ever_credited_beneficiaries
FROM exp_ministry__deposit AS d
INNER JOIN exp_ministry__user AS u ON u.user_id = d.user_id
WHERE d.deposit_type <> 'GRANT_FREE'
  AND (u.user_is_active = TRUE OR u.user_suspension_reason = 'upon user request')
GROUP BY cohort_month
ORDER BY cohort_month;
```

### Active beneficiaries at month-end (raw proxy)

Proxy: credit not expired at snapshot date. Does not check whether the credit balance is fully spent.

```
WITH month_end AS (
    SELECT DATE_TRUNC(day, MONTH) + INTERVAL 1 MONTH - INTERVAL 1 DAY AS snapshot_date
    FROM UNNEST(GENERATE_DATE_ARRAY('2021-01-01', CURRENT_DATE(), INTERVAL 1 MONTH)) AS day
)
SELECT
    m.snapshot_date,
    COUNT(DISTINCT d.user_id) AS active_beneficiaries
FROM month_end AS m
JOIN exp_ministry__deposit AS d
    ON d.deposit_creation_date <= m.snapshot_date
    AND d.deposit_expiration_date >= m.snapshot_date
JOIN exp_ministry__user AS u ON u.user_id = d.user_id
WHERE d.deposit_type <> 'GRANT_FREE'
  AND (u.user_is_active = TRUE OR u.user_suspension_reason = 'upon user request')
GROUP BY m.snapshot_date
ORDER BY m.snapshot_date;
```

### Coverage numerator by milestone age

`milestone_age` = exact age at first deposit activation (accounts for birthday not yet reached in that calendar year). The canonical vidoc definition computes age from monthly birth-cohort matching over each active-deposit day; this query is a snapshot-at-activation proxy. The INSEE population denominator covers ages 16–19; age 20 beneficiaries have no matching denominator for the coverage rate.

```
WITH eligible_users AS (
    SELECT
        user_id,
        DATE_DIFF(user_first_deposit_creation_date, user_birth_date, YEAR)
        - IF(
            EXTRACT(MONTH FROM user_birth_date) * 100 + EXTRACT(DAY FROM user_birth_date)
            > EXTRACT(MONTH FROM user_first_deposit_creation_date) * 100
              + EXTRACT(DAY FROM user_first_deposit_creation_date),
            1, 0
        ) AS milestone_age
    FROM exp_ministry__user
    WHERE user_first_deposit_creation_date IS NOT NULL
)
SELECT
    milestone_age,
    COUNT(DISTINCT user_id) AS total_beneficiaries
FROM eligible_users
WHERE milestone_age IN (16, 17, 18, 19, 20)
GROUP BY milestone_age
ORDER BY milestone_age;
```
