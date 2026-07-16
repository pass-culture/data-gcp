## Ministry exports

This section documents the `exp_ministry` export tables with one dedicated page per main table family.

Each page provides table definitions, join keys, and practical pseudo-SQL examples to calculate operational indicators.

### Main table pages

| Page                                                                                                        | Main tables covered                                                                                                                                            | Focus                                       |
| ----------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| [User and deposits](https://pass-culture.github.io/data-gcp/dbt/ministry_kpis/user_and_deposit/index.md)    | `exp_ministry__user`, `exp_ministry__deposit`                                                                                                                  | Beneficiary lifecycle and credit stock      |
| [Bookings](https://pass-culture.github.io/data-gcp/dbt/ministry_kpis/booking/index.md)                      | `exp_ministry__booking` (+ links to user/offer/stock keys)                                                                                                     | Consumption, statuses, and transaction KPIs |
| [Offers and stocks](https://pass-culture.github.io/data-gcp/dbt/ministry_kpis/offer_and_stock/index.md)     | `exp_ministry__offer`, `exp_ministry__stock`, `exp_ministry__offer_metadata`                                                                                   | Supply catalog and availability             |
| [Offerers and venues](https://pass-culture.github.io/data-gcp/dbt/ministry_kpis/offerer_and_venue/index.md) | `exp_ministry__offerer`, `exp_ministry__venue`                                                                                                                 | Cultural actor and place network            |
| [Geography tables](https://pass-culture.github.io/data-gcp/dbt/ministry_kpis/geography/index.md)            | `exp_ministry__address`, `exp_ministry__user_address`, `exp_ministry__venue_address`, `exp_ministry__offerer_representation_address`, `exp_ministry__geo_iris` | Territorial segmentation and joins          |

## Calculating global indicators from ministry exports

The examples below show how to compute key operational indicators directly from the raw export tables.

### 1. Ever-credited beneficiaries

```
SELECT
    DATE_TRUNC(d.deposit_creation_date, MONTH) AS cohort_month,
    COUNT(DISTINCT d.user_id) AS total_beneficiaries
FROM exp_ministry__deposit AS d
INNER JOIN exp_ministry__user AS u ON u.user_id = d.user_id
WHERE d.deposit_type <> 'GRANT_FREE'
  AND (u.user_is_active = TRUE OR u.user_suspension_reason = 'upon user request')
GROUP BY cohort_month
ORDER BY cohort_month;
```

### 2. Active beneficiaries at month-end

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

### 3. Booking volume and revenue

```
SELECT
    DATE_TRUNC(b.booking_creation_date, MONTH) AS partition_month,
    COUNT(DISTINCT b.booking_id) AS total_bookings,
    SUM(b.booking_amount) AS total_revenue
FROM exp_ministry__booking AS b
WHERE b.booking_status NOT IN ('CANCELLED')
GROUP BY partition_month
ORDER BY partition_month;
```

### 4. Booking metrics by territory

```
SELECT
    DATE_TRUNC(b.booking_creation_date, MONTH) AS partition_month,
    ua.user_region_name,
    ua.user_department_code,
    COUNT(DISTINCT b.booking_id) AS total_bookings,
    SUM(b.booking_amount) AS total_booking_amount
FROM exp_ministry__booking AS b
LEFT JOIN exp_ministry__user_address AS ua
    ON ua.user_id = b.user_id
WHERE b.booking_status NOT IN ('CANCELLED')
GROUP BY partition_month, ua.user_region_name, ua.user_department_code
ORDER BY partition_month;
```

### 5. Coverage numerator by age cohort

For the denominator (eligible population by age), see [INSEE population data](https://github.com/pass-culture/data-insee-population/). The INSEE population series covers ages 16–19; age 20 beneficiaries have no matching denominator for the coverage rate.

`milestone_age` = exact age at first deposit activation (accounts for birthday not yet reached in that calendar year). The canonical vidoc definition computes age from monthly birth-cohort matching over each active-deposit day; this query is a snapshot-at-activation proxy.

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

### 6. Diversity: beneficiaries with 3+ categories

Scope: beneficiaries with an expired age-18 credit (`GRANT_18` or `GRANT_17_18`). Bookings across all credits (ages 15–20) are counted. Only validated bookings (`booking_used_date IS NOT NULL`) are counted, matching the canonical vidoc definition.

```
WITH expired_age18_beneficiaries AS (
    SELECT DISTINCT d.user_id
    FROM exp_ministry__deposit AS d
    WHERE d.deposit_type IN ('GRANT_18', 'GRANT_17_18')
      AND d.deposit_expiration_date < CURRENT_DATE()
),
user_categories AS (
    SELECT
        b.user_id,
        o.offer_category
    FROM exp_ministry__booking AS b
    JOIN exp_ministry__offer AS o ON o.offer_id = b.offer_id
    WHERE b.booking_used_date IS NOT NULL
    GROUP BY b.user_id, o.offer_category
),
user_diversity AS (
    SELECT
        user_id,
        COUNT(DISTINCT offer_category) AS category_count
    FROM user_categories
    GROUP BY user_id
)
SELECT
    COUNTIF(ud.category_count >= 3) AS beneficiaries_with_3plus_categories,
    COUNT(*) AS total_beneficiaries,
    SAFE_DIVIDE(COUNTIF(ud.category_count >= 3), COUNT(*)) AS diversity_rate
FROM expired_age18_beneficiaries AS eb
LEFT JOIN user_diversity AS ud ON ud.user_id = eb.user_id;
```
