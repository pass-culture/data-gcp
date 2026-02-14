# Semantic Layer Query Examples

## Setup Commands

```bash
cd /Users/valentin/Github/data-gcp/orchestration/dags/data_gcp_dbt

# Parse project
dbt parse

# List semantic models
mf list semantic-models

# List metrics
mf list metrics

# List dimensions for user_beneficiary
mf list dimensions
```

---

## User Beneficiary Queries

### Coverage & Activation Metrics

#### 1. National Coverage by Month
```bash
mf query \
  --metrics total_users,activated_users,current_beneficiaries \
  --group-by user__nat,user__user_creation_date__month \
  --order user__user_creation_date__month desc \
  --limit 12
```

#### 2. Activation Rate by Region
```bash
mf query \
  --metrics total_users,activated_users,activation_rate \
  --group-by user__reg \
  --order activation_rate desc
```

#### 3. Coverage by Department (Top 20)
```bash
mf query \
  --metrics total_users,current_beneficiaries,activation_rate,booking_rate \
  --group-by user__dep \
  --order total_users desc \
  --limit 20
```

#### 4. Monthly Trend - All Levels
```bash
mf query \
  --metrics total_users,activated_users,users_with_bookings,cumulative_users \
  --group-by user__user_creation_date__month \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__month asc
```

---

### Geographic Drill-Down Queries

#### 5. Full Geographic Hierarchy - One Month
```bash
# National
mf query \
  --metrics total_users,activated_users,activation_rate \
  --group-by user__nat \
  --where "user__user_creation_date >= '2025-01-01' AND user__user_creation_date < '2025-02-01'"

# Regional
mf query \
  --metrics total_users,activated_users,activation_rate \
  --group-by user__reg \
  --where "user__user_creation_date >= '2025-01-01' AND user__user_creation_date < '2025-02-01'" \
  --order total_users desc

# Department
mf query \
  --metrics total_users,activated_users,activation_rate \
  --group-by user__dep \
  --where "user__user_creation_date >= '2025-01-01' AND user__user_creation_date < '2025-02-01'" \
  --order total_users desc \
  --limit 20

# EPCI
mf query \
  --metrics total_users,activated_users,activation_rate \
  --group-by user__epci \
  --where "user__user_creation_date >= '2025-01-01' AND user__user_creation_date < '2025-02-01'" \
  --order total_users desc \
  --limit 20

# City
mf query \
  --metrics total_users,activated_users,activation_rate \
  --group-by user__com \
  --where "user__user_creation_date >= '2025-01-01' AND user__user_creation_date < '2025-02-01'" \
  --order total_users desc \
  --limit 20
```

---

### Segmentation Queries

#### 6. Priority Public Distribution
```bash
mf query \
  --metrics total_users,priority_public_users,pct_priority_public \
  --group-by user__reg \
  --order pct_priority_public desc
```

#### 7. QPV Users by Department
```bash
mf query \
  --metrics total_users,qpv_users,pct_qpv,activation_rate \
  --group-by user__dep \
  --where "user__qpv_users > 0" \
  --order pct_qpv desc \
  --limit 20
```

#### 8. Student Activation Trends
```bash
mf query \
  --metrics student_users,activated_users,activation_rate \
  --group-by user__user_is_in_education,user__user_creation_date__quarter \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__quarter desc
```

#### 9. Segment Comparison
```bash
mf query \
  --metrics total_users,priority_public_users,qpv_users,student_users,unemployed_users \
  --group-by user__user_creation_date__month \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__month desc \
  --limit 12
```

---

### Financial Metrics

#### 10. Spending by Region
```bash
mf query \
  --metrics total_deposits,total_spent,spend_rate,avg_spend_per_user \
  --group-by user__reg \
  --order total_spent desc
```

#### 11. Category Spending Distribution
```bash
mf query \
  --metrics total_spent,total_digital_goods_spent,total_physical_goods_spent,total_outings_spent \
  --group-by user__reg \
  --order total_spent desc
```

#### 12. Spend Rate by Department (Top Performers)
```bash
mf query \
  --metrics total_deposits,total_spent,spend_rate,total_remaining_credit \
  --group-by user__dep \
  --order spend_rate desc \
  --limit 20
```

---

### Diversity Metrics

#### 13. Diversity Score by Region
```bash
mf query \
  --metrics total_users,users_with_bookings,avg_diversity_score,avg_bookings_per_user \
  --group-by user__reg \
  --order avg_diversity_score desc
```

#### 14. Category Mix Analysis
```bash
mf query \
  --metrics total_spent,pct_digital_spending,pct_physical_spending,pct_outings_spending \
  --group-by user__user_creation_date__quarter \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__quarter desc
```

---

### Time-to-Action Metrics

#### 15. Average Time to First Booking
```bash
mf query \
  --metrics avg_days_to_first_booking,avg_days_to_first_paid_booking \
  --group-by user__reg \
  --order avg_days_to_first_booking asc
```

#### 16. Activation Velocity by Cohort
```bash
mf query \
  --metrics total_users,activated_users,avg_days_to_first_booking \
  --group-by user__user_creation_date__month \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__month desc
```

---

### Multi-Dimensional Analysis

#### 17. Priority Public by Geography and Time
```bash
mf query \
  --metrics total_users,priority_public_users,pct_priority_public,activation_rate \
  --group-by user__reg,user__user_creation_date__quarter \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__quarter desc,pct_priority_public desc
```

#### 18. Student Spending Patterns
```bash
mf query \
  --metrics student_users,total_spent,avg_spend_per_user,avg_diversity_score \
  --group-by user__user_is_in_education,user__dep \
  --order student_users desc \
  --limit 30
```

#### 19. Density-Based Analysis
```bash
mf query \
  --metrics total_users,activated_users,spend_rate,avg_diversity_score \
  --group-by user__user_macro_density_label,user__reg \
  --order total_users desc
```

---

### Export to CSV

#### 20. Monthly Report Export
```bash
# National level - all metrics
mf query \
  --metrics total_users,activated_users,current_beneficiaries,activation_rate,booking_rate,total_spent,spend_rate,avg_diversity_score \
  --group-by user__user_creation_date__month \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__month desc \
  --csv monthly_national_report.csv

# Regional breakdown
mf query \
  --metrics total_users,activated_users,activation_rate,total_spent,avg_spend_per_user \
  --group-by user__reg,user__user_creation_date__month \
  --where "user__user_creation_date >= '2024-01-01'" \
  --order user__user_creation_date__month desc,total_users desc \
  --csv monthly_regional_report.csv
```

---

## Comparison with Old Approach

### OLD (Aggregated Table)
```sql
-- Limited to pre-aggregated dimension combinations
SELECT
  partition_month,
  dimension_name,
  dimension_value,
  kpi_name,
  kpi
FROM mrt_external_reporting__individual
WHERE dimension_name = 'REG'
  AND kpi_name = 'activation_rate'
```

❌ Can't combine multiple dimensions
❌ Can't drill down flexibly
❌ Limited to pre-computed KPIs
❌ Can't aggregate yearly without re-processing

### NEW (Semantic Layer)
```bash
mf query \
  --metrics activation_rate,booking_rate,spend_rate \
  --group-by user__reg,user__user_macro_density_label,user__user_creation_date__month \
  --where "user__user_creation_date >= '2024-01-01'"
```

✅ Multiple metrics in one query
✅ Multiple dimensions simultaneously
✅ Any time granularity (day/month/quarter/year)
✅ Filter on any dimension
✅ MetricFlow handles SQL optimization

---

## Next Steps

1. **Parse and test**:
   ```bash
   dbt parse
   mf list metrics
   ```

2. **Run validation queries** (examples 1-5 above)

3. **Compare with existing reports** to validate accuracy

4. **Expand to other models**:
   - Daily user deposit
   - Venue
   - Offer
   - Cultural partner

5. **Deprecate aggregated tables** after migration
