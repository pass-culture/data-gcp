---
title: Native Monthly Beneficiary Coverage
description: Description of the `mrt_native__monthly_beneficiary_coverage` table.
---

{% docs description__mrt_native__monthly_beneficiary_coverage %}
This table presents the total number of young individuals and the number of beneficiaries for each age group per department at a given active month.
{% enddocs %}

### **Business Rules**
- Each row is uniquely identified by (`population_snapshot_month`, `population_decimal_age`, `population_department_code`).
- The dataset captures the evolution of beneficiaries over time and enables analysis of coverage trends.

### **Example Usage**
This table is used to:
1. Track the proportion of young beneficiaries in various age brackets.
2. Analyze the population coverage per department.
3. Identify seasonal trends enrollment MoM.

### **Sources**
- `mrt_native__daily_user_deposit`: Tracks daily user deposits.
- `int_seed__monthly_france_population`: Provides population estimates by age and department.

{% enddocs %}

## Table description

{% docs table__mrt_native__monthly_beneficiary_coverage %}{% enddocs %}
