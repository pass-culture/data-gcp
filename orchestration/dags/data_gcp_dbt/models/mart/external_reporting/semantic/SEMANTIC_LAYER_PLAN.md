# External Reporting Semantic Layer - Implementation Plan

## Problem Statement

Current approach creates aggregated tables with schema:
```
partition_month | dimension_name | dimension_value | kpi_name | numerator | denominator | kpi
```

This makes querying inflexible - you can't slice by multiple dimensions or drill down easily.

## Solution: Source-Based Semantic Models

Create semantic models directly on source tables (`mrt_global__booking`, `mrt_global__user_beneficiary`) with proper geographic dimensions, allowing MetricFlow to handle aggregations.

---

## Architecture

### Core Principle
**One semantic model per source table** with:
- Geographic dimensions generated from `get_dimensions()` macro
- Categorical dimensions (categories, domains, age groups)
- Measures for KPI calculations
- Metrics defined as simple/ratio/cumulative types

### Advantages
✅ Query any KPI by any dimension combination
✅ Monthly/yearly aggregation handled by MetricFlow
✅ Geographic drill-down (National → Region → Department → EPCI → City)
✅ Minimal YAML using Jinja macros
✅ Reusable dimension patterns

---

## Source Tables & Semantic Models

### 1. **Bookings** (DONE)
- **Source**: `mrt_global__booking`
- **Semantic Model**: `bookings.yml`
- **Dimensions**: 73 (time, user geo, venue geo, offer attributes)
- **Measures**: 28 (counts, revenue, segments)
- **Use Cases**: Revenue, usage rates, cancellations, diversity

### 2. **User Beneficiary**
- **Source**: `mrt_global__user_beneficiary`
- **Entity**: user (primary)
- **Dimensions**:
  - **Geographic**: NAT, REG, DEP, EPCI, COM using `get_dimensions('user', 'geo_full')`
  - **Demographic**: age, civility, activity status
  - **Segmentation**: QPV, education, unemployment, priority public
  - **Temporal**: creation_date, activation_date, deposit_creation_date
- **Measures**:
  - User counts (total, active, by segment)
  - Deposit amounts (total, average, by type)
  - Coverage metrics
- **KPIs**: Coverage rate, activation rate, beneficiary diversity

### 3. **Daily User Deposit**
- **Source**: `mrt_native__daily_user_deposit`
- **Entity**: daily_deposit (composite key)
- **Dimensions**:
  - **Geographic**: User location (NAT, REG, DEP, EPCI, COM)
  - **Temporal**: deposit_active_date
  - **Categorical**: deposit_type, age_group
- **Measures**:
  - Amount spent (daily, cumulative)
  - Active users per day
  - Deposit usage rates
- **KPIs**: Daily spend, usage rate by cohort

### 4. **Venue**
- **Source**: `mrt_global__venue`
- **Entity**: venue (primary)
- **Dimensions**:
  - **Geographic**: `get_dimensions('venue', 'geo_full')`
  - **Categorical**: venue_type, is_permanent, is_virtual
  - **Partner**: partner_type, is_local_authority
- **Measures**:
  - Venue counts
  - Offers per venue
  - Bookings per venue
- **KPIs**: Venue coverage, partner diversity

### 5. **Offer**
- **Source**: `mrt_global__offer`
- **Entity**: offer (primary)
- **Dimensions**:
  - **Geographic**: Venue location
  - **Categorical**: category, subcategory, domain
  - **Attributes**: digital_goods, physical_goods, event
  - **Temporal**: creation_date, publication_date
- **Measures**:
  - Offer counts (total, active, by category)
  - Booking rates
  - Stock quantities
- **KPIs**: Offer diversity, catalog coverage

### 6. **Cultural Partner**
- **Source**: `mrt_global__cultural_partner` or `mrt_global__offerer`
- **Entity**: offerer (primary)
- **Dimensions**:
  - **Geographic**: `get_dimensions('partner', 'academic_extended')`
  - **Categorical**: partner_type, is_EPN, is_local_authority
  - **Tags**: offerer_tags
- **Measures**:
  - Partner counts
  - Offers per partner
  - Revenue per partner
- **KPIs**: Partner diversity, geographic distribution

---

## Jinja Macro Strategy

### Macro 1: `generate_semantic_dimensions()`
```jinja
{% macro generate_semantic_dimensions(entity_prefix, hierarchy_type='geo_full', additional_dims=[]) %}
  {# Generates dimension YAML from get_dimensions() output #}
  {% set geo_dims = get_dimensions(entity_prefix, hierarchy_type) %}

  dimensions:
    # Geographic dimensions (auto-generated)
    {% for dim in geo_dims %}
    - name: {{ dim.name | lower }}
      type: categorical
      description: "Geographic dimension: {{ dim.name }}"
      expr: {{ dim.value_expr }}
    {% endfor %}

    # Additional custom dimensions
    {% for dim in additional_dims %}
    - name: {{ dim.name }}
      type: {{ dim.type }}
      description: "{{ dim.description }}"
      {% if dim.expr %}
      expr: {{ dim.expr }}
      {% endif %}
    {% endfor %}
{% endmacro %}
```

### Macro 2: `generate_count_measures()`
```jinja
{% macro generate_count_measures(entity_name, filters=[]) %}
  {# Generates standard count measures with optional filters #}
  measures:
    - name: {{ entity_name }}_count
      description: "Total count of {{ entity_name }}"
      agg: count
      expr: "1"

    {% for filter in filters %}
    - name: {{ entity_name }}_{{ filter.name }}
      description: "{{ filter.description }}"
      agg: sum
      expr: "CASE WHEN {{ filter.condition }} THEN 1 ELSE 0 END"
    {% endfor %}
{% endmacro %}
```

### Macro 3: `generate_kpi_metrics()`
```jinja
{% macro generate_kpi_metrics(kpi_configs) %}
  {# Generates metrics from KPI configuration #}
  metrics:
    {% for kpi in kpi_configs %}
    - name: {{ kpi.name }}
      description: "{{ kpi.description }}"
      type: {{ kpi.type }}
      label: "{{ kpi.label }}"
      type_params:
        {% if kpi.type == 'simple' %}
        measure: {{ kpi.measure }}
        {% elif kpi.type == 'ratio' %}
        numerator: {{ kpi.numerator }}
        denominator: {{ kpi.denominator }}
        {% elif kpi.type == 'cumulative' %}
        measure: {{ kpi.measure }}
        {% endif %}
    {% endfor %}
{% endmacro %}
```

---

## Example: User Beneficiary Semantic Model

### File: `models/mart/external_reporting/semantic/user_beneficiary.yml`

```yaml
version: 2

semantic_models:
  - name: user_beneficiary
    description: "User beneficiary data with geographic and demographic dimensions"
    model: ref('mrt_global__user_beneficiary')

    defaults:
      agg_time_dimension: user_creation_date

    entities:
      - name: user
        type: primary
        expr: user_id

    {# Use macro to generate geographic dimensions #}
    {{
      generate_semantic_dimensions(
        entity_prefix='user',
        hierarchy_type='geo_full',
        additional_dims=[
          {'name': 'user_age', 'type': 'categorical', 'description': 'User age'},
          {'name': 'user_is_in_qpv', 'type': 'categorical', 'description': 'Lives in QPV'},
          {'name': 'user_activity', 'type': 'categorical', 'description': 'Activity status'},
        ]
      )
    }}

    {# Use macro to generate count measures #}
    {{
      generate_count_measures(
        entity_name='users',
        filters=[
          {'name': 'active', 'description': 'Active users', 'condition': 'user_is_active = true'},
          {'name': 'qpv', 'description': 'QPV users', 'condition': 'user_is_in_qpv = true'},
          {'name': 'students', 'description': 'Students', 'condition': 'user_is_in_education = true'},
        ]
      )
    }}

    measures:
      - name: total_deposit_amount
        description: "Sum of deposit amounts"
        agg: sum
        expr: total_deposit_amount
```

---

## Query Examples

### Coverage Rate by Region
```bash
mf query \
  --metrics coverage_rate \
  --group-by user__reg,user__partition_month__month \
  --where "user__partition_month >= '2024-01-01'" \
  --order user__partition_month__month desc
```

### Diversity by Department
```bash
mf query \
  --metrics users_booked_3plus_categories,total_users \
  --group-by user__dep \
  --order users_booked_3plus_categories desc \
  --limit 20
```

### Spend by User Segment Over Time
```bash
mf query \
  --metrics total_spend,avg_spend_per_user \
  --group-by user__user_is_in_qpv,user__partition_month__quarter \
  --where "user__partition_month >= '2024-01-01'"
```

---

## Implementation Steps

### Phase 1: Core Semantic Models (Week 1)
1. ✅ Bookings (done)
2. ⏳ User Beneficiary
3. ⏳ Daily User Deposit

### Phase 2: Partner & Catalog (Week 2)
4. ⏳ Venue
5. ⏳ Offer
6. ⏳ Cultural Partner

### Phase 3: Metrics & Validation (Week 3)
7. Define all KPI metrics
8. Validate against existing aggregated tables
9. Document query patterns

### Phase 4: Deprecate Aggregated Tables (Week 4+)
10. Migrate dashboards to semantic layer queries
11. Deprecate `mrt_external_reporting__individual` and `mrt_external_reporting__eac`
12. Remove redundant UNION ALL generation code

---

## Success Criteria

✅ Can query any KPI by any geographic level (NAT/REG/DEP/EPCI/COM)
✅ Can aggregate monthly or yearly with simple time grain syntax
✅ Can combine multiple dimensions in a single query
✅ YAML files are <200 lines using Jinja macros
✅ Query performance ≤ existing aggregated tables
✅ 100% metric accuracy vs current reports

---

## File Structure

```
models/mart/
├── external_reporting/
│   ├── semantic/
│   │   ├── _macros/
│   │   │   ├── generate_dimensions.sql
│   │   │   ├── generate_measures.sql
│   │   │   └── generate_metrics.sql
│   │   ├── user_beneficiary.yml
│   │   ├── daily_user_deposit.yml
│   │   ├── venue.yml
│   │   ├── offer.yml
│   │   ├── cultural_partner.yml
│   │   └── external_reporting_metrics.yml
│   ├── individual/
│   │   └── kpis_segments/  (deprecated after migration)
│   └── collective/
│       └── kpis_segments/  (deprecated after migration)
```

---

## Next Action

Create Jinja macros for dimension/measure generation, then generate `user_beneficiary.yml` as first example.
