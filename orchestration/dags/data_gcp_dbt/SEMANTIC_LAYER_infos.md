# Semantic Layer Implementation Plan

## Overview

This document outlines the research, best practices, and implementation plan for adding a semantic layer to the data-gcp dbt project using dbt's MetricFlow.

## What is a Semantic Layer?

A semantic layer is an abstraction layer that sits between raw data in a warehouse and business intelligence tools, translating complex data structures into simple, business-friendly metrics and dimensions. It serves as the single source of truth for metric definitions across an organization.

### Key Benefits

1. **Consistency**: Ensures all stakeholders use the same metric definitions
2. **Self-Service Analytics**: Empowers non-technical users to query data safely
3. **Reduced Redundancy**: Define metrics once, use everywhere
4. **Governed Access**: Centralized security and access control
5. **AI-Ready**: Provides structured context for AI/LLM tools to understand business metrics

## dbt Semantic Layer & MetricFlow

### Architecture

The dbt Semantic Layer, powered by MetricFlow (open-sourced October 2025 under Apache 2.0), consists of:

- **Semantic Models**: YAML abstractions defining data tables, entities, dimensions, and measures
- **Metrics**: Business KPIs built from measures using different calculation types
- **MetricFlow Engine**: SQL query generation and metric computation

### Core Components

#### 1. Semantic Models
Data entry points that define:
- **Entities**: Join keys for connecting models (primary/foreign keys)
- **Dimensions**: Categorical or time-based attributes for grouping/filtering
- **Measures**: Aggregations performed on columns (sum, count, avg, etc.)

#### 2. Metrics
Five types supported:
- **Simple**: Direct aggregation of a measure
- **Ratio**: Division of two measures
- **Cumulative**: Running totals over time
- **Derived**: Combination of other metrics using expressions
- **Conversion**: Percentage-based metrics (e.g., conversion rates)

### Requirements

- dbt version 1.6 or higher
- Supported warehouses: BigQuery, Snowflake, Databricks, Redshift, Postgres (dbt Core only)
- dbt Cloud account for Semantic Layer API access (optional for local development)

## Open Source Semantic Layer Tools Landscape

While this POC focuses on dbt MetricFlow, it's important to understand the broader ecosystem of open source semantic layer tools. This knowledge helps in making informed architecture decisions and understanding potential integration points.

### Core Open Source Platforms

#### 1. dbt MetricFlow (Apache 2.0)
**Status**: Open-sourced October 2025

**Key Features**:
- Vendor-neutral, works with BigQuery, Snowflake, Databricks, Redshift, Postgres
- YAML-based metric definitions
- Five metric types: simple, ratio, cumulative, derived, conversion
- Query generation engine that translates to optimized SQL
- Part of Open Semantic Interchange (OSI) initiative

**Best For**: dbt-first organizations, multi-warehouse environments, vendor flexibility

**Resources**: [dbt Developer Hub](https://docs.getdbt.com/docs/build/about-metricflow)

---

#### 2. Cube (Apache 2.0)
**Status**: Mature, actively maintained

**Key Features**:
- Headless BI platform with API-first architecture
- REST, GraphQL, SQL, MDX, DAX API support
- Object-oriented semantic modeling with reusable components
- Built-in relational caching engine (sub-second latency)
- Pre-aggregations and multi-tenant security
- Native vector database support and AI-powered query optimization
- 40+ data source connectors

**Best For**: Embedded analytics, API-first architectures, external data products, AI/LLM integration

**Resources**: [cube.dev](https://cube.dev/use-cases/semantic-layer) | [GitHub](https://github.com/cube-js/cube)

---

#### 3. Synmetrix (Open Source)
**Status**: Production-ready, built on Cube

**Key Features**:
- Built on top of Cube Core
- 50+ data source connectors
- Unified governed data model
- Pre-aggregation capabilities
- Web-based UI for model management

**Best For**: Teams wanting Cube capabilities with additional tooling layer

**Resources**: [synmetrix.org](https://synmetrix.org/) | [GitHub](https://github.com/synmetrix/synmetrix)

---

#### 4. Malloy (Open Source)
**Status**: Actively developed (moved from Google to Meta)

**Key Features**:
- Modern semantic modeling and query language
- Combines safety of semantic models with SQL flexibility
- Publisher: Open-source semantic model server
- Supports BigQuery, Snowflake, Trino, DuckDB, Postgres, MySQL
- Define models once, use everywhere
- Created by Looker founder Lloyd Tabb

**Best For**: Teams wanting a new modeling paradigm, advanced hierarchical data analysis

**Resources**: [malloydata.dev](https://www.malloydata.dev/) | [GitHub: malloy](https://github.com/malloydata/malloy) | [GitHub: publisher](https://github.com/malloydata/publisher)

---

### BI Tools with Semantic Layer Capabilities

#### 5. Apache Superset (Apache 2.0)
**Key Features**:
- Powerful dashboarding and exploration
- Intentionally thin semantic layer
- Dataset layer for metrics and dimensions
- SQL Lab for complex queries
- RBAC support
- 40+ database connectors

**Best For**: Visualization layer on top of Cube/dbt, developer-friendly BI

**Integration**: Works excellently with Cube as semantic layer - [Cube + Superset guide](https://cube.dev/blog/open-source-looker-alternative)

---

#### 6. Lightdash (MIT License)
**Key Features**:
- Native dbt integration
- Leverages dbt metrics and models directly
- Define metrics in dbt, visualize in Lightdash
- Self-service analytics for dbt-first teams
- Open-source Looker alternative

**Best For**: dbt-native organizations wanting BI layer

**Compatibility**: Built specifically for dbt

---

#### 7. Metabase (AGPL)
**Key Features**:
- SQL and NoSQL database support (MongoDB, Druid)
- dbt integration for semantic layering
- User-friendly query builder
- Embedded analytics capabilities

**Best For**: Non-technical users, broad database support

**Compatibility**: Integrates with dbt, Cube

---

### Comparison Matrix

| Tool | License | Architecture | Best Use Case | Data Warehouses | API Support |
|------|---------|--------------|---------------|-----------------|-------------|
| **dbt MetricFlow** | Apache 2.0 | Query generator | dbt-first, multi-warehouse | BQ, Snowflake, Databricks, Redshift, Postgres | Limited (via dbt Cloud) |
| **Cube** | Apache 2.0 | Headless BI | Embedded analytics, APIs | 40+ sources | REST, GraphQL, SQL, MDX, DAX |
| **Synmetrix** | Open Source | Cube-based | Cube + UI | 50+ sources | Via Cube |
| **Malloy** | Open Source | Language + Server | Advanced modeling | BQ, Snowflake, Trino, DuckDB, Postgres, MySQL | Via Publisher |
| **Superset** | Apache 2.0 | BI Platform | Visualization | 40+ sources | REST |
| **Lightdash** | MIT | dbt-native BI | dbt teams | Via dbt | REST |
| **Metabase** | AGPL | BI Platform | Self-service | SQL & NoSQL | REST |

### Recommended Stacks

**Stack 1: dbt-Native** (Recommended for this project)
- **Semantic Layer**: dbt MetricFlow
- **BI Tool**: Lightdash or Preset (commercial Superset)
- **Best For**: dbt-first organizations, modern data stack

**Stack 2: API-First / Embedded**
- **Semantic Layer**: Cube
- **BI Tool**: Apache Superset or custom apps
- **Best For**: SaaS products, embedded analytics, external data products

**Stack 3: Multi-Tool Flexibility**
- **Semantic Layer**: Cube or dbt MetricFlow
- **BI Tools**: Mix of Superset, Metabase, custom integrations
- **Best For**: Organizations supporting multiple analytics use cases

**Stack 4: Advanced Modeling**
- **Semantic Layer**: Malloy + Publisher
- **BI Tool**: Custom or compatible SQL clients
- **Best For**: Teams wanting next-gen query language

### Key Trends for 2026

1. **Open Semantic Interchange (OSI)**: Vendor-neutral standards emerging (dbt, Snowflake, Salesforce, ThoughtSpot)
2. **AI Integration**: Semantic layers powering ChatGPT, Snowflake Cortex, Databricks Genie
3. **Real-time Support**: All major platforms adding streaming data capabilities
4. **Interoperability**: Metrics portable across tools and clouds

### Additional Resources

- [Announcing open source MetricFlow - dbt Labs](https://www.getdbt.com/blog/open-source-metricflow-governed-metrics)
- [Cube GitHub Repository](https://github.com/cube-js/cube)
- [Semantic Layers: A Buyers Guide](https://davidsj.substack.com/p/semantic-layers-a-buyers-guide)
- [Open source Looker alternative with Cube and Apache Superset](https://cube.dev/blog/open-source-looker-alternative)
- [Semantic Layer 2025: MetricFlow vs Snowflake vs Databricks](https://www.typedef.ai/resources/semantic-layer-metricflow-vs-snowflake-vs-databricks)
- [What Is a Semantic Layer? - Airbyte](https://airbyte.com/blog/the-rise-of-the-semantic-layer-metrics-on-the-fly)

## POC Use Case: Booking Analytics

### Rationale

The `mrt_global__booking` model is ideal for a POC because:

1. **Business Critical**: Core metric for the platform
2. **Rich Dimensions**: User demographics, venue attributes, offer categories, time
3. **Clear Measures**: Booking amount, quantity, counts
4. **Multiple Metric Types**: Revenue, cancellation rate, average booking value, user retention
5. **Well-Structured**: Already in mart layer with proper joins

### Current State Analysis

The booking model (`models/mart/global/mrt_global__booking.sql`) includes:
- **Fact Data**: 89 columns covering booking transactions
- **User Dimensions**: Age, location, demographics, activity status
- **Venue Dimensions**: Location, type, density, partner info
- **Offer Dimensions**: Category, subcategory, type (physical/digital/event)
- **Time Dimensions**: Creation date, cancellation date, usage date
- **Measures**: booking_amount, booking_quantity, booking_rank

## Implementation Plan

### Phase 1: Foundation (Week 1-2)

#### Task 1.1: Create Semantic Model for Bookings
**File**: `models/mart/global/semantic/booking.yml`

```yaml
semantic_models:
  - name: bookings
    description: "Individual booking transactions with user and venue context"
    model: ref('mrt_global__booking')
    defaults:
      agg_time_dimension: booking_created_at

    # Primary entity
    entities:
      - name: booking
        type: primary
        expr: booking_id
      - name: user
        type: foreign
        expr: user_id
      - name: offer
        type: foreign
        expr: offer_id
      - name: venue
        type: foreign
        expr: venue_id
      - name: offerer
        type: foreign
        expr: offerer_id

    # Dimensions for slicing/filtering
    dimensions:
      - name: booking_created_at
        type: time
        type_params:
          time_granularity: day
      - name: booking_status
        type: categorical
      - name: booking_is_cancelled
        type: categorical
      - name: booking_is_used
        type: categorical
      - name: deposit_type
        type: categorical
      - name: offer_category_id
        type: categorical
      - name: offer_subcategory_id
        type: categorical
      - name: user_age
        type: categorical
      - name: user_department_code
        type: categorical
      - name: user_region_name
        type: categorical
      - name: venue_department_code
        type: categorical
      - name: venue_region_name
        type: categorical
      - name: venue_type_label
        type: categorical
      - name: physical_goods
        type: categorical
      - name: digital_goods
        type: categorical
      - name: event
        type: categorical

    # Measures for aggregation
    measures:
      - name: bookings
        description: "Total count of bookings"
        agg: count
        expr: booking_id
      - name: booking_revenue
        description: "Total booking amount"
        agg: sum
        expr: booking_amount
      - name: booking_quantity_total
        description: "Total quantity booked"
        agg: sum
        expr: booking_quantity
      - name: cancelled_bookings
        description: "Count of cancelled bookings"
        agg: count
        expr: booking_id
        agg_params:
          filter: booking_is_cancelled = true
      - name: used_bookings
        description: "Count of used bookings"
        agg: count
        expr: booking_id
        agg_params:
          filter: booking_is_used = true
      - name: unique_users
        description: "Count of unique users with bookings"
        agg: count_distinct
        expr: user_id
```

#### Task 1.2: Define Core Metrics
**File**: `models/mart/global/semantic/metrics.yml`

```yaml
metrics:
  # Simple metrics
  - name: total_bookings
    description: "Total number of bookings"
    type: simple
    label: "Total Bookings"
    type_params:
      measure: bookings

  - name: total_revenue
    description: "Total booking revenue"
    type: simple
    label: "Total Revenue"
    type_params:
      measure: booking_revenue

  - name: active_users
    description: "Number of unique users with bookings"
    type: simple
    label: "Active Users"
    type_params:
      measure: unique_users

  # Ratio metrics
  - name: cancellation_rate
    description: "Percentage of bookings that were cancelled"
    type: ratio
    label: "Cancellation Rate"
    type_params:
      numerator: cancelled_bookings
      denominator: bookings

  - name: usage_rate
    description: "Percentage of bookings that were used"
    type: ratio
    label: "Usage Rate"
    type_params:
      numerator: used_bookings
      denominator: bookings

  - name: average_booking_value
    description: "Average revenue per booking"
    type: ratio
    label: "Avg Booking Value"
    type_params:
      numerator: booking_revenue
      denominator: bookings

  - name: revenue_per_user
    description: "Average revenue per active user"
    type: ratio
    label: "Revenue per User"
    type_params:
      numerator: booking_revenue
      denominator: unique_users

  # Cumulative metrics
  - name: cumulative_revenue
    description: "Running total of revenue over time"
    type: cumulative
    label: "Cumulative Revenue"
    type_params:
      measure: booking_revenue
```

#### Task 1.3: Update dbt_project.yml
Add MetricFlow configuration:

```yaml
# Add to existing dbt_project.yml
semantic-models:
  +enabled: true

metrics:
  +enabled: true
```

### Phase 2: Testing & Validation (Week 3)

#### Task 2.1: Local Testing with MetricFlow CLI
```bash
# List available metrics
dbt sl list metrics

# Query a simple metric
dbt sl query --metrics total_bookings --group-by booking_created_at__month

# Query with dimensions
dbt sl query --metrics total_revenue,cancellation_rate \
  --group-by user_region_name \
  --where "booking_created_at >= '2025-01-01'"

# Test ratio metrics
dbt sl query --metrics average_booking_value,revenue_per_user \
  --group-by offer_category_id
```

#### Task 2.2: Validate Metric Consistency
Compare semantic layer results with existing analytics:
1. Run queries using semantic layer
2. Run equivalent queries against `mrt_global__booking` directly
3. Validate numbers match
4. Document any discrepancies

#### Task 2.3: Create Documentation
- Add descriptions to all metrics
- Document common use cases
- Create example queries for business users

### Phase 3: Extension & Integration (Week 4+)

#### Task 3.1: Expand to Related Models
Create semantic models for:
- **Offers**: `mrt_global__offer`
- **Users**: `mrt_global__user_beneficiary`
- **Venues**: `mrt_global__venue`

This enables cross-entity metrics like:
- Offer conversion rate (offers viewed vs booked)
- User lifetime value
- Venue popularity metrics

#### Task 3.2: Add Advanced Metrics
- **Derived Metrics**: Combine multiple base metrics
- **Conversion Metrics**: Funnel analysis
- **Cohort Analysis**: User retention by cohort

#### Task 3.3: Integration Options
Depending on infrastructure:
- **dbt Cloud**: Expose via Semantic Layer API
- **BI Tools**: Integrate with Tableau, Preset, Hex, Lightdash
- **SQL Proxy**: Query via JDBC/ODBC
- **Python/Notebooks**: Query via dbt-metricflow package

### Phase 4: Governance & Scale

#### Task 4.1: Establish Metric Governance
- Define metric ownership (@analytics, @data-science, @data-engineering)
- Create approval process for new metrics
- Version control for metric changes
- Deprecation policy for outdated metrics

#### Task 4.2: Performance Optimization
- Add appropriate materialization strategies
- Create aggregate tables for frequently queried metrics
- Monitor query performance
- Optimize dimension cardinality

#### Task 4.3: Training & Adoption
- Document metric catalog
- Train business users on metric usage
- Create self-service guidelines
- Establish feedback loop

## Technical Considerations

### Directory Structure
```
models/mart/global/
├── semantic/
│   ├── booking.yml          # Booking semantic model
│   ├── offer.yml            # Offer semantic model
│   ├── user.yml             # User semantic model
│   ├── venue.yml            # Venue semantic model
│   └── metrics.yml          # All metric definitions
├── mrt_global__booking.sql
├── mrt_global__offer.sql
└── ...
```

### Version Control
- All semantic models and metrics in version control
- Document changes in CHANGELOG.md
- Use semantic versioning for breaking changes
- Tag releases when deploying to production

### Testing Strategy
- Unit tests: Validate measure calculations
- Integration tests: Verify cross-model joins
- Data tests: Ensure dimension uniqueness
- Regression tests: Compare against existing queries

## Success Metrics for POC

1. **Accuracy**: 100% match with existing analytics queries
2. **Performance**: Query response time < 5 seconds for standard metrics
3. **Adoption**: 3+ teams using semantic layer metrics
4. **Coverage**: 10+ core business metrics defined
5. **Self-Service**: 50% reduction in ad-hoc SQL requests

## Risks & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Performance degradation | High | Benchmark queries, create materialized views |
| Metric definition conflicts | Medium | Establish governance process early |
| Steep learning curve | Medium | Create comprehensive docs + training |
| Limited BI tool support | Low | Start with dbt Cloud integration |
| Data quality issues | High | Implement data tests at source |

## Resources & References

### Official Documentation
- [About MetricFlow - dbt Developer Hub](https://docs.getdbt.com/docs/build/about-metricflow)
- [Build your metrics - dbt Developer Hub](https://docs.getdbt.com/docs/build/build-metrics-intro)
- [dbt Semantic Layer - dbt Developer Hub](https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl)
- [Semantic models - dbt Developer Hub](https://docs.getdbt.com/docs/build/semantic-models)
- [Best practices for building metrics](https://docs.getdbt.com/best-practices/how-we-build-our-metrics/semantic-layer-9-conclusion)

### Industry Resources
- [Simplify Metrics: Exploring dbt Semantic Layer - Datafold](https://www.datafold.com/blog/dbt-semantic-layer)
- [dbt Semantic Layer for Metrics Definition - Atlan](https://atlan.com/dbt-semantic-layer/)
- [How the dbt Semantic Layer works with MetricFlow - dbt Labs](https://www.getdbt.com/blog/how-the-dbt-semantic-layer-works)
- [Best Semantic Layer Solutions for Data Teams [2026 Guide] - Kaelio](https://kaelio.com/blog/best-semantic-layer-solutions-for-data-teams-2026-guide)
- [What Is a Semantic Layer? - IBM](https://www.ibm.com/think/topics/semantic-layer)
- [The Role of Semantic Layers in Modern Data Analytics - Databricks](https://www.databricks.com/glossary/semantic-layer)
- [What is a Semantic Layer? A Detailed Guide - DataCamp](https://www.datacamp.com/blog/semantic-layer)

### Latest Developments
- [dbt Labs Open Sources MetricFlow (October 2025)](https://www.prnewswire.com/news-releases/dbt-labs-affirms-commitment-to-open-semantic-interchange-by-open-sourcing-metricflow-302582794.html) - MetricFlow now available under Apache 2.0 license

## Next Steps

1. Review and approve this implementation plan
2. Set up development environment with dbt 1.6+
3. Begin Phase 1 implementation
4. Schedule weekly sync to review progress
5. Plan Phase 2 validation activities

## Questions for Discussion

1. Which BI tools does the team currently use for analytics?
2. Are there specific business metrics causing definition conflicts?
3. What is the current approval process for new metrics?
4. Should we integrate with dbt Cloud or use open-source dbt Core?
5. Are there performance benchmarks we need to meet?

---

**Document Version**: 1.0
**Last Updated**: 2026-01-06
**Owner**: @data-engineering
