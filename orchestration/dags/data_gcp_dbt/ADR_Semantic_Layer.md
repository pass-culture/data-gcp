# ADR-001: Adoption of a Semantic Layer for Analytics

## Status

**Proposed**

## Date

2026-01-06

---

## Context

This ADR builds on the Semantic Layer Implementation Plan provided as input, which includes:

- An overview of semantic layers and their benefits (consistency, self-service, governed access, AI-readiness)
- A detailed comparison of open-source semantic layer tools (MetricFlow, Cube, Malloy, etc.)
- A proposed POC use case focused on Booking Analytics (`mrt_global__booking`)
- A phased implementation approach (Foundation → Validation → Extension → Governance)

### Current State

The current analytics stack is **BigQuery + Metabase**, with:

- **BigQuery** providing scalable, cost-aware query execution and native result caching
- **Metabase** providing dashboarding and its own query/result cache
- Metrics often defined directly in SQL within dashboards

This setup performs well for visualization and basic analytics, but introduces increasing challenges as usage grows:

- Metric definition drift across dashboards
- High human maintenance cost when metrics change
- Difficulty onboarding new analysts
- Risk of costly ad-hoc SQL queries
- Lack of a single, governed definition of business metrics

### Problem Statement

The team is evaluating whether to introduce a semantic layer, with a strong constraint on **operational cost** (infra, setup, maintenance), and with the explicit goal of validating semantic-layer value via a POC, not a full production migration.

---

## Decision

We will introduce a semantic layer using **dbt MetricFlow** as a metric definition and governance layer, while continuing to:

- Use **BigQuery** as the execution engine
- Use **Metabase** as the visualization layer with its existing cache

**We will not** introduce an API server or additional serving infrastructure at this stage.

### MetricFlow Role

MetricFlow will initially be used as:

1. A single source of truth for metric definitions
2. A metric registry and documentation layer
3. A foundation for future extensibility (API, embedded analytics, AI use cases)

---

## Rationale

### Why a semantic layer at all?

The primary problem is **not** performance or compute cost, but **human cost and metric trust**:

**Problems:**
- Dashboards encode business logic in multiple places
- Similar metrics can disagree silently
- Metric changes require editing many dashboards
- New analysts must reverse-engineer SQL to understand definitions

**Solutions:**

A semantic layer addresses these by:
- Centralizing metric definitions
- Separating meaning (metrics) from presentation (dashboards)
- Making metric ownership, intent, and change history explicit

Dashboards remain the primary interface for humans, but become simpler and more trustworthy.

---

### Why dbt MetricFlow?

MetricFlow was selected because it is:

- **Open source** (Apache 2.0)
- **Stateless and infra-free** (no servers, no cache, no background jobs)
- **Aligned with existing dbt usage**
- **Warehouse-native** (queries execute directly in BigQuery)

MetricFlow acts as a **metric query compiler**, not a serving layer:
- It generates SQL for metrics on demand
- It relies on BigQuery and Metabase for caching and performance

This makes it the **lowest-cost option** for a POC and early adoption.

---

### Why no API server (e.g. Cube) initially?

**API-based semantic layers provide:**
- Programmatic access to metrics
- Caching and pre-aggregations
- Strong row-level security
- Support for embedded analytics and high concurrency

**However, they also introduce:**
- New infrastructure
- Ongoing operational overhead
- Setup and maintenance cost

**Given the current usage pattern** (internal dashboards, cached BI queries, low concurrency), these benefits do not yet justify the cost.

**MetricFlow intentionally does not provide:**
- A cache
- Pre-aggregated tables
- A serving API

This is **by design** and aligns with the cost-aware platform constraints.

---

## Consequences

### Positive

- ✅ Single, governed definition of core business metrics
- ✅ Reduced dashboard drift and metric inconsistency
- ✅ Lower human maintenance cost when metrics change
- ✅ Safer self-service analytics with curated dimensions
- ✅ Faster onboarding through business-level abstractions
- ✅ Future-proofing for API, BI tool changes, or AI integration
- ✅ Zero additional infrastructure cost

### Negative / Trade-offs

- ⚠️ No immediate reduction in BigQuery costs
- ⚠️ No inherent performance improvement beyond existing caching
- ⚠️ Added modeling and governance effort (YAML maintenance)
- ⚠️ MetricFlow queries can still scan large tables if not carefully modeled
- ⚠️ Requires discipline to avoid duplicate metric definitions in Metabase

---

## Dashboard Strategy for the POC

### Key Decision

**Dashboards will NOT be fully migrated for the POC.**

### Rationale

The POC's objective is to validate:
- ✓ Correctness of metric definitions
- ✓ Fitness of MetricFlow for the booking use case
- ✓ Human and governance benefits

It is **not** intended to:
- ✗ Replace Metabase
- ✗ Rebuild dashboards
- ✗ Optimize performance

Fully migrating dashboards would:
- Increase scope and risk
- Confound metric validation with UX changes
- Provide little additional learning at this stage

### POC Dashboard Approach

1. Keep existing Metabase dashboards as-is
2. Select 1–2 representative dashboards (e.g. booking KPIs)
3. For those dashboards:
   - Compare current SQL-based metrics vs semantic-layer metrics
   - Validate numerical equivalence
   - Document differences and edge cases

**Dashboards remain the consumer, not the driver, of the POC.**

---

## Guardrails & Usage Guidelines

- ✓ MetricFlow must become the **single source of truth** for shared metrics
- ✓ Dashboards should **not** redefine business metrics in SQL
- ✓ Start with a **small, high-value metric set** (5–10 metrics)
- ✓ Avoid over-modeling dimensions early
- ✓ Push business logic into measures, not ad-hoc SQL
- ✓ Monitor BigQuery cost and query patterns

---

## Cost Evaluation Strategy

### Objective

Establish a **data-driven approach** to measure the total cost of ownership (TCO) for the semantic layer POC, ensuring that:

1. We can quantify both **infrastructure costs** (BigQuery) and **human costs** (maintenance time)
2. Cost changes are **detected early** and addressed proactively
3. The cost/benefit trade-off is **transparent and measurable**

### Cost Categories

#### 1. Infrastructure Costs (BigQuery)

**What We're Tracking:**
- Query execution costs (bytes processed)
- Storage costs (no expected change)
- Slot usage patterns (on-demand vs reserved)

**Why This Matters:**
- MetricFlow generates SQL that executes in BigQuery
- Poor metric design can lead to expensive full table scans
- Need to ensure semantic layer doesn't introduce query inefficiency

#### 2. Human Costs

**What We're Tracking:**
- Time spent maintaining metric definitions in dashboards
- Time spent resolving metric definition conflicts
- Time spent onboarding new analysts
- Time spent debugging inconsistent metrics

**Why This Matters:**
- The primary value proposition is reducing human maintenance cost
- These costs are often invisible but significant
- Need baseline to measure improvement

#### 3. Development Costs

**What We're Tracking:**
- Time to define semantic models and metrics (YAML)
- Time to validate metrics vs existing SQL
- Time to document and train users

**Why This Matters:**
- One-time setup cost that should be amortized over time
- Need to ensure POC learnings reduce future development time

---

### Baseline Measurement (Pre-POC)

#### BigQuery Cost Baseline

**Week 1 Action Items:**

```bash
# 1. Establish 30-day baseline query cost for booking-related queries
# Run this query in BigQuery to analyze historical costs

SELECT
  DATE(creation_time) as query_date,
  user_email,
  COUNT(*) as query_count,
  SUM(total_bytes_processed) / POW(10, 12) as total_tb_processed,
  SUM(total_bytes_processed) / POW(10, 12) * 5.0 as estimated_cost_usd
FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND statement_type = 'SELECT'
  AND (
    -- Filter for booking-related queries
    REGEXP_CONTAINS(query, r'mrt_global__booking')
    OR REGEXP_CONTAINS(query, r'booking')
  )
GROUP BY query_date, user_email
ORDER BY query_date DESC;
```

**Capture:**
- Average daily query cost for booking analytics
- Peak query patterns (daily/weekly)
- Top query users and their patterns
- Median bytes processed per query

#### Human Cost Baseline

**Week 1 Survey:**
1. How many hours per month are spent updating metric definitions in dashboards?
2. How many hours per month are spent resolving "why don't these numbers match?" questions?
3. How many hours does it take to onboard a new analyst to understand key metrics?
4. How many times per month are metric definitions found to be inconsistent?

**Document Current State:**
- Number of dashboards with booking metrics
- Number of SQL queries defining booking revenue
- Number of different booking cancellation rate definitions

---

### Ongoing Monitoring (During & Post-POC)

#### 1. Query Cost Monitoring

**Tools:**
- BigQuery `INFORMATION_SCHEMA.JOBS` for query costs
- dbt Cloud query logs (if using dbt Cloud)
- Custom monitoring dashboard in Metabase

**Weekly Metrics:**
```sql
-- Track MetricFlow-generated query costs
SELECT
  DATE(creation_time) as query_date,
  REGEXP_CONTAINS(query, r'-- MetricFlow') as is_metricflow,
  COUNT(*) as query_count,
  AVG(total_bytes_processed) / POW(10, 9) as avg_gb_processed,
  SUM(total_bytes_processed) / POW(10, 12) * 5.0 as total_cost_usd
FROM `project.region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT`
WHERE
  creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND statement_type = 'SELECT'
GROUP BY query_date, is_metricflow
ORDER BY query_date DESC;
```

**Cost Thresholds:**
- 🟢 **Green**: MetricFlow queries cost ≤ 110% of baseline equivalent
- 🟡 **Yellow**: MetricFlow queries cost 110-150% of baseline (investigate)
- 🔴 **Red**: MetricFlow queries cost > 150% of baseline (action required)

#### 2. Query Performance Monitoring

**Track:**
- Query execution time (median, p95, p99)
- Bytes processed per metric query
- Cache hit rates in Metabase
- Failed queries and error patterns

**Optimization Triggers:**
- If p95 query time > 10 seconds → investigate partitioning/clustering
- If bytes processed > 100GB for simple metrics → review dimension joins
- If Metabase cache hit rate < 50% → investigate caching strategy

#### 3. Human Cost Tracking

**Monthly Check-ins:**
- Time spent on YAML maintenance vs SQL dashboard updates
- Number of metric definition conflicts resolved
- Time to answer "what does this metric mean?" questions
- Analyst satisfaction survey (1-5 scale)

**Success Indicators:**
- ✅ Metric update time reduced by >30%
- ✅ Metric conflicts reduced by >50%
- ✅ Onboarding time reduced by >40%
- ✅ Analyst satisfaction score ≥ 4/5

---

### Cost Comparison Methodology

#### Apples-to-Apples Query Comparison

For each metric in the POC, compare:

**Metric**: Total Booking Revenue (Last 30 Days, by Region)

| Approach | Bytes Processed | Query Time | Maintenance Time (monthly) | Trust Level |
|----------|----------------|------------|---------------------------|-------------|
| **Current SQL** | 45 GB | 2.3s | 2 hours | Medium |
| **MetricFlow** | 48 GB | 2.5s | 0.5 hours | High |

**Cost Delta Calculation:**
```
Infrastructure Cost Change = (MetricFlow Cost - Current Cost) / Current Cost
Human Cost Savings = (Current Maintenance - MetricFlow Maintenance) * Hourly Rate
Net Cost Impact = Infrastructure Cost Change - Human Cost Savings
```

#### Total Cost of Ownership (TCO) Formula

**Monthly TCO:**
```
TCO = BigQuery Costs + (Human Hours * Hourly Rate) + (Infrastructure Ops * Hourly Rate)

POC TCO Target:
- Infrastructure cost increase: ≤ 10%
- Human cost reduction: ≥ 30%
- Net TCO reduction: ≥ 20%
```

---

### Decision Criteria

At the end of Week 3, evaluate whether to proceed based on:

#### Go/No-Go Thresholds

**PROCEED if:**
- ✅ BigQuery cost increase < 20% AND human cost reduction > 25%
- ✅ BigQuery cost increase < 10% AND human cost reduction > 15%
- ✅ Metric accuracy = 100% match with current definitions

**REEVALUATE if:**
- 🟡 BigQuery cost increase 20-30% (optimize before expanding)
- 🟡 Human cost reduction 10-25% (marginal benefit, assess alternatives)
- 🟡 Minor metric discrepancies found (document and decide if acceptable)

**PAUSE/PIVOT if:**
- 🔴 BigQuery cost increase > 30%
- 🔴 Human cost reduction < 10%
- 🔴 Significant metric inaccuracies (>5% variance)
- 🔴 Query performance degradation (p95 > 15s for simple metrics)

---

### Cost Optimization Playbook

If costs exceed thresholds, apply these strategies:

#### 1. Query Optimization
- Add clustering to `mrt_global__booking` on commonly filtered columns
- Add partitioning on `booking_created_at`
- Review dimension joins to avoid unnecessary table scans
- Use `WHERE` filters in semantic model to limit data scanned

#### 2. Metric Design Optimization
- Avoid dimension joins that multiply row counts
- Use measure filters instead of dimension filters where possible
- Create materialized views for frequently used metric combinations
- Leverage BigQuery BI Engine for repeated queries

#### 3. Caching Strategy
- Ensure Metabase cache settings are optimized
- Create scheduled refreshes for high-frequency metrics
- Use MetricFlow's SQL preview to validate query efficiency before deployment

#### 4. Scope Adjustment
- Reduce number of dimensions in semantic model
- Limit time range for cumulative metrics
- Create summary tables for historical data

---

### Reporting & Documentation

#### Weekly POC Cost Report

**Template:**
```markdown
## Week [N] Cost Report

### Infrastructure Costs
- Total BigQuery cost: $XXX
- MetricFlow queries: $XX (YY% of total)
- Cost vs baseline: +/- Z%

### Query Performance
- Median query time: X.Xs
- P95 query time: X.Xs
- Bytes processed per query: XX GB (median)
- Failed queries: N

### Human Costs (estimated)
- YAML maintenance: X hours
- Validation work: X hours
- Training/documentation: X hours

### Issues & Actions
- [Issue 1]: [Action taken]
- [Issue 2]: [Planned action]

### Status: 🟢 Green / 🟡 Yellow / 🔴 Red
```

#### Final POC Cost Analysis

At end of Week 3, produce comprehensive report:
1. **Total Cost Comparison**: Before/After TCO
2. **Cost Breakdown**: Infrastructure vs Human vs Development
3. **ROI Projection**: Expected savings over 12 months
4. **Qualitative Benefits**: Metric trust, analyst satisfaction, reduced errors
5. **Recommendation**: Proceed / Optimize / Pivot

---

## Planning & Execution

### POC Scope (Explicitly In-Scope)

- ✅ One core fact model: `mrt_global__booking`
- ✅ One semantic model (`booking.yml`)
- ✅ One metric file (`metrics.yml`)
- ✅ ~5–10 core business metrics (revenue, bookings, rates)
- ✅ CLI-based MetricFlow queries for validation

### Explicitly Out of Scope

- ❌ Full dashboard rewrites
- ❌ BI tool replacement
- ❌ API servers or caching layers
- ❌ Pre-aggregation infrastructure
- ❌ Organization-wide metric governance

### High-Level Timeline

#### Week 1
- Create semantic model for bookings
- Define core measures and simple metrics
- Enable MetricFlow in `dbt_project.yml`

#### Week 2
- Add ratio and cumulative metrics
- Validate MetricFlow queries vs raw SQL
- Fix modeling gaps (dimensions, filters)

#### Week 3
- Side-by-side comparison with 1–2 Metabase dashboards
- Document discrepancies and learnings
- Decide whether semantic-layer value is proven

---

## Future Evolution Path

### Phase 1 – Now (POC)
- dbt MetricFlow for metric definitions
- BigQuery execution
- Metabase dashboards + cache

### Phase 2 – Adoption
- Broader dashboard migration to semantic metrics
- Improved documentation and metric ownership

### Phase 3 – Scale / Product Use Cases (Optional)
- Introduce an API-based semantic layer (e.g. Cube)
- Re-express existing metric definitions (logic preserved, syntax rewritten)
- Add caching, pre-aggregations, RLS, and embedded analytics

**MetricFlow semantic design will be structured to allow low-friction migration to an API server if and when needed.**

---

## Decision Summary

A semantic layer is adopted **not to replace dashboards**, but to:

1. **Stabilize meaning**
2. **Reduce human and organizational cost**
3. **Increase trust in metrics**

**dbt MetricFlow** is chosen as the **lowest-cost, lowest-risk entry point**, with a clear upgrade path if requirements change.

---

## References

- [Semantic Layer Implementation Plan](./SEMANTIC_LAYER.md)
- [dbt MetricFlow Documentation](https://docs.getdbt.com/docs/build/about-metricflow)
- [Open Semantic Interchange (OSI) Initiative](https://www.getdbt.com/blog/open-source-metricflow-governed-metrics)

---

**ADR Author**: @data-engineering
**Reviewers**: TBD
**Last Updated**: 2026-01-06
